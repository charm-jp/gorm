package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var hostRegex = regexp.MustCompile("\\((.*)\\)")
var mssqlHostRegex = regexp.MustCompile("@(.*)\\?")
var NoMaster = errors.New("no master host available")
var NoHost = errors.New("no host available")

type DatabaseConnection struct {
	connection *sql.DB
	dsn        string
	host       string
	driverName string
	isMaster   bool
	isActive   bool
}

type ConnectionManager struct {
	connections []*DatabaseConnection // Physical databases
	serverType  string
	next        uint
}

// Open concurrently opens each underlying physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func ConnectionOpen(driverName, dataSourceNames string) (*ConnectionManager, error) {
	conns := strings.Split(strings.TrimSuffix(dataSourceNames, ";"), ";")

	db := &ConnectionManager{connections: make([]*DatabaseConnection, len(conns))}

	for i := range conns {
		db.connections[i] = newConnection(driverName, conns[i])
		db.serverType = driverName
	}

	// Wait until at least one master node is returned
	time.Sleep(5 * time.Second)
	err := db.waitForMaster()

	fmt.Println("Have a master available. Enabling connection")

	return db, err
}

func newConnection(driverName, dsn string) *DatabaseConnection {
	host := "unknown"

	switch driverName {
	case "mysql":
		getHostname := hostRegex.FindStringSubmatch(dsn)

		if len(getHostname) == 2 {
			// Return an empty connection. This won't have any reconnect attached it it
			host = getHostname[1]
		}
	case "mssql":
		getHostname := mssqlHostRegex.FindStringSubmatch(dsn)

		if len(getHostname) == 2 {
			// Return an empty connection. This won't have any reconnect attached it it
			host = getHostname[1]
		}
	}

	dc := &DatabaseConnection{}
	dc.driverName = driverName
	dc.dsn = dsn
	dc.host = host

	dc.connect()

	return dc
}

func (conn *DatabaseConnection) connect() {
	var err error
	fmt.Println("Attempting connection to " + conn.host + " using " + conn.driverName + " driver")

	conn.connection, err = sql.Open(conn.driverName, conn.dsn)

	if err == nil {
		// Check the connection
		go func() {
			conn.updateStatus(nil)
		}()
	} else {
		// Queue up a reconnect
		fmt.Println("Connection to " + conn.host + " failed with message: " + err.Error())
		go func() {
			time.Sleep(5 * time.Second)
			conn.connect()
		}()
	}
}

func (db *ConnectionManager) ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	fmt.Println("Process Error:")
	fmt.Println(err)

	mysqlErrors := []string{
		"Error 1290", // Server is read only
		"Error 1053", // Server is shutting down
		"Error 3100", // Error on observer while running replication hook 'before_commit'.
		"Error 1040", // Too many connections
		"connectex",  // Connection error
		"database is closed",
		"invalid connection",
	}

	for _, errStr := range mysqlErrors {
		if strings.Contains(err.Error(), errStr) {
			_ = db.Ping()
			return true
		}
	}

	return false
}

func (conn *DatabaseConnection) updateStatus(ctx *context.Context) {
	// Ping the database
	var status error
	fmt.Println("Checking health of host " + conn.host)

	if ctx != nil {
		status = conn.connection.PingContext(*ctx)
	} else {
		status = conn.connection.Ping()
	}

	if status != nil {
		// Close and begin connection loop
		fmt.Println("Host " + conn.host + " is OFFLINE - " + status.Error())
		conn.isActive = false
		_ = conn.connection.Close()
		time.Sleep(5 * time.Second)
		conn.connect()
		return
	}

	conn.isActive = true

	// Check whether it's a master or a slave
	switch conn.driverName {
	case "mysql":
		{
			var (
				readOnly      int
				superReadOnly int
			)

			rowData := conn.connection.QueryRow("SELECT @@global.read_only, @@global.super_read_only;")
			err := rowData.Scan(&readOnly, &superReadOnly)

			if err == sql.ErrNoRows || (readOnly == 0 && superReadOnly == 0) {
				conn.isMaster = true
			} else {
				conn.isMaster = false
			}
		}
	default:
		conn.isMaster = true
	}

	if conn.isMaster {
		fmt.Println("Host " + conn.host + " is ONLINE as a MASTER")
	} else {
		fmt.Println("Host " + conn.host + " is ONLINE as a SLAVE")
	}
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *ConnectionManager) Close() error {
	for i := range db.connections {
		conn := db.connections[i]
		fmt.Println("Closing connection to " + conn.host)

		conn.isActive = false
		conn.isMaster = false
		_ = conn.connection.Close()
	}

	return nil
}

// Driver returns the physical database's underlying driver.
func (db *ConnectionManager) Driver() driver.Driver {
	m, i := db.Master()

	if m == nil {
		return nil
	}

	d := m.Driver()

	if d == nil {
		// Reconnect the master and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.Driver()
	}

	return d
}

func (db *ConnectionManager) Begin() (*sql.Tx, error) {
	tx, _, err := db.BeginHost()
	return tx, err
}

// Begin starts a transaction on the master. The isolation level is dependent on the driver.
func (db *ConnectionManager) BeginHost() (*sql.Tx, string, error) {
	m, i := db.Master()

	if m == nil {
		return nil, "", NoMaster
	}

	result, err := m.Begin()

	if err == driver.ErrBadConn {
		// Reconnect the master and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.BeginHost()
	}

	return result, db.connections[i].host, err
}

func (db *ConnectionManager) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, _, err := db.BeginHostTx(ctx, opts)
	return tx, err
}

// BeginTx starts a transaction with the provided context on the master.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *ConnectionManager) BeginHostTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, string, error) {
	m, i := db.Master()

	if m == nil {
		return nil, "", NoMaster
	}

	result, err := m.BeginTx(ctx, opts)

	if err == driver.ErrBadConn {
		// Reconnect the master and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.BeginHostTx(ctx, opts)
	}

	return result, db.connections[i].host, err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *ConnectionManager) Exec(query string, args ...interface{}) (sql.Result, error) {
	m, i := db.Master()

	if m == nil {
		return nil, NoMaster
	}

	result, err := m.Exec(query, args...)

	if err == driver.ErrBadConn {
		// Reconnect the master and attempt to run the query again
		db.connections[i].updateStatus(nil)
		return db.Exec(query, args...)
	}

	return result, err
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the master as the underlying physical db.
func (db *ConnectionManager) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	m, i := db.Master()

	if m == nil {
		return nil, NoMaster
	}

	result, err := m.ExecContext(ctx, query, args...)

	if err == driver.ErrBadConn {
		// Reconnect the master and attempt to run the query again
		db.connections[i].updateStatus(&ctx)

		return db.ExecContext(ctx, query, args...)
	}

	return result, err
}

func (db ConnectionManager) waitForMaster() error {
	if d, _ := db.Master(); d == nil {
		return NoMaster
	}

	return nil
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *ConnectionManager) Ping() error {
	fmt.Println("Pinging all nodes")
	for i := range db.connections {
		go db.connections[i].updateStatus(nil)
	}

	return nil
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *ConnectionManager) PingContext(ctx context.Context) error {
	fmt.Println("Pinging all nodes with context")
	for i := range db.connections {
		go db.connections[i].updateStatus(&ctx)
	}

	return nil
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *ConnectionManager) Prepare(query string) (*sql.Stmt, error) {
	//stmts := make([]*sql.Stmt, len(db.pdbs))
	//
	//err := scatter(len(db.pdbs), func(i int) (err error) {
	//	stmts[i], err = db.pdbs[i].Prepare(query)
	//	return err
	//})
	//
	//if err != nil {
	//	return nil, err
	//}
	//
	//return &stmt{db: db, stmts: stmts}, nil
	return &sql.Stmt{}, nil
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *ConnectionManager) PrepareContext(ctx context.Context, query string) (sql.Stmt, error) {
	//stmts := make([]*sql.Stmt, len(db.pdbs))
	//
	//err := scatter(len(db.pdbs), func(i int) (err error) {
	//	stmts[i], err = db.pdbs[i].PrepareContext(ctx, query)
	//	return err
	//})
	//
	//if err != nil {
	//	return nil, err
	//}
	//return &stmt{db: db, stmts: stmts}, nil
	return sql.Stmt{}, nil
}

func (db *ConnectionManager) Query(query string, args ...interface{}) (*sql.Rows, error) {
	result, _, err := db.QueryHost(query, args...)
	return result, err
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a slave as the physical db.
func (db *ConnectionManager) QueryHost(query string, args ...interface{}) (*sql.Rows, string, error) {
	m, i := db.Slave()

	if m == nil {
		return nil, "", NoHost
	}

	result, err := m.Query(query, args...)

	if err == driver.ErrBadConn {
		// Reconnect the node and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.QueryHost(query, args...)
	}

	return result, db.connections[i].host, err
}

func (db *ConnectionManager) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	result, _, err := db.QueryHost(query, args...)
	return result, err
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// QueryContext uses a slave as the physical db.
func (db *ConnectionManager) QueryHostContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, string, error) {
	m, i := db.Slave()

	if m == nil {
		return nil, "", NoHost
	}

	result, err := m.QueryContext(ctx, query, args...)

	if err == driver.ErrBadConn {
		// Reconnect the node and attempt to run the query again
		db.connections[i].updateStatus(&ctx)

		return db.QueryHostContext(ctx, query, args...)
	}

	return result, db.connections[i].host, err
}

func (db *ConnectionManager) QueryRow(query string, args ...interface{}) *sql.Row {
	row, _ := db.QueryHostRow(query, args...)
	return row
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a slave as the physical db.
func (db *ConnectionManager) QueryHostRow(query string, args ...interface{}) (*sql.Row, string) {
	m, i := db.Slave()

	if m == nil {
		return &sql.Row{}, ""
	}

	// We need to ping the node because we don't know what the status will be
	err := m.Ping()

	if err == driver.ErrBadConn {
		// Reconnect the node and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.QueryHostRow(query, args...)
	}

	return m.QueryRow(query, args...), db.connections[i].host
}

func (db *ConnectionManager) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows, _ := db.QueryHostRowContext(ctx, query, args...)
	return rows
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRowContext uses a slave as the physical db.
func (db *ConnectionManager) QueryHostRowContext(ctx context.Context, query string, args ...interface{}) (*sql.Row, string) {
	m, i := db.Slave()

	if m == nil {
		return &sql.Row{}, ""
	}

	// We need to ping the node because we don't know what the status will be
	err := m.PingContext(ctx)

	if err == driver.ErrBadConn {
		// Reconnect the node and attempt to run the query again
		db.connections[i].updateStatus(nil)

		return db.QueryHostRowContext(ctx, query, args...)
	}

	return m.QueryRowContext(ctx, query, args...), db.connections[i].host
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *ConnectionManager) SetMaxIdleConns(n int) {
	for i := range db.connections {
		db.connections[i].connection.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *ConnectionManager) SetMaxOpenConns(n int) {
	for i := range db.connections {
		db.connections[i].connection.SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *ConnectionManager) SetConnMaxLifetime(d time.Duration) {
	for i := range db.connections {
		db.connections[i].connection.SetConnMaxLifetime(d)
	}
}

// Slave returns one of the physical databases which is a slave
func (db *ConnectionManager) Slave() (*sql.DB, int) {
	var candidates []int
	for i := range db.connections {
		if db.connections[i].isActive && !db.connections[i].isMaster {
			candidates = append(candidates, i)
		}
	}

	if len(candidates) == 0 {
		// Only return the master if there are no active slaves
		for i := range db.connections {
			if db.connections[i].isActive && db.connections[i].isMaster {
				db.next = 0 // Reset the next server so we do a full sweep
				candidates = append(candidates, i)
			}
		}
	}

	// If there are still no candidates attempt a retry
	if len(candidates) == 0 {
		// We've fallen through which means no active servers
		fmt.Println("Request for SLAVE node pending but none available. Waiting for 3 seconds...")
		db.next = 0 // Reset the next server so we do a full sweep
		for i := range db.connections {
			go db.connections[i].updateStatus(nil)
		}

		time.Sleep(3 * time.Second)
		return db.Slave()
	}

	// Go through the candidates and get the next matching host and the following
	for _, c := range candidates {
		if c >= int(db.next) {
			// Set the next host to the next candidate
			if c < len(candidates) {
				db.next = uint(c + 1)
			} else {
				db.next = 0
			}

			return db.connections[c].connection, c
		}
	}

	// Unable to fulfill next server requirement so reset and try again immediately
	db.next = 0
	return db.Slave()
}

// Master returns the master physical database
func (db *ConnectionManager) Master() (*sql.DB, int) {
	for i := range db.connections {
		if db.connections[i].isMaster && db.connections[i].isActive {
			return db.connections[i].connection, i
		}
	}

	// If there is no master yet available, wait for one to become available
	fmt.Println("Request for MASTER node pending but none available. Checking for new masters and waiting for 3 seconds...")
	for i := range db.connections {
		go db.connections[i].updateStatus(nil)
	}

	time.Sleep(3 * time.Second)
	return db.Master()
}

// Stmt is an aggregate prepared statement.
// It holds a prepared statement for each underlying physical db.
type Stmt interface {
	Close() error
	Exec(...interface{}) (sql.Result, error)
	Query(...interface{}) (*sql.Rows, error)
	QueryRow(...interface{}) *sql.Row
}

type stmt struct {
	db    *ConnectionManager
	stmts []*sql.Stmt
}

// Close closes the statement by concurrently closing all underlying
// statements concurrently, returning the first non nil error.
func (s *stmt) Close() error {
	//return scatter(len(s.stmts), func(i int) error {
	//	return s.stmts[i].Close()
	//})
	return nil
}

// Exec executes a prepared statement with the given arguments
// and returns a Result summarizing the effect of the statement.
// Exec uses the master as the underlying physical db.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	//return s.stmts[0].Exec(args...)
	return nil, nil
}

// Query executes a prepared query statement with the given
// arguments and returns the query results as a *sql.Rows.
// Query uses a slave as the underlying physical db.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	//return s.stmts[s.db.slave(len(s.db.pdbs))].Query(args...)
	return &sql.Rows{}, nil
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error
// will be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *sql.Row's Scan scans the first selected row and discards the rest.
// QueryRow uses a slave as the underlying physical db.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
	//return s.stmts[s.db.slave(len(s.db.pdbs))].QueryRow(args...)
	return &sql.Row{}
}

//func getHostAndDriver(comm DB) (host, driver string) {
//
//	host = comm
//	driver = comm.parent.db.(*ConnectionManager).serverType
//	dsn := getConnector(tx)
//
//	host = "unknown"
//
//	switch driver {
//	case "mssql":
//
//	case "mysql":
//		getHostname := hostRegex.FindStringSubmatch(dsn)
//
//		if len(getHostname) == 2 {
//			// Return an empty connection. This won't have any reconnect attached it it
//			host = getHostname[1]
//		}
//	}
//
//	return
//}

func getConnector(tx *sql.Tx) (dsn string) {
	dsn = "unknown"

	v := reflect.ValueOf(tx).Elem()
	db := v.FieldByName("db").Elem()

	if db.IsZero() {
		return
	}

	connector := db.FieldByName("connector").Elem()

	if connector.IsZero() {
		return
	}

	dsn = connector.Field(0).String()

	return
}
