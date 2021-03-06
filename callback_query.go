package gorm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

// Define callbacks for querying
func init() {
	DefaultCallback.Query().Register("gorm:query", queryCallback)
	DefaultCallback.Query().Register("gorm:preload", preloadCallback)
	DefaultCallback.Query().Register("gorm:after_query", afterQueryCallback)
}

// queryCallback used to query data from database
func queryCallback(scope *Scope) {
	if _, skip := scope.InstanceGet("gorm:skip_query_callback"); skip {
		return
	}

	//we are only preloading relations, dont touch base model
	if _, skip := scope.InstanceGet("gorm:only_preload"); skip {
		return
	}

	defer scope.trace(NowFunc())

	var (
		isSlice, isPtr bool
		resultType     reflect.Type
		results        = scope.IndirectValue()
	)

	if orderBy, ok := scope.Get("gorm:order_by_primary_key"); ok {
		if primaryField := scope.PrimaryField(); primaryField != nil {
			scope.Search.Order(fmt.Sprintf("%v.%v %v", scope.QuotedTableName(), scope.Quote(primaryField.DBName), orderBy))
		}
	}

	if value, ok := scope.Get("gorm:query_destination"); ok {
		results = indirect(reflect.ValueOf(value))
	}

	if kind := results.Kind(); kind == reflect.Slice {
		isSlice = true
		resultType = results.Type().Elem()
		results.Set(reflect.MakeSlice(results.Type(), 0, 0))

		if resultType.Kind() == reflect.Ptr {
			isPtr = true
			resultType = resultType.Elem()
		}
	} else if kind != reflect.Struct {
		scope.Err(errors.New("unsupported destination, should be slice or struct"))
		return
	}

	scope.prepareQuerySQL()

	if !scope.HasError() {
		scope.db.RowsAffected = 0

		if str, ok := scope.Get("gorm:query_hint"); ok {
			scope.SQL = fmt.Sprint(str) + scope.SQL
		}

		if str, ok := scope.Get("gorm:query_option"); ok {
			scope.SQL += addExtraSpaceIfExist(fmt.Sprint(str))
		}

		switch scope.db.db.(type) {
		case *sql.Tx:
			db := scope.SQLDB().(*sql.Tx)
			scope.Host = "master"
			scope.CacheResult = "tx"

			if rows, err := db.QueryContext(scope.DB().context, scope.SQL, scope.SQLVars...); scope.Err(err) == nil {
				defer rows.Close()

				columns, _ := rows.Columns()
				for rows.Next() {
					scope.db.RowsAffected++

					elem := results
					if isSlice {
						elem = reflect.New(resultType).Elem()
					}

					scope.scan(rows, columns, scope.New(elem.Addr().Interface()).Fields())

					if isSlice {
						if isPtr {
							results.Set(reflect.Append(results, elem.Addr()))
						} else {
							results.Set(reflect.Append(results, elem))
						}
					}
				}

				if err := rows.Err(); err != nil {
					scope.Err(err)
				} else if scope.db.RowsAffected == 0 && !isSlice {
					scope.Err(ErrRecordNotFound)
				}
			}
		case *ConnectionManager:
			// Work out if we can return a result from cache
			cacheOperation := scope.Cache()
			syncOperation := scope.Sync()

			writeToCache := false
			readFromDB := true

			key := fmt.Sprint(scope.SQL, scope.SQLVars)
			scope.HostType = scope.SQLDB().(*ConnectionManager).serverType

			cacheType := "not"

			if syncOperation {
				cacheType = "sync"
			} else if cacheOperation != nil {
				// If the time is > 0, simply provide the cached results
				if *cacheOperation > 0 || *cacheOperation == -1 {
					cacheResults, err := scope.CacheStore().GetItem(key, *cacheOperation)
					if cacheResults != nil {
						scope.Err(err) // Add any error if exists
						results.Set(reflect.ValueOf(cacheResults))

						switch reflect.ValueOf(cacheResults).Type().Kind() {
						case reflect.Struct:
							scope.db.RowsAffected = 1
						case reflect.Slice:
							scope.db.RowsAffected = int64(reflect.ValueOf(cacheResults).Len())
						}

						cacheType = "hit"
						scope.Host = "memory-cache"
						readFromDB = false
					} else {
						readFromDB = true
						cacheType = "miss"
						writeToCache = true
					}
				} else {
					cacheType = "refresh"
					readFromDB = true
					writeToCache = true
				}
			}
			scope.CacheResult = cacheType

			if readFromDB {
				if rows, h, err := scope.SQLDB().(*ConnectionManager).QueryHost(scope.db.context, syncOperation, scope.SQL, scope.SQLVars...); scope.Err(err) == nil {
					defer func() { scope.Host = h }()
					defer rows.Close()

					columns, _ := rows.Columns()
					for rows.Next() {
						scope.db.RowsAffected++

						elem := results
						if isSlice {
							elem = reflect.New(resultType).Elem()
						}

						scope.scan(rows, columns, scope.New(elem.Addr().Interface()).Fields())

						if isSlice {
							if isPtr {
								results.Set(reflect.Append(results, elem.Addr()))
							} else {
								results.Set(reflect.Append(results, elem))
							}
						}
					}

					if err := rows.Err(); err != nil {
						scope.Err(err)
					} else if scope.db.RowsAffected == 0 && !isSlice {
						scope.Err(ErrRecordNotFound)
					}
				}
			}

			// If we're allowed, write the results to the cache
			if writeToCache {
				scope.CacheStore().StoreItem(key, results.Interface(), scope.db.Error)
			}
		}
	}
}

// afterQueryCallback will invoke `AfterFind` method after querying
func afterQueryCallback(scope *Scope) {
	if !scope.HasError() {
		scope.CallMethod("AfterFind")
	}
}
