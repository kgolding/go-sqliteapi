package sqliteapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

var ErrUnknownKey = errors.New("unknown key")
var ErrUnknownTable = errors.New("unknown table/view")

// InsertMap inserts the map including referenced table (xxx_RefTable), and updates
// data["id"] field if there is an autoincrement primary key
func (d *Database) InsertMap(table string, data map[string]interface{}, user User) (int64, error) {
	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		return 0, err
	}

	err = d.runHooks(table, HookParams{table, data, HookBeforeInsert, tx, user})
	if err != nil {
		d.log.Printf("error running before before hook: %s", err)
		return 0, err
	}

	id, err := d.insertMapWithTx(tx, table, data, user)

	if err != nil {
		tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	err = d.runHooks(table, HookParams{table, data, HookAfterInsert, tx, user})
	if err != nil {
		d.log.Printf("error running before before hook: %s", err)
		return 0, err
	}

	return id, nil
}

func (d *Database) insertMapWithTx(tx *sqlx.Tx, table string, data map[string]interface{}, user User) (int64, error) {
	logf := func(format string, args ...interface{}) {
		d.debugLog.Printf("insertMap: "+format, args...)
	}

	// tableFields, err := d.CheckTableNameGetFields(table)
	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		// logf("error fetching table info: %s", err)
		return 0, fmt.Errorf("unknown table name '%s'", table)
	}

	// Create an array of fields and an equal array of values to use as args
	fields := make([]string, 0)
	values := make([]interface{}, 0)

	// Unused data fields which we'll check later for joined tables
	unusedDataFields := make([]string, 0)

	var err error

	// Populate the fields & args arrays
	for k, v := range data {
		used := false
		for _, f := range tableInfo.Fields {
			if f.Name == k {
				used = true
				if f.PrimaryKey > 0 && tableInfo.IsPrimaryKeyId {
					continue // Do insert as autoinc value
				}
				if d.IsFieldWritable(table, k) { // And are writable
					err = d.FieldValidation(table, k, v)
					if err != nil {
						return 0, err
					}
					fields = append(fields, k)
					switch v.(type) {
					case []interface{}:
						values = append(values, "")
					default:
						values = append(values, v)
					}
				}
			}
		}
		if !used {
			unusedDataFields = append(unusedDataFields, k)
		}
	}

	if len(fields) == 0 {
		return 0, errors.New("no values to store")
	}

	sql := "INSERT INTO `" + table + "`"
	sql += " (" + strings.Join(fields, ",") + ")"
	sql += " VALUES (?" + strings.Repeat(",?", len(values)-1) + ")"

	logf("SQL: %s\nArgs: %v\n", sql, values)

	res, err := tx.Exec(sql, values...)
	if err != nil {
		logf("error executing sql: %s", err)
		return 0, err
	}

	v, err := res.LastInsertId()
	id := v
	if err == nil {
		data["id"] = id
	}

	// Handle joined tables if data exists
	if d.config != nil {
		// logf("unused fields: %s", unusedDataFields)
		for _, k := range unusedDataFields {
			// See if the field name is a table e.g. "sessionThing_RefTable"
			// logf("test : %s: %t", k, strings.HasSuffix(k, RefTableSuffix))
			if strings.HasSuffix(k, RefTableSuffix) {
				// logf("got join table field: %s", k)
				if t := d.config.GetTable(strings.TrimSuffix(k, RefTableSuffix)); t != nil {
					// logf("got join table %s", t.Name)
					for _, f := range t.Fields {
						logf("link table %s, checking field %s: %s vs %s.", t.Name, f.Name, f.References, table)
						if strings.HasPrefix(f.References, table+".") { // Target table has a ref to the main table
							ref, err := NewReference(f.References)
							if err != nil {
								logf("bad reference '%s' in joined table %s", f.References, t.Name)
								continue
							}
							// logf("link table %s.%s: %s", t.Name, f.Name, ref)

							// Clear out existing records
							tx.Exec("DELETE FROM `"+t.Name+"` WHERE `"+ref.KeyField+`" = ?`, data[ref.KeyField])

							sdata, err := interfaceToArrayMapStringInterface(data[k])
							if err != nil {
								return 0, err
							}
							for _, data2 := range sdata {
								data2[f.Name] = data[ref.KeyField]
								_, err := d.insertMapWithTx(tx, t.Name, data2, user)
								if err != nil {
									logf("%s: %s", f.References, err)
									return 0, fmt.Errorf("%s: %w", t.Name, err)
								}
							}
						}
					}
				}
			}
		}
	}

	return id, nil
}

func interfaceToArrayMapStringInterface(v interface{}) ([]map[string]interface{}, error) {
	switch x := v.(type) {
	case []map[string]interface{}:
		return x, nil

	case []interface{}:
		ret := make([]map[string]interface{}, len(x))
		for i, y := range x {
			if z, ok := y.(map[string]interface{}); ok {
				ret[i] = z
			} else {
				return nil, fmt.Errorf("unknown data type in element %d: %T", i, v)
			}
		}
		return ret, nil
	}
	return nil, fmt.Errorf("unknown data type %T", v)
}
