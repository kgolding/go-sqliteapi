package gdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

var ErrUnknownKey = errors.New("unknown key")
var ErrUnknownTable = errors.New("unknown table/view")

func (d *Database) insertMap(table string, data map[string]interface{}, user User) (int, error) {
	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
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

	return id, nil
}

func (d *Database) insertMapWithTx(tx *sqlx.Tx, table string, data map[string]interface{}, user User) (int, error) {
	logf := func(format string, args ...interface{}) {
		d.debugLog.Printf("insertMap: "+format, args...)
	}

	// tableFields, err := d.CheckTableNameGetFields(table)
	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		// logf("error fetching table info: %s", err)
		return 0, fmt.Errorf("unknown table name '%s'", table)
	}

	err := d.runHooks(table, HookParams{data, HookBeforeInsert, tx, user})
	if err != nil {
		logf("error running before before hook: %s", err)
		return 0, err
	}

	// Create an array of fields and an equal array of values to use as args
	fields := make([]string, 0)
	values := make([]interface{}, 0)

	// Unused data fields which we'll check for joined tables
	unusedDataFields := make([]string, 0)
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

	logf("SQL: %s", sql)

	res, err := tx.Exec(sql, values...)
	if err != nil {
		logf("error executing sql: %s", err)
		return 0, err
	}

	v, err := res.LastInsertId()
	id := int(v)
	if err == nil {
		data["id"] = id
	}

	// Handle joined tables if data exists
	if d.config != nil {
		for _, k := range unusedDataFields {
			// See if the field name is a table e.g. "sessionThing"
			if t := d.config.GetTable(k); t != nil {
				logf("got join table %s", t.Name)
				for _, f := range t.Fields {
					logf("link table %s, checking field %s: %s vs %s.", t.Name, f.Name, f.References, table)
					if strings.HasPrefix(f.References, table+".") { // Target table has a ref to the main table
						ref, err := NewReference(f.References)
						if err != nil {
							logf("bad reference '%s' in joined table %s", f.References, t.Name)
							continue
						}
						logf("link table %s.%s: %s", t.Name, f.Name, ref)
						// Clear out existing records

						tx.Exec("DELETE FROM `"+t.Name+"` WHERE `"+ref.KeyField+`" = ?`, data[ref.KeyField])
						// We might have an array of id's or an array of map[string]interface{}
						switch x := data[k].(type) {
						case []map[string]interface{}:
							for _, data2 := range x {
								data2[f.Name] = data[ref.KeyField]
								// fmt.Printf("f.Name: %s, ref.KeyField: %v, data: %#v\n", f.Name, ref.KeyField, data)
								// fmt.Printf("INSERT JOIN TABLE '%s': %#v\n", t.Name, data2)
								_, err := d.insertMapWithTx(tx, t.Name, data2, user)
								if err != nil {
									logf("%s: %s", f.References, err)
								}
							}

						// case []interface{}: // simple array of key values
						// 	xFields := []string{ref.ResultColKey().String()} // e.g. sessionThing.id
						// 	xValues := []interface{}{data[ref.KeyField]}     // e.g. id

						// 	sql := "INSERT INTO `" + t.Name + "` "
						// 	sql += "(" + ref.ResultColKey().String() + ", "

						default:
							logf("unknown data type for %T for %s", x, k)
						}
					}
				}
			}
		}
	}

	err = d.runHooks(table, HookParams{data, HookAfterInsert, tx, user})
	if err != nil {
		logf("error running after insert hook: %s", err)
		return 0, err
	}

	return int(id), nil
}

func (d *Database) updateMap(table string, data map[string]interface{}, user User) error {
	logf := func(format string, args ...interface{}) {
		d.log.Printf("updateMap: "+format, args...)
	}

	tableInfo, ok := d.dbInfo[table]
	if !ok {
		return ErrUnknownTable
	}

	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		logf("error starting transaction: %s", err)
		return err
	}

	err = d.runHooks(table, HookParams{data, HookBeforeUpdate, tx, user})
	if err != nil {
		logf("error running before insert hook: %s", err)
		tx.Rollback()
		return err
	}

	fields := []string{}           // Fields to set
	pks := []string{}              // Primary keys
	fieldValues := []interface{}{} // The values to fill in the ?'s
	pkValues := []interface{}{}    // The values to fill in the ?'s
	for k, v := range data {
		for _, tf := range tableInfo.Fields {
			if tf.Name == k {
				// if tf, ok := tableFields[k]; ok { // Only save fields that exist in the table
				if tf.PrimaryKey > 0 {
					pkValues = append(pkValues, v)
					pks = append(pks, k)
				} else if d.IsFieldWritable(table, k) { // And are writable
					err = d.FieldValidation(table, k, v)
					if err != nil {
						return err
					}
					fieldValues = append(fieldValues, v)
					fields = append(fields, k)
				}
			}
		}
	}
	if len(fields) == 0 {
		tx.Rollback()
		return errors.New("no values to store")
	}
	if len(pks) == 0 {
		tx.Rollback()
		return errors.New("no primary key fields")
	}
	// @todo check enough pk's?

	sql := "UPDATE `" + table + "`"
	sql += " SET " + strings.Join(fields, "=?,") + "=?"
	sql += " WHERE " + strings.Join(pks, "=? AND ") + "=?"

	args := append(fieldValues, pkValues...)

	logf("SQL: %s\nArgs: %s", sql, args)

	res, err := tx.Exec(sql, args...)
	if err != nil {
		logf("error executing sql: %s", err)
		tx.Rollback()
		return err
	}
	if i, _ := res.RowsAffected(); i != 1 {
		logf("")
		tx.Rollback()
		return ErrUnknownKey
	}

	err = d.runHooks(table, HookParams{data, HookAfterUpdate, tx, user})
	if err != nil {
		logf("error running after hook: %s", err)
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		logf("error committing: %s", err)
		tx.Rollback()
		return err
	}

	return nil
}

func (d *Database) delete(table string, key interface{}, user User) error {
	logf := func(format string, args ...interface{}) {
		d.debugLog.Printf("updateMap: "+format, args...)
	}

	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		return ErrUnknownKey
	}

	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		logf("error starting transaction: %s", err)
		return err
	}

	// err = d.runHooks(table, HookParams{data, HookBeforeDelete, tx, user})
	// if err != nil {
	// 	logf("error running before hook: %s", err)
	// 	tx.Rollback()
	// 	return err
	// }

	sql := "DELETE FROM `" + table + "`"
	sql += " WHERE " + tableInfo.GetPrimaryKey().Field + "=?"

	logf("SQL: %s", sql)

	res, err := tx.Exec(sql, key)
	if err != nil {
		logf("error executing sql: %s", err)
		tx.Rollback()
		return err
	}

	// err = d.runHooks(table, HookParams{data, HookAfterDelete, tx, user})
	// if err != nil {
	// 	logf("error running after hook: %s", err)
	// 	tx.Rollback()
	// 	return err
	// }

	err = tx.Commit()
	if err != nil {
		logf("error committing: %s", err)
		tx.Rollback()
		return err
	}

	v, err := res.RowsAffected()
	if err != nil {
		logf("unable to read rows affected: %s", err)
		tx.Rollback()
		return err
	}

	if v == 0 {
		return ErrUnknownKey
	}

	d.log.Printf("%s: Delete row where %s = '%v'", table, tableInfo.GetPrimaryKey().Field, key)

	return nil
}
