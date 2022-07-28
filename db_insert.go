package gdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrUnknownKey = errors.New("unknown key")

func (d *Database) insertMap(table string, data map[string]interface{}, user User) (int, error) {
	logf := func(format string, args ...interface{}) {
		d.log.Printf("insertMap: "+format, args...)
	}

	// tableFields, err := d.CheckTableNameGetFields(table)
	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		// logf("error fetching table info: %s", err)
		return 0, fmt.Errorf("unknown table name '%s'", table)
	}

	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		logf("error starting transaction: %s", err)
		return 0, err
	}

	err = d.runHooks(table, HookParams{data, HookBeforeInsert, tx, user})
	if err != nil {
		logf("error running before before hook: %s", err)
		tx.Rollback()
		return 0, err
	}

	fields := make([]string, 0)
	values := make([]interface{}, 0)
	for k, v := range data {
		for _, f := range tableInfo.Fields {
			if f.Name == k {
				if f.PrimaryKey > 0 && tableInfo.IsPrimaryKeyId {
					continue // Do insert an autoinc value
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
	}

	if len(fields) == 0 {
		tx.Rollback()
		return 0, errors.New("no values to store")
	}

	sql := "INSERT INTO `" + table + "`"
	sql += " (" + strings.Join(fields, ",") + ")"
	sql += " VALUES (?" + strings.Repeat(",?", len(values)-1) + ")"

	logf("SQL: %s", sql)

	res, err := tx.Exec(sql, values...)
	if err != nil {
		logf("error executing sql: %s", err)
		tx.Rollback()
		return 0, err
	}

	v, _ := res.LastInsertId()
	id := int(v)
	data["id"] = id

	err = d.runHooks(table, HookParams{data, HookAfterInsert, tx, user})
	if err != nil {
		logf("error running after insert hook: %s", err)
		tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		logf("error commiting: %s", err)
		return 0, err
	}

	return int(id), nil
}

func (d *Database) updateMap(table string, data map[string]interface{}, user User) error {
	logf := func(format string, args ...interface{}) {
		d.log.Printf("updateMap: "+format, args...)
	}

	tableFields, err := d.CheckTableNameGetFields(table)
	if err != nil {
		logf("error fetching table info: %s", err)
		return err
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
		for _, tf := range tableFields {
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
		d.log.Printf("updateMap: "+format, args...)
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

	return nil
}
