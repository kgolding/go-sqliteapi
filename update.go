package sqliteapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (d *Database) UpdateMap(table string, data map[string]interface{}, user User) error {
	logf := func(format string, args ...interface{}) {
		d.debugLog.Printf("updateMap: "+format, args...)
	}

	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
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

	// Handle reference tables
	for _, ref := range d.config.GetBackReferences(table) {
		if jdata, ok := data[ref.SourceTable+RefTableSuffix]; ok {
			if w, ok := data[ref.KeyField]; ok {
				_, err := tx.Exec("DELETE FROM "+ref.SourceTable+" WHERE "+ref.SourceField+"=?", w)
				if err != nil {
					return err
				}

				sdata, err := interfaceToArrayMapStringInterface(jdata)
				if err != nil {
					return err
				}
				for _, data2 := range sdata {
					data2[ref.SourceField] = w
					_, err := d.insertMapWithTx(tx, ref.SourceTable, data2, user)
					if err != nil {
						return fmt.Errorf("%s: %w", ref.SourceTable, err)
					}
				}
			}
		}
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
