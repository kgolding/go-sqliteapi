package sqliteapi

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

func (d *Database) Delete(table string, key interface{}, user User) (err error) {
	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		return ErrUnknownTable
	}

	defer func() {
		if err != nil {
			d.log.Printf("%s: error deleting row where %s = '%v': %v", table, tableInfo.GetPrimaryKey().Field, key, err)
		}
	}()

	var tx *sqlx.Tx
	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err = d.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	var data map[string]interface{}
	data, err = d.GetMap(table, key, false)
	if err != nil {
		tx.Rollback()
		return
	}

	err = d.runHooks(table, HookParams{table, key, data, HookBeforeDelete, tx, user})
	if err != nil {
		d.log.Printf("error running before delete hook: %s", err)
		tx.Rollback()
		return
	}

	// @TODO replace this in the FOREIGN KEY??
	for _, ref := range d.config.GetBackReferences(table) {
		skey, ok := data[ref.KeyField]
		if ok {
			sql := "DELETE FROM `" + ref.SourceTable + "`"
			sql += " WHERE " + ref.SourceField + "=?"
			d.debugLog.Printf("SQL: %s\nArgs: %v\n", sql, skey)
			_, err = tx.Exec(sql, skey)
			if err != nil {
				tx.Rollback()
				return
			}
		}
	}

	q := "DELETE FROM `" + table + "`"
	q += " WHERE " + tableInfo.GetPrimaryKey().Field + "=?"
	d.debugLog.Printf("SQL: %s\nArgs: %v\n", q, key)
	var res sql.Result
	res, err = tx.Exec(q, key)
	if err != nil {
		tx.Rollback()
		return
	}

	var v int64
	v, err = res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return
	}
	if v == 0 {
		tx.Rollback()
		return ErrUnknownKey
	}

	err = d.runHooks(table, HookParams{table, key, data, HookAfterDelete, tx, user})
	if err != nil {
		d.log.Printf("error running after delete hook: %s", err)
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return
	}
	d.log.Printf("%s: Deleted row where %s = '%v'", table, tableInfo.GetPrimaryKey().Field, key)

	return nil
}
