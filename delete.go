package sqliteapi

import (
	"context"

	"github.com/jmoiron/sqlx"
)

func (d *Database) Delete(table string, key interface{}, user User) error {
	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		return ErrUnknownKey
	}

	var err error

	defer func() {
		if err != nil {
			d.log.Printf("%s: Error deleting row where %s = '%v': %v", table, tableInfo.GetPrimaryKey().Field, key, err)
		}
	}()

	var tx *sqlx.Tx
	ctx, _ := context.WithTimeout(context.Background(), d.timeout)
	tx, err = d.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	data, err := d.GetMap(table, key, false)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = d.runHooks(table, HookParams{table, key, data, HookBeforeDelete, tx, user})
	if err != nil {
		d.log.Printf("error running before delete hook: %s", err)
		tx.Rollback()
		return err
	}

	for _, ref := range d.config.GetBackReferences(table) {
		skey, ok := data[ref.KeyField]
		if ok {
			sql := "DELETE FROM `" + ref.SourceTable + "`"
			sql += " WHERE " + ref.SourceField + "=?"

			_, err := tx.Exec(sql, skey)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	sql := "DELETE FROM `" + table + "`"
	sql += " WHERE " + tableInfo.GetPrimaryKey().Field + "=?"

	res, err := tx.Exec(sql, key)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	v, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}

	if v == 0 {
		return ErrUnknownKey
	}

	err = d.runHooks(table, HookParams{table, key, data, HookAfterDelete, nil, user})
	if err != nil {
		d.log.Printf("error running after delete hook: %s", err)
		return err
	}

	d.log.Printf("%s: Deleted row where %s = '%v'", table, tableInfo.GetPrimaryKey().Field, key)

	return nil
}
