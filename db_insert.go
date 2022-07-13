package gdb

import (
	"context"
	"errors"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var ErrUnknownID = errors.New("unknown ID")

func (d *Database) insertMap(table string, data map[string]interface{}, user User) (int, error) {
	tableFields, err := d.CheckTableNameGetFields(table)
	if err != nil {
		log.WithError(err).Error("Insert MAP - error fetching table info")
		return 0, err
	}

	// d.Lock()
	// defer d.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		log.WithError(err).Error("Insert MAP - error starting transaction")
		return 0, err
	}

	err = d.runHooks(table, HookParams{data, HookBeforeInsert, tx, user})
	if err != nil {
		log.WithError(err).Error("Insert MAP - error running hook")
		tx.Rollback()
		return 0, err
	}

	fields := make([]string, 0)
	values := make([]interface{}, 0)
	for k, v := range data {
		for _, f := range tableFields {
			if f.Name == k {
				// if _, ok := tableFields[k]; ok { // Only save fields that exist in the table
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

	log.WithField("SQL", sql).Info("Insert MAP")

	res, err := tx.Exec(sql, values...)
	if err != nil {
		log.WithError(err).Error("Insert MAP")
		tx.Rollback()
		return 0, err
	}

	v, _ := res.LastInsertId()
	id := int(v)
	data["id"] = id

	err = d.runHooks(table, HookParams{data, HookAfterInsert, tx, user})
	if err != nil {
		log.WithError(err).Error("Insert MAP - error running hook")
		tx.Rollback()
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		log.WithError(err).Error("Insert MAP - error committing")
		return 0, err
	}

	return int(id), nil
}

func (d *Database) updateMap(table string, data map[string]interface{}, user User) error {
	tableFields, err := d.CheckTableNameGetFields(table)
	if err != nil {
		log.WithError(err).Error("Insert MAP - error fetching table info")
		return err
	}

	// d.Lock()
	// defer d.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		log.WithError(err).Error("Update MAP - error starting transaction")
		return err
	}

	err = d.runHooks(table, HookParams{data, HookBeforeUpdate, tx, user})
	if err != nil {
		log.WithError(err).Error("Update MAP - error running hook")
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

	log.WithField("SQL", sql).WithField("Values", fieldValues).Info("Update MAP")

	_, err = tx.Exec(sql, append(fieldValues, pkValues...)...)
	if err != nil {
		log.WithError(err).Error("Update MAP - error updating")
		tx.Rollback()
		return err
	}

	err = d.runHooks(table, HookParams{data, HookAfterUpdate, tx, user})
	if err != nil {
		log.WithError(err).Error("Update MAP - error running hook")
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.WithError(err).Error("Update MAP - error committing")
		tx.Rollback()
		return err
	}

	return nil
}

func (d *Database) delete(table string, id int, user User) error {
	_, err := d.CheckTableNameGetFields(table)
	if err != nil {
		log.WithError(err).Error("Delete - error fetching table info")
		return err
	}

	// d.Lock()
	// defer d.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	tx, err := d.DB.BeginTxx(ctx, nil)
	if err != nil {
		log.WithError(err).Error("Delete row - error starting transaction")
		return err
	}

	data := map[string]interface{}{
		"id": id,
	}
	err = d.runHooks(table, HookParams{data, HookBeforeDelete, tx, user})
	if err != nil {
		log.WithError(err).Error("Delete row - error running hook")
		tx.Rollback()
		return err
	}

	sql := "DELETE FROM `" + table + "`"
	sql += " WHERE id=?"

	log.WithField("SQL", sql).WithField("ID", id).Info("Delete row")

	res, err := tx.Exec(sql, id)
	if err != nil {
		log.WithError(err).Error("Delete row - error updating")
		tx.Rollback()
		return err
	}

	err = d.runHooks(table, HookParams{data, HookAfterDelete, tx, user})
	if err != nil {
		log.WithError(err).Error("Delete row - error running hook")
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.WithError(err).Error("Delete row - error committing")
		tx.Rollback()
		return err
	}

	v, err := res.RowsAffected()
	if err != nil {
		log.WithError(err).Error("Delete row - error committing")
		tx.Rollback()
		return err
	}

	if v == 0 {
		return ErrUnknownID
	}

	return nil
}
