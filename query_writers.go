package gdb

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// queryJsonArrayWriter runs the query and streams the result as a json array of arrays
// to the given Writer
func (d *Database) QueryJsonArrayWriter(w io.Writer, query string, args []interface{}) error {
	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // This is a query so we always rollback

	rows, err := tx.Queryx(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	w.Write([]byte("["))
	addComma := false // we prefix with a comma when it's not the first row

	for rows.Next() {
		ret, err := rows.SliceScan()
		if err != nil {
			return err
		}

		if addComma {
			w.Write([]byte(","))
		} else {
			addComma = true
		}
		b, err := json.Marshal(ret)
		if err != nil {
			return err
		}
		w.Write(b)
	}
	w.Write([]byte("]"))

	return nil
}

// queryJsonWriter runs the query and streams the result as json to the given Writer
func (d *Database) QueryJsonWriter(w io.Writer, query string, args []interface{}) error {
	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // This is a query so we always rollback

	rows, err := tx.Queryx(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	w.Write([]byte("["))
	addComma := false // we prefix with a comma when it's not the first row

	for rows.Next() {
		ret := make(map[string]interface{})
		err := rows.MapScan(ret)
		if err != nil {
			return err
		}

		if addComma {
			w.Write([]byte(","))
		} else {
			addComma = true
		}
		b, err := json.Marshal(ret)
		if err != nil {
			return err
		}
		w.Write(b)
	}
	w.Write([]byte("]"))

	return nil
}

// queryCsvWriter runs the query and streams the result as csv to the given Writer
func (d *Database) QueryCsvWriter(w io.Writer, query string, args []interface{}) error {
	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // This is a query so we always rollback

	rows, err := tx.Queryx(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	csv := csv.NewWriter(w)
	err = csv.Write(cols)
	if err != nil {
		return err
	}

	for rows.Next() {
		ret := make(map[string]interface{})
		err := rows.MapScan(ret)
		if err != nil {
			return err
		}

		row := []string{}
		for _, c := range cols {
			v, ok := ret[c]
			if ok && v != nil {
				row = append(row, fmt.Sprintf("%v", v))
			} else {
				row = append(row, "")
			}
		}
		err = csv.Write(row)
		if err != nil {
			return err
		}
	}
	csv.Flush()

	return nil
}

// queryJsonWriter runs the query and streams the result as json to the given Writer
func (d *Database) queryJsonWriterRow(w io.Writer, query string, args []interface{}) error {
	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // This is a query so we always rollback

	row := tx.QueryRowx(query, args...)
	if row == nil {
		return sql.ErrNoRows
	}

	ret := make(map[string]interface{})

	err = row.MapScan(ret)
	if err != nil {
		return err
	}

	b, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	w.Write(b)
	return nil
}
