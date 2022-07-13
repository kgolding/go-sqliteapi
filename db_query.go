package gdb

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// queryToMap runs the query and returns an array of rows
// func (d *Database) queryToMap(query string, args ...interface{}) ([]map[string]interface{}, error) {
// 	// an array of JSON objects
// 	// the map key is the field name
// 	var objects []map[string]interface{}

// 	rows, err := d.DB.Query(query, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		// figure out what columns were returned
// 		// the column names will be the JSON object field keys
// 		columns, err := rows.ColumnTypes()
// 		if err != nil {
// 			return nil, err
// 		}

// 		// Scan needs an array of pointers to the values it is setting
// 		// This creates the object and sets the values correctly
// 		values := make([]interface{}, len(columns))
// 		object := map[string]interface{}{}
// 		for i, column := range columns {
// 			v := new(interface{})
// 			scanType := column.ScanType()
// 			if scanType != nil { // Deal with null values
// 				if t := reflect.New(scanType).Interface(); t != nil {
// 					v = &t
// 				}
// 			}
// 			object[column.Name()] = v
// 			values[i] = object[column.Name()]
// 		}

// 		err = rows.Scan(values...)
// 		if err != nil {
// 			return nil, err
// 		}

// 		objects = append(objects, object)
// 	}

// 	return objects, nil
// }

// queryJsonWriter runs the query and streams the result as json to the given Writer
func (d *Database) QueryJsonWriter(w io.Writer, query string, args ...interface{}) error {
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
func (d *Database) QueryCsvWriter(w io.Writer, query string, args ...interface{}) error {
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
	// for _, f := range d.secretFields {
	// 	for i, c := range cols {
	// 		if f == c {
	// 			cols = append(cols[:i], cols[:i+1]...)
	// 		}
	// 	}
	// }

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
		// d.removeSecretFields(ret)

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
func (d *Database) queryJsonWriterRow(w io.Writer, query string, args ...interface{}) error {
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

// queryWriter runs the query returns the result
// func (d *Database) QueryRow(query string, args ...interface{}) (map[string]interface{}, error) {
// 	rows, err := d.DB.Query(query, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()
// 	if rows.Next() {
// 		// figure out what columns were returned
// 		// the column names will be the JSON object field keys
// 		columns, err := rows.ColumnTypes()
// 		if err != nil {
// 			return nil, err
// 		}

// 		// Scan needs an array of pointers to the values it is setting
// 		// This creates the object and sets the values correctly
// 		values := make([]interface{}, len(columns))
// 		object := map[string]interface{}{}
// 		for i, column := range columns {
// 			v := new(interface{})
// 			scanType := column.ScanType()
// 			if scanType != nil { // Deal with null values
// 				if t := reflect.New(scanType).Interface(); t != nil {
// 					v = &t
// 				}
// 			}
// 			object[column.Name()] = v
// 			values[i] = object[column.Name()]
// 		}

// 		err = rows.Scan(values...)
// 		if err != nil {
// 			return nil, err
// 		}

// 		return object, nil

// 	}

// 	return nil, sql.ErrNoRows
// }
