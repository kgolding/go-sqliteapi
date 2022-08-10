package gdb

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
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
func (d *Database) queryJsonWriterRow(w io.Writer, sb *SelectBuilder, args []interface{}) error {
	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback() // This is a query so we always rollback

	d.AddRefLabels(sb, "")

	query, err := sb.ToSql()
	if err != nil {
		return err
	}

	d.debugLog.Printf("queryJsonWriterRow: query: %s\nArgs: %s", query, args)

	row := tx.QueryRowx(query, args...)
	if row == nil {
		return sql.ErrNoRows
	}

	ret := make(map[string]interface{})

	err = row.MapScan(ret)
	if err != nil {
		return err
	}

	// For each select field, check to see if it is referenced to from other tables
	d.debugLog.Printf("Select: %s\n", sb.Select)
	d.debugLog.Printf("Config: %s\n", d.config.String())
	for _, table2 := range d.config.Tables {
		// d.debugLog.Printf("AAAAAAAAAAAAAAAAA. table: %s, field: %s, table2: %s\n", table, field, table2.Name)
		for _, field2 := range table2.Fields {
			if field2.References != "" {
				ref, err := NewReference(field2.References)
				if ref.Table == sb.From {
					// d.debugLog.Printf("B. ref: %#v, err: %v\n", ref, err)
					if err == nil {
						ssb := &SelectBuilder{
							From:  table2.Name,
							Where: []string{tableFieldWrapped(table2.Name, field2.Name) + "=?"},
						}
						d.AddRefLabels(ssb, sb.From)
						query, err := ssb.ToSql()
						if err != nil {
							return err
						}
						subArgs := make([]interface{}, 0)
						subArgs = append(subArgs, ret[ref.KeyField])
						// d.debugLog.Printf("C. Sub-query: %s\nArgs: %v", query, subArgs)

						rows, err := tx.Queryx(query, subArgs...)
						if err != nil {
							return fmt.Errorf("sub-query '%s': %w", table2.Name, err)
						}

						subRet := make([]map[string]interface{}, 0)
						for rows.Next() {
							// d.debugLog.Printf("D. sub-query:\n")
							m := make(map[string]interface{}, 0)
							err = rows.MapScan(m)
							// d.debugLog.Printf("E. sub-query: %v\n", m)
							if err != nil {
								return fmt.Errorf("sub-query '%s': %w", table2.Name, err)
							}
							subRet = append(subRet, m)
							// d.debugLog.Printf("F: sub-query: add %v\n", m)
						}
						refFieldName := table2.Name + RefTableSuffix
						if _, exists := ret[refFieldName]; exists {
							refFieldName = table2.Name + "_" + field2.Name + RefTableSuffix
						}
						// d.debugLog.Printf("G. sub-query: set '%s' = %v\n", refFieldName, subRet)
						ret[refFieldName] = subRet
					}
				}
			}
		}
	}

	b, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	w.Write(b)
	return nil
}

// queryJsonWriter runs the query and streams the result as json to the given Writer
func (d *Database) XqueryJsonWriterRow(w io.Writer, query string, args []interface{}) error {
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

func processFields_JSON(m map[string]interface{}) {
	for k, v := range m {
		if strings.HasSuffix(k, "_JSON") {
			switch q := v.(type) {
			case nil:
				delete(m, k)

			case string:
				var x interface{}
				err := json.Unmarshal([]byte(q), &x)
				if err == nil {
					delete(m, k)
					if x != nil {
						newK := strings.TrimSuffix(k, "_JSON")
						m[newK] = x
					}
				}
			}
		}
	}
}

func (d *Database) AddRefLabels(sb *SelectBuilder, exclTable string) {
	// d.debugLog.Printf("AddRefLabels: sb: %#v\n", sb)
	if ct := d.config.GetTable(sb.From); ct != nil {
		if len(sb.Select) == 0 {
			for _, f := range ct.Fields {
				sb.Select = append(sb.Select, tableFieldWrapped(ct.Name, f.Name))
			}
		}
		// d.debugLog.Printf("AddRefLabels: ct.Fields: %#v\n", ct.Fields)
		for _, selectField := range sb.Select {
			// d.debugLog.Printf("AddRefLabels: selectField: %#v\n", selectField)
			for _, f := range ct.Fields {
				// d.debugLog.Printf("AddRefLabels: f: %#v\n", f)
				if selectField == tableFieldWrapped(sb.From, f.Name) && f.References != "" {
					ref, err := NewReference(f.References)
					if err == nil && ref.LabelField != "" && ref.Table != exclTable {
						sb.Select = append(sb.Select, ref.ResultColLabel(f.Name+RefLabelSuffix).StringAs())
						sb.Joins = append(sb.Joins, Join{
							Type:  LEFT_OUTER,
							Table: ref.Table,
							On: []JoinOn{
								{
									Field:       ref.KeyField,
									ParentField: f.Name,
								},
							},
						})
					}
				}
			}
		}
	}
}
