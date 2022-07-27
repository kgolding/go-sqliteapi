package gdb

import (
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
)

type BuildQueryConfig struct {
	Table     string
	Key       string // Optional
	FieldsCSV string
	Search    string
	Sort      string
	Offset    uint
	Limit     uint
}

func BuildQueryConfigFromRequest(r *http.Request, withKey bool) (BuildQueryConfig, error) {

	GetQueryUint := func(param string, defValue uint) (uint, error) {
		if v := r.URL.Query().Get(param); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return defValue, err
			}
			if n < 0 {
				return defValue, fmt.Errorf("not a positive number '%s'", v)
			}
			return uint(n), nil
		}
		return defValue, nil
	}

	var err error

	bqc := BuildQueryConfig{
		FieldsCSV: r.URL.Query().Get("select"),
		Search:    r.URL.Query().Get("search"),
		Sort:      r.URL.Query().Get("sort"),
	}

	bqc.Table = path.Base(r.URL.Path)
	if withKey {
		bqc.Key = bqc.Table
		bqc.Table = path.Base(path.Dir(r.URL.Path))
	}

	bqc.Offset, err = GetQueryUint("offset", 0)
	if err != nil {
		return bqc, err
	}

	bqc.Limit, err = GetQueryUint("limit", 1000)
	if err != nil {
		return bqc, err
	}

	return bqc, nil
}

func (d *Database) BuildQuery(c BuildQueryConfig) (string, []interface{}, error) {

	if !regName.MatchString(c.Table) {
		return "", nil, fmt.Errorf("invalid table name '%s'", c.Table)
	}

	resultCols, err := d.SanitiseSelectByTable(c.FieldsCSV, c.Table)
	if err != nil {
		return "", nil, err
	}

	joins := make([]string, 0)

	// Add joins, relies on config existing to get field refs
	if d.config != nil {
		if ct := d.config.GetTable(c.Table); ct != nil {
			for _, rc := range resultCols {
				for _, f := range ct.Fields {
					if rc.Field == f.Name && f.References != "" {
						ref, err := NewReference(f.References)
						if err == nil {
							resultCols = append(resultCols, ref.ResultColLabel(rc.Field+"_RefLabel"))
							joins = append(joins, fmt.Sprintf(
								"LEFT OUTER JOIN `%s` ON %s = %s",
								ref.Table, ref.ResultColKey("").String(), rc.String()))
						}
					}
				}
			}
		}
	}

	args := make([]interface{}, 0)

	q := "SELECT " + resultCols.String() + "\n\tFROM `" + c.Table + "`"

	// JOINS
	if len(joins) > 0 {
		q += "\n\t" + strings.Join(joins, "\n\t")
	}

	// WHERE
	if c.Key != "" {
		if ti := d.dbInfo.GetTableInfo(c.Table); ti != nil {
			q += "\n\tWHERE " + ti.GetPrimaryKey().String() + " = ?"
			args = append(args, c.Key)
		}
	} else {
		if c.Search != "" {
			conditions := make([]string, 0)
			for _, rc := range resultCols {
				conditions = append(conditions, rc.String()+" LIKE ?")
				args = append(args, c.Search)
			}
			q += "\n\tWHERE " + strings.Join(conditions, "\n\t\tOR ")
		}
	}

	// TO FIX @TODO
	// if where != "" {
	// 	q += "\nWHERE " + where
	// }

	// ORDER BY
	if c.Sort != "" {
		q += "\nORDER BY " + c.Sort
	}

	// LIMIT/OFFSET
	if c.Limit > 0 || c.Offset > 0 {
		q += fmt.Sprintf("\n\tLIMIT %d, %d", c.Offset, c.Limit)
	}

	return q, args, nil
}
