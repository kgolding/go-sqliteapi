package gdb

import (
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// type BuildQueryConfig struct {
// 	Table            string
// 	PkValue          interface{} // Optional
// 	FieldsCSV        string
// 	Where            string
// 	Search           string // Search all fields
// 	Sort             string
// 	Offset           uint
// 	Limit            uint
// 	IncludeJunctions bool
// 	SubQueries       []SubQuery
// 	// SubBuildQueryConfigs []BuildQueryConfig
// }

// type SubQuery struct {
// 	Name      string
// 	Query     string
// 	ArgFields []string
// }

// BuildQueryConfigFromRequest
// func BuildQueryConfigFromRequest(r *http.Request, withKey bool) (*BuildQueryConfig, error) {

// 	GetQueryUint := func(param string, defValue uint) (uint, error) {
// 		if v := r.URL.Query().Get(param); v != "" {
// 			n, err := strconv.Atoi(v)
// 			if err != nil {
// 				return defValue, err
// 			}
// 			if n < 0 {
// 				return defValue, fmt.Errorf("not a positive number '%s'", v)
// 			}
// 			return uint(n), nil
// 		}
// 		return defValue, nil
// 	}

// 	var err error

// 	bqc := &BuildQueryConfig{
// 		FieldsCSV:        r.URL.Query().Get("select"),
// 		Search:           r.URL.Query().Get("search"),
// 		Sort:             r.URL.Query().Get("sort"),
// 		IncludeJunctions: r.URL.Query().Has("junctions"),
// 		SubQueries:       make([]SubQuery, 0),
// 		// SubBuildQueryConfigs: make([]BuildQueryConfig, 0),
// 	}

// 	bqc.Table = path.Base(r.URL.Path)
// 	if withKey {
// 		bqc.PkValue = bqc.Table
// 		bqc.Table = path.Base(path.Dir(r.URL.Path))
// 	}

// 	bqc.Offset, err = GetQueryUint("offset", 0)
// 	if err != nil {
// 		return bqc, err
// 	}

// 	bqc.Limit, err = GetQueryUint("limit", 1000)
// 	if err != nil {
// 		return bqc, err
// 	}

// 	return bqc, nil
// }

// // BuildQuery takes a BuildQueryConfig and returns a raw SQL query and an array of args
// func (d *Database) BuildQuery(c *BuildQueryConfig) (string, []interface{}, error) {

// 	if !regName.MatchString(c.Table) {
// 		return "", nil, fmt.Errorf("invalid table name '%s'", c.Table)
// 	}

// 	resultCols, err := d.SanitiseSelectByTable(c.FieldsCSV, c.Table)
// 	if err != nil {
// 		return "", nil, err
// 	}

// 	joins := make([]string, 0)

// 	// Add joins, relies on config existing to get field refs
// 	if d.config != nil {
// 		// Get labels for fields that have references
// 		if ct := d.config.GetTable(c.Table); ct != nil {
// 			for _, rc := range resultCols {
// 				for _, f := range ct.Fields {
// 					if rc.Field == f.Name && f.References != "" {
// 						ref, err := NewReference(f.References)
// 						if err == nil && ref.LabelField != "" {
// 							resultCols = append(resultCols, ref.ResultColLabel(rc.Field+"_RefLabel"))
// 							joins = append(joins, fmt.Sprintf(
// 								"LEFT OUTER JOIN `%s` ON %s = %s",
// 								ref.Table, ref.ResultColKey("").String(), rc.String()))
// 						}
// 					}
// 				}
// 			}
// 		}
// 		if c.IncludeJunctions {
// 			// Get id's of junction tables by looking for other tables with references back to our main table
// 			for _, t := range d.config.Tables {
// 				for _, f := range t.Fields {
// 					if strings.HasPrefix(f.References, c.Table+".") {
// 						ref, err := NewReference(f.References)
// 						if err != nil {
// 							continue
// 						}

// 						// @TODO ADD _RefLabels

// 						sq := SubQuery{
// 							Name:      t.Name,
// 							Query:     "SELECT * FROM `" + t.Name + "` ",
// 							ArgFields: []string{ref.KeyField},
// 						}

// 						// Get labels for fields that have references
// 						if ct := d.config.GetTable(c.Table); ct != nil {
// 							for _, rc := range resultCols {
// 								for _, f := range ct.Fields {
// 									if rc.Field == f.Name && f.References != "" {
// 										ref, err := NewReference(f.References)
// 										if err == nil && ref.LabelField != "" {
// 											resultCols = append(resultCols, ref.ResultColLabel(rc.Field+"_RefLabel"))
// 											sq.Query += fmt.Sprintf(
// 												"LEFT OUTER JOIN `%s` ON %s = %s ",
// 												ref.Table, ref.ResultColKey("").String(), rc.String())
// 										}
// 									}
// 								}
// 							}
// 						}
// 						sq.Query += "WHERE `" + f.Name + "`=? "

// 						// sbqc := &BuildQueryConfig{
// 						// 	Table: t.Name,
// 						// 	Where: "`" + f.Name + "`=?",
// 						// }
// 						c.SubQueries = append(c.SubQueries, sq)
// 						// c.SubBuildQueryConfigs = append(c.SubBuildQueryConfigs, sbqc)
// 					}
// 				}
// 			}
// 		}
// 	}

// 	/*
// 		&gdb.BuildQueryConfig{Table:"session", PkValue:"1", FieldsCSV:"", Where:"", Search:"", Sort:"", Offset:0x0, Limit:0x3e8, IncludeJunctions:true,
// 		SubQueries:[]gdb.SubQuery{gdb.SubQuery{Name:"sessionArea", Query:"
// 		SELECT * FROM `sessionArea`
// 		WHERE `sessionId`=?
// 		LEFT OUTER JOIN `driver` ON `driver`.`id` = `session`.`driverId` LEFT OUTER JOIN `vehicletype` ON `vehicletype`.`code` = `session`.`vehicletypeCode`
// 		", ArgFields:[]string{"id"}}, gdb.SubQuery{Name:"sessionThing", Query:"SELECT * FROM `sessionThing` WHERE `sessionId`=? LEFT OUTER JOIN `driver` ON `driver`.`id` = `session`.`driverId` LEFT OUTER JOIN `vehicletype` ON `vehicletype`.`code` = `session`.`vehicletypeCode`", ArgFields:[]string{"id"}}}}

// 	*/

// 	args := make([]interface{}, 0)

// 	q := "SELECT " + resultCols.String() + "\n\tFROM `" + c.Table + "`"

// 	// JOINS
// 	if len(joins) > 0 {
// 		q += "\n\t" + strings.Join(joins, "\n\t")
// 	}

// 	// WHERE
// 	if c.PkValue != nil {
// 		if ti := d.dbInfo.GetTableInfo(c.Table); ti != nil {
// 			q += "\n\tWHERE " + ti.GetPrimaryKey().String() + " = ?"
// 			args = append(args, c.PkValue)
// 		}
// 	} else {
// 		if c.Search != "" {
// 			conditions := make([]string, 0)
// 			for _, rc := range resultCols {
// 				conditions = append(conditions, rc.String()+" LIKE ?")
// 				args = append(args, c.Search)
// 			}
// 			q += "\n\tWHERE " + strings.Join(conditions, "\n\t\tOR ")
// 		}
// 	}

// 	// TO FIX @TODO
// 	// if where != "" {
// 	// 	q += "\nWHERE " + where
// 	// }

// 	// ORDER BY
// 	if c.Sort != "" {
// 		q += "\nORDER BY " + c.Sort
// 	}

// 	// LIMIT/OFFSET
// 	if c.Limit > 0 || c.Offset > 0 {
// 		q += fmt.Sprintf("\n\tLIMIT %d, %d", c.Offset, c.Limit)
// 	}

// 	return q, args, nil
// }

// SanitiseSelectByTable takes a comma seperated list of fields, and returns an
// array of ResultColumn's, removing any hidden fields
// func (d *Database) SanitiseSelectByTable(selectStr string, table string) (ResultColumns, error) {
// 	invalidFields := []string{}
// 	selectArray := make(ResultColumns, 0)

// 	selectStr = strings.TrimSpace(selectStr)
// 	if selectStr == "" || selectStr == "*" {
// 		tableInfo, ok := d.dbInfo[table]
// 		if !ok {
// 			return nil, ErrUnknownTable
// 		}
// 		for _, f := range tableInfo.Fields {
// 			if d.IsFieldReadable(table, f.Name) {
// 				selectArray = append(selectArray, ResultColumn{Table: table, Field: f.Name})
// 			}
// 		}
// 	} else {
// 		for _, f := range strings.Split(selectStr, ",") {
// 			f = strings.TrimSpace(f)
// 			if !regName.MatchString(f) {
// 				invalidFields = append(invalidFields, f)
// 			} else if d.IsFieldReadable(table, f) {
// 				selectArray = append(selectArray, ResultColumn{Table: table, Field: f})
// 			}
// 		}
// 	}
// 	if len(invalidFields) > 0 {
// 		return selectArray, fmt.Errorf("invalid field(s): %s", strings.Join(invalidFields, ", "))
// 	}
// 	if len(selectArray) == 0 {
// 		return selectArray, fmt.Errorf("no valid fields")
// 	}
// 	return selectArray, nil
// }

// BuildQueryConfigFromRequest
func (d *Database) SelectBuilderFromRequest(r *http.Request, withKey bool) (*SelectBuilder, []interface{}, error) {

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

	sb := &SelectBuilder{}
	args := make([]interface{}, 0)

	sb.From = path.Base(r.URL.Path)
	if withKey {
		pkValue := sb.From
		sb.From = path.Base(path.Dir(r.URL.Path))
		ct := d.config.GetTable(sb.From)
		if ct != nil {
			sb.Where = append(sb.Where, EqualsArg(sb.From, ct.PrimaryKey()))
			args = append(args, pkValue)
		}
	}

	tableInfo := d.dbInfo.GetTableInfo(sb.From)

	if s := r.URL.Query().Get("select"); s != "" && s != "*" {
		sb.Select = strings.Split(s, ",")
		for i, _ := range sb.Select {
			sb.Select[i] = strings.TrimSpace(sb.Select[i])
		}
	}

	if s := r.URL.Query().Get("search"); s != "" {
		fields := sb.Select
		if len(fields) == 0 {
			for _, f := range tableInfo.Fields {
				fields = append(fields, tableFieldWrapped(tableInfo.Name, f.Name))
			}
		}

		conditions := make([]string, 0)
		for _, f := range fields {
			conditions = append(conditions, f+" LIKE ?")
			args = append(args, s)
		}
		sb.Where = append(sb.Where, "("+strings.Join(conditions, " OR ")+")")
	}

	if s := r.URL.Query().Get("sort"); s != "" {
		for _, e := range strings.Split(s, ",") {
			m := regOrderBy.FindStringSubmatch(e)
			if len(m) == 4 {
				ob := OrderBy{
					Field:     m[2],
					Ascending: strings.ToLower(m[3]) == "asc",
				}
				sb.OrderBy = append(sb.OrderBy, ob)
			}
		}
	}

	sb.Limit, err = GetQueryUint("limit", 1000)
	if err != nil {
		return nil, nil, err
	}

	sb.Offset, err = GetQueryUint("offset", 0)
	if err != nil {
		return nil, nil, err
	}

	return sb, args, nil
}

// https://regex101.com/r/9n82vv/1
var regOrderBy = regexp.MustCompile(`(?i)(-?)(\w+) *(?:(asc|desc|))`)
