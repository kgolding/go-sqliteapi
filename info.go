package sqliteapi

import (
	"fmt"
	"strings"
)

type TableInfos map[string]TableInfo

type TableInfo struct {
	Name           string           `json:"name"`
	IsView         bool             `json:"isView"`
	Fields         []TableFieldInfo `json:"fields"`
	IsPrimaryKeyId bool             `json:"isPkId"`
}

type TableFieldInfo struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	NotNull      bool        `json:"notnull,omitempty"`
	DefaultValue interface{} `json:"default,omitempty"`
	PrimaryKey   int         `json:"pk,omitempty"` // number of prmimary keys
}

func (tis TableInfos) GetTableInfo(name string) *TableInfo {
	for _, tfi := range tis {
		if tfi.Name == name {
			return &tfi
		}
	}
	return nil
}

func (ti TableInfo) GetPrimaryKey() ResultColumn {
	for _, f := range ti.Fields {
		if f.PrimaryKey > 0 {
			return ResultColumn{
				Table: ti.Name,
				Field: f.Name,
			}
		}
	}
	return ResultColumn{
		Table: ti.Name,
		Field: "?",
	}
}

func (tis TableInfos) String() string {
	s := ""
	for _, ti := range tis {
		s += "Table: " + ti.Name + "\n"
		for _, f := range ti.Fields {
			s += fmt.Sprintf(" - %s %s", f.Name, f.Type)
			if f.NotNull {
				s += " NOT NULL"
			}
			if f.PrimaryKey > 0 {
				s += " PRIMARY KEY"
			}
			if f.DefaultValue != nil {
				s += fmt.Sprintf(" DEFAULT %s", f.DefaultValue)
			}
			s += "\n"
		}
	}

	return s
}

// Refresh updates dbInfo
func (d *Database) Refresh() error {

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	info := make(TableInfos)
	rows, err := tx.Query(`
		SELECT type, tbl_name FROM sqlite_master
		WHERE type IN ("table", "view") AND name NOT LIKE "sqlite_%" AND name NOT LIKE "gdb_%" ORDER BY tbl_name
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var t string
		var n string
		err = rows.Scan(&t, &n)
		if err != nil {
			return err
		}

		table := TableInfo{
			Name:   n,
			IsView: t == "view",
			Fields: make([]TableFieldInfo, 0),
		}
		frows, err := tx.Query(`PRAGMA TABLE_INFO("` + n + `");`)
		if err != nil {
			return err
		}
		for frows.Next() {
			f := TableFieldInfo{}
			var cid int
			err = frows.Scan(&cid, &f.Name, &f.Type, &f.NotNull, &f.DefaultValue, &f.PrimaryKey)
			if err != nil {
				return err
			}
			if f.Name == "id" && f.PrimaryKey > 0 {
				table.IsPrimaryKeyId = true
			}
			f.Type = strings.ToLower(f.Type)
			f.DefaultValue = removeQuotesIfString(f.DefaultValue)
			table.Fields = append(table.Fields, f)
		}

		info[n] = table
	}
	d.dbInfo = info
	return nil
}

func removeQuotesIfString(x interface{}) interface{} {
	if s, ok := x.(string); ok {
		if len(s) > 3 {
			if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
				(strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) {
				return s[1 : len(s)-1]
			}
		}
	}
	return x
}
