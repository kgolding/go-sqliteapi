package gdb

type TableInfo struct {
	Name   string            `json:"name"`
	IsView bool              `json:"isView"`
	Fields []*TableFieldInfo `json:"fields"`
}

type TableFieldInfo struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	NotNull      bool        `json:"notnull,omitempty"`
	DefaultValue interface{} `json:"default,omitempty"`
	PrimaryKey   int         `json:"pk,omitempty"` // number of prmimary keys
}

func (d *Database) Refresh() error {
	info := make(map[string]*TableInfo)

	rows, err := d.DB.Query(`
		SELECT type, tbl_name FROM sqlite_master
		WHERE type IN ("table", "view") AND name NOT LIKE "sqlite_%" ORDER BY tbl_name
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
			Fields: make([]*TableFieldInfo, 0),
		}
		frows, err := d.DB.Query(`PRAGMA TABLE_INFO("` + n + `")`)
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
			table.Fields = append(table.Fields, &f)
		}

		info[n] = &table
	}
	d.dbInfo = info
	return nil
}
