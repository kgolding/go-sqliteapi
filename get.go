package sqliteapi

import (
	"database/sql"
	"fmt"
)

func (d *Database) GetMap(table string, pk interface{}, withRefTables bool) (map[string]interface{}, error) {
	tx, err := d.DB.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // This is a query so we always rollback

	sb := NewSelectBuilder(table, []string{})

	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		return nil, ErrUnknownTable
	}

	sb.Where = []string{tableInfo.GetPrimaryKey().String() + "=?"}

	d.AddRefLabels(sb, "")

	query, err := sb.ToSql()
	if err != nil {
		return nil, err
	}

	d.debugLog.Printf("GetMap: query: %s, args: %v", query, pk)

	row := tx.QueryRowx(query, pk)
	if row == nil {
		return nil, sql.ErrNoRows
	}

	ret := make(map[string]interface{})

	err = row.MapScan(ret)
	if err != nil {
		return nil, err
	}

	d.debugLog.Printf("GetMap: ret: %v", ret)

	if withRefTables {
		// Check for references from other tables
		for _, ref := range d.config.GetBackReferences(table) {
			// fmt.Printf("A. BackRef: %v\n", ref)
			ssb := &SelectBuilder{
				From:  ref.SourceTable,
				Where: []string{tableFieldWrapped(ref.SourceTable, ref.SourceField) + "=?"},
			}
			d.AddRefLabels(ssb, sb.From)
			query, err := ssb.ToSql()
			if err != nil {
				return nil, err
			}
			subArgs := make([]interface{}, 0)
			subArgs = append(subArgs, ret[ref.KeyField])
			// fmt.Printf("C. Sub-query: %s\nArgs: %v\n", query, subArgs)

			rows, err := tx.Queryx(query, subArgs...)
			if err != nil {
				return nil, fmt.Errorf("sub-query '%s': %w", ref.SourceTable, err)
			}

			subRet := make([]map[string]interface{}, 0)
			for rows.Next() {
				// d.debugLog.Printf("D. sub-query:\n")
				m := make(map[string]interface{}, 0)
				err = rows.MapScan(m)
				// d.debugLog.Printf("E. sub-query: %v\n", m)
				if err != nil {
					return nil, fmt.Errorf("sub-query '%s': %w", ref.SourceTable, err)
				}
				subRet = append(subRet, m)
				// d.debugLog.Printf("F: sub-query: add %v\n", m)
			}
			refFieldName := ref.SourceTable + RefTableSuffix
			if _, exists := ret[refFieldName]; exists {
				refFieldName = ref.SourceTable + "_" + ref.SourceField + RefTableSuffix
			}
			// d.debugLog.Printf("G. sub-query: set '%s' = %v\n", refFieldName, subRet)
			ret[refFieldName] = subRet
		}
	}

	return ret, nil
}
