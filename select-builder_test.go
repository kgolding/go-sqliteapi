package gdb

import (
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/stretchr/testify/assert"
)

func MakeDB(t *testing.T) *sqlx.DB {
	d, err := sqlx.Open("sqlite3", "file::memory:?cache=shared")
	assert.NoError(t, err)

	_, err = d.Exec("CREATE TABLE table1 (id INTEGER PRIMARY KEY, text TEXT)")
	assert.NoError(t, err)

	_, err = d.Exec(`CREATE TABLE table2 (
				id INTEGER PRIMARY KEY,
				table1Id INTEGER UNIQUE,
				text2 TEXT,
				FOREIGN KEY (table1Id) REFERENCES table1(id)
				)`)
	assert.NoError(t, err)

	return d
}

func QueryDB(t *testing.T, d *sqlx.DB, query string, args ...interface{}) []map[string]interface{} {
	rows, err := d.Queryx(query, args...)
	ret := make([]map[string]interface{}, 0)
	if assert.NoError(t, err) {
		for rows.Next() {
			m := make(map[string]interface{})
			assert.NoError(t, rows.MapScan(m))
			ret = append(ret, m)
		}
	}
	return ret
}

func TestSelectBuilder(t *testing.T) {
	d := MakeDB(t)
	defer d.Close()

	sb := SelectBuilder{
		From: "table1",
	}
	s, err := sb.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `table1`.*\nFROM `table1`", s)
	QueryDB(t, d, s)

	sb.Select = []string{"id", "text"}
	s, err = sb.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `table1`.`id`, `table1`.`text`\nFROM `table1`", s)
	QueryDB(t, d, s)

	sb.Joins = []Join{
		{
			Type:  LEFT_OUTER,
			Table: "table2",
			On: []JoinOn{
				{
					Field:       "table1Id",
					ParentField: "id",
				},
			},
		},
	}
	s, err = sb.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `table1`.`id`, `table1`.`text`\nFROM `table1`\nLEFT OUTER JOIN `table2` ON `table2`.`table1Id`=`table1`.`id`", s)
	QueryDB(t, d, s)

	sb.OrderBy = []OrderBy{
		{
			Field:     "id",
			Ascending: true,
		}}
	s, err = sb.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `table1`.`id`, `table1`.`text`\nFROM `table1`\nLEFT OUTER JOIN `table2` ON `table2`.`table1Id`=`table1`.`id`\nORDER BY `table1`.`id` ASC", s)
	QueryDB(t, d, s)

	sb.Where = []string{EqualsArg("table1", "id")}
	s, err = sb.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT `table1`.`id`, `table1`.`text`\nFROM `table1`\nLEFT OUTER JOIN `table2` ON `table2`.`table1Id`=`table1`.`id`\nWHERE `table1`.`id`=?\nORDER BY `table1`.`id` ASC", s)
	QueryDB(t, d, s, 1)
}
