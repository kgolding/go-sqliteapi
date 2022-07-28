package gdb

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared",
		Log(log.Default()))
	assert.NoError(t, err)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	defer db.Close()

	_, err = db.DB.Exec(`
	CREATE TABLE test (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		text TEXT
	)`)
	assert.NoError(t, err)
}

func TestJoin(t *testing.T) {
	db, err := NewDatabase("file::memory:",
		Log(log.Default()))
	assert.NoError(t, err)
	defer db.Close()

	cfg := &Config{
		Tables: []ConfigTable{
			ConfigTable{
				Name: "t1",
				Fields: []ConfigField{
					ConfigField{Name: "id"},
					ConfigField{Name: "name"},
				},
			},
			ConfigTable{
				Name: "t2",
				Fields: []ConfigField{
					ConfigField{Name: "id"},
					ConfigField{Name: "name"},
					ConfigField{Name: "t1Id", References: "t1.id/name"},
				},
			},
		},
	}

	assert.NoError(t, db.ApplyConfig(cfg, nil))
	db.Refresh()

	_, err = db.insertMap("t1", map[string]interface{}{"name": "T1 Row 1"}, nil)
	assert.NoError(t, err)
	_, err = db.insertMap("t1", map[string]interface{}{"name": "T1 Row 2"}, nil)
	assert.NoError(t, err)
	t2r1Id, err := db.insertMap("t2", map[string]interface{}{"name": "T2 Row 1", "t1Id": 1}, nil)
	assert.NoError(t, err)

	// Get t2 row joined with t1 and check t1 name is in the result
	bcq := BuildQueryConfig{
		Table: "t2",
		Key:   fmt.Sprintf("%d", t2r1Id),
	}
	q, args, err := db.BuildQuery(bcq)
	assert.NoError(t, err)

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, q, args))
	assert.Contains(t, b.String(), "T1 Row 1")
}

func TestJoinMultipleLabels(t *testing.T) {
	db, err := NewDatabase("file::memory:",
		Log(log.Default()))
	assert.NoError(t, err)
	defer db.Close()

	cfg := &Config{
		Tables: []ConfigTable{
			ConfigTable{
				Name: "t1",
				Fields: []ConfigField{
					ConfigField{Name: "id"},
					ConfigField{Name: "name"},
				},
			},
			ConfigTable{
				Name: "t2",
				Fields: []ConfigField{
					ConfigField{Name: "id"},
					ConfigField{Name: "name"},
					ConfigField{Name: "t1Id", References: "t1.id/id,name"},
				},
			},
		},
	}

	assert.NoError(t, db.ApplyConfig(cfg, nil))
	db.Refresh()

	_, err = db.insertMap("t1", map[string]interface{}{"name": "T1 Row 1"}, nil)
	assert.NoError(t, err)
	_, err = db.insertMap("t1", map[string]interface{}{"name": "T1 Row 2"}, nil)
	assert.NoError(t, err)
	t2r1Id, err := db.insertMap("t2", map[string]interface{}{"name": "T2 Row 1", "t1Id": 1}, nil)
	assert.NoError(t, err)

	// Get t2 row joined with t1 and check t1 name is in the result
	bcq := BuildQueryConfig{
		Table: "t2",
		Key:   fmt.Sprintf("%d", t2r1Id),
	}
	q, args, err := db.BuildQuery(bcq)
	assert.NoError(t, err)

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, q, args))
	assert.Contains(t, b.String(), "1|T1 Row 1")
}

func TestBackup(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared",
		Log(log.Default()))
	assert.NoError(t, err)
	defer db.Close()

	_, err = db.DB.Exec(`
	CREATE TABLE test (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		text TEXT
	)`)
	assert.NoError(t, err)
	db.Refresh()

	rows := 100

	m := make(map[string]interface{})
	for i := 1; i <= rows; i++ {
		m["text"] = fmt.Sprintf("Item %d", i)
		_, err = db.insertMap("test", m, nil)
		assert.NoError(t, err)
	}

	fname := "./database.backup.db"
	os.Remove(fname)
	assert.NoError(t, db.Backup(fname))

	d, err := sqlx.Open("sqlite3", fname)
	assert.NoError(t, err)
	defer d.Close()

	var c int
	assert.NoError(t, d.Get(&c, "SELECT COUNT(*) FROM test"))
	assert.Equal(t, c, rows)
}

func TestNoneIdPrimaryKey(t *testing.T) {
	const yaml = `
tables:
  table1:
    oid:
      pk: true
      notnull: true
    text:
  table2:
    id:
    oid:
      ref: table1.oid/text
`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()))
	assert.NoError(t, err)
	defer db.Close()

	// t.Log(db.config.String())

	_, err = db.insertMap("table1", map[string]interface{}{
		"text": "Dummy",
	}, nil)
	assert.Error(t, err)

	_, err = db.insertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 1",
	}, nil)
	assert.NoError(t, err)

	row := db.DB.QueryRowx("SELECT * FROM table1")
	assert.NoError(t, row.Err())

	_, err = db.insertMap("table2", map[string]interface{}{
		"oid": "abc1",
	}, nil)
	assert.NoError(t, err)

	m := make(map[string]interface{})

	assert.NoError(t, row.MapScan(m))

	assert.Equal(t, "abc1", m["oid"])

	// Get table2 row joined with table1 and check label name is in the result
	bcq := BuildQueryConfig{
		Table: "table2",
		Key:   "1",
	}
	q, args, err := db.BuildQuery(bcq)
	assert.NoError(t, err)

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, q, args))
	assert.Contains(t, b.String(), "ABC 1")

	_, err = db.insertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 2",
	}, nil)
	assert.Error(t, err, "duplicated oid should not be allowed")

}
