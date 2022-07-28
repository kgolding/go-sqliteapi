package gdb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared")
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
	db, err := NewDatabase("file::memory:?cache=shared")
	assert.NoError(t, err)
	defer db.Close()

	cfg := &Config{
		Tables: []Table{
			Table{
				Name: "t1",
				Fields: []Field{
					Field{Name: "id"},
					Field{Name: "name"},
				},
			},
			Table{
				Name: "t2",
				Fields: []Field{
					Field{Name: "id"},
					Field{Name: "name"},
					Field{Name: "t1Id", References: "t1.id/name"},
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

func TestBackup(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared")
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
