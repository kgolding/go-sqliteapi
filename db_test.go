package gdb

import (
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
