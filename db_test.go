package gdb

import (
	"testing"

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
