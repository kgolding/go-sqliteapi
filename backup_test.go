package sqliteapi

import (
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestBackup(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared",
		// Log(log.Default()),
		YamlConfig([]byte(`
tables:
  test:
    id:
    text:
      label: Text
`)),
	)
	assert.NoError(t, err)
	defer db.Close()

	rows := 100

	// Create rows in memory database
	m := make(map[string]interface{})
	for i := 1; i <= rows; i++ {
		m["text"] = fmt.Sprintf("Item %d", i)
		_, err = db.InsertMap("test", m, nil)
		assert.NoError(t, err)
	}

	fname := "./database.backup.db"
	defer os.Remove(fname)

	// Live back up to a file
	assert.NoError(t, db.Backup(fname))

	// Open backup database and check table/rows exist
	d, err := sqlx.Open("sqlite3", fname)
	assert.NoError(t, err)
	defer d.Close()

	var c int
	assert.NoError(t, d.Get(&c, "SELECT COUNT(*) FROM test"))
	assert.Equal(t, c, rows)
}
