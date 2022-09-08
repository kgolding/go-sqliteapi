package sqliteapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigViews(t *testing.T) {
	yaml := []byte(`
tables:
  table1:
    id:
    createdAt:
    text:
views:
  view1:
    SELECT id, createdAt, text AS myText
    FROM table1
`)

	db, err := NewDatabase("file::memory:",
		// Log(log.Default()),
		// DebugLog(log.Default()),
		YamlConfig(yaml),
	)
	assert.NoError(t, err)
	defer db.Close()

	_, err = db.InsertMap("table1", map[string]interface{}{
		"text": "Hello world!",
	}, nil)
	assert.NoError(t, err)

	var s string
	assert.NoError(t, db.DB.Get(&s, "SELECT myText FROM view1 LIMIT 1"))
	assert.Equal(t, "Hello world!", s)

}
