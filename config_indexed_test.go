package sqliteapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigTableFieldIndexed(t *testing.T) {
	yaml := []byte(`
tables:
  table1:
    id:
    createdAt:
    text:
`)

	db, err := NewDatabase("file::memory:",
		// Log(log.Default()),
		// DebugLog(log.Default()),
		YamlConfig(yaml),
	)
	assert.NoError(t, err)
	defer db.Close()

	var i int
	assert.NoError(t, db.DB.Get(&i, "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"))
	assert.Equal(t, 0, i)

	yamlWithIndex := append(yaml, []byte(`
      indexed: true`)...)

	db, err = NewDatabase("file::memory:",
		// Log(log.Default()),
		// DebugLog(log.Default()),
		YamlConfig(yamlWithIndex),
	)
	assert.NoError(t, err)
	defer db.Close()

	assert.NoError(t, db.DB.Get(&i, "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"))
	assert.Equal(t, 1, i)

}
