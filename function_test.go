package sqliteapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_FunctionParse(t *testing.T) {
	yaml := `
tables:
  table1:
    id:
    createdAt:
    text:
functions:
  func1:
    params:
      text:
        notnull: true
        min: 2
    statements:
      - INSERT INTO table1 (text) VALUES ($1)
`

	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		// Log(log.Default()),
		// DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}

	c := db.config

	assert.Equal(t, 1, len(c.Functions))
	assert.Equal(t, 1, len(c.Functions[0].Params))
	assert.Equal(t, "func1", c.Functions[0].Name)
	assert.Equal(t, "text", c.Functions[0].Params[0].Name)
	assert.Equal(t, true, c.Functions[0].Params[0].Notnull)
	assert.Equal(t, int64(2), c.Functions[0].Params[0].Min)

	assert.Equal(t, 1, len(c.Functions[0].Statements))

	assert.NoError(t, db.CallFunction("func1", map[string]interface{}{"text": "Hello world"}, nil))

	m, err := db.GetMap("table1", 1, false)
	assert.NoError(t, err)
	assert.Equal(t, "Hello world", m["text"])
}
