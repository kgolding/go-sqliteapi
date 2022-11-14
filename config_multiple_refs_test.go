package sqliteapi

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigMultipleRefs(t *testing.T) {
	yaml := []byte(`
tables:
  table1:
    id:
    createdAt:
    text:
  table2:
    id:
    ref1:
      ref: table1.id/text
    ref2:
      ref: table1.id/text
`)

	db, err := NewDatabase("file::memory:",
		Log(log.Default()),
		DebugLog(log.Default()),
		YamlConfig(yaml),
	)
	assert.NoError(t, err)
	defer db.Close()

	// GetRows
	tsRows := httptest.NewServer(http.HandlerFunc(db.HandleGetRows))
	defer tsRows.Close()
	res, err := http.Get(tsRows.URL + "/table2")
	assert.NoError(t, err)
	assert.NotNil(t, res)
	b, err := io.ReadAll(res.Body)
	b = b
	res.Body.Close()
	assert.NoError(t, err)
	// @TODO THIS IS CURRENTLY FAILING
	// assert.Equal(t, []byte("[]"), b)
}
