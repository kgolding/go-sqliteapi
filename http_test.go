package gdb

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHttp(t *testing.T) {
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
		// Log(log.Default()),
		// DebugLog(log.Default()),
		YamlConfig([]byte(yaml)))
	assert.NoError(t, err)
	defer db.Close()

	// Test row as json
	j := `{"oid":"abc1","text":"ABC 1"}`

	tsPost := httptest.NewServer(http.HandlerFunc(db.PostTable))
	defer tsPost.Close()

	// Post and create a row
	data := bytes.NewBufferString(j)
	res, err := http.Post(tsPost.URL+"/table1", "Content-Type: application/json", data)
	assert.NoError(t, err, "posting data to create new row")
	assert.Equal(t, http.StatusOK, res.StatusCode)

	// Post again and expect an error
	data = bytes.NewBufferString(j)
	res, err = http.Post(tsPost.URL+"/table1", "Content-Type: application/json", data)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	assert.Equal(t, string(b), "UNIQUE constraint failed: table1.oid\n")

	// GetRows
	tsRows := httptest.NewServer(http.HandlerFunc(db.GetRows))
	defer tsRows.Close()
	res, err = http.Get(tsRows.URL + "/table1")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("["+j+"]"), b)

	// GetRows as array of array
	res, err = http.Get(tsRows.URL + "/table1?format=array")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[["abc1","ABC 1"]]`), b)

	// GetRows Info
	tsTableNames := httptest.NewServer(http.HandlerFunc(db.GetTableNames))
	defer tsTableNames.Close()
	res, err = http.Get(tsTableNames.URL)
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("[\"table1\",\"table2\"]\n"), b)

	// GetRows as csv
	res, err = http.Get(tsRows.URL + "/table1?format=csv")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "oid,text\nabc1,ABC 1\n", string(b))

	// GetRow
	tsRow := httptest.NewServer(http.HandlerFunc(db.GetRow))
	defer tsRow.Close()
	res, err = http.Get(tsRow.URL + "/table1/abc1")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []byte(j), b)

	// Update row
	tsPut := httptest.NewServer(http.HandlerFunc(db.PutRow))
	defer tsPut.Close()

	client := &http.Client{}

	// Put to update row
	data = bytes.NewBufferString(`{"text":"NEW TEXT"}`)
	req, err := http.NewRequest(http.MethodPut, tsPut.URL+"/table1/abc1", data)
	assert.NoError(t, err, "put data")
	res, err = client.Do(req)
	assert.NoError(t, err, "put data")
	assert.Equal(t, http.StatusOK, res.StatusCode)

	// Put to non existent row
	data = bytes.NewBufferString(`{"text":"NEW TEXT"}`)
	req, err = http.NewRequest(http.MethodPut, tsPut.URL+"/table1/DUMMY", data)
	assert.NoError(t, err, "put data")
	res, err = client.Do(req)
	assert.NoError(t, err, "put data")
	assert.Equal(t, http.StatusBadRequest, res.StatusCode)

	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []byte("unknown key\n"), b)

	// Delete row
	tsDelete := httptest.NewServer(http.HandlerFunc(db.DelRow))
	defer tsDelete.Close()
	req, err = http.NewRequest(http.MethodDelete, tsDelete.URL+"/table1/abc1", nil)
	assert.NoError(t, err, "delete data")
	res, err = client.Do(req)
	assert.NoError(t, err, "delete data")
	assert.Equal(t, http.StatusOK, res.StatusCode)

}
