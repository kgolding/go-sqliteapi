package sqliteapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHttp(t *testing.T) {
	const yaml = `
tables:
  table1:
    oid:
      pk: 1
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

	assert.Equal(t, db.config.GetTable("table2").Fields[1].References, "table1.oid/text")

	// Test row as json
	j := `{"oid":"abc1","text":"ABC 1"}`

	tsPost := httptest.NewServer(http.HandlerFunc(db.HandlePostTable))
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
	assert.Equal(t, "table1.oid is already used and this field must be unique\n", string(b))

	// Post SQL and check result
	tsSqlPost := httptest.NewServer(db.Handler(""))
	defer tsSqlPost.Close()
	data = bytes.NewBufferString("SELECT * FROM table1")
	res, err = http.Post(tsSqlPost.URL, "text/plain", data)
	assert.NoError(t, err, "posting data to create new row")
	assert.Equal(t, http.StatusOK, res.StatusCode)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.Equal(t, `[{"oid":"abc1","text":"ABC 1"}]`, string(b))

	// GetRows
	tsRows := httptest.NewServer(http.HandlerFunc(db.HandleGetRows))
	defer tsRows.Close()
	res, err = http.Get(tsRows.URL + "/table1")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("["+j+"]"), b)

	// GetRows select fields
	res, err = http.Get(tsRows.URL + "/table1?select=oid,text")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("["+j+"]"), b)

	res, err = http.Get(tsRows.URL + "/table1?search=" + url.QueryEscape("ABC%"))
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("["+j+"]"), b)

	res, err = http.Get(tsRows.URL + "/table1?search=" + url.QueryEscape("%DO_NOT_MATCH%"))
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte("[]"), b)

	// GetRows as array of array
	res, err = http.Get(tsRows.URL + "/table1?format=array")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, []byte(`[["abc1","ABC 1"]]`), b)

	// GetRows Info
	tsTableNames := httptest.NewServer(http.HandlerFunc(db.HandleGetTableNames))
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
	tsRow := httptest.NewServer(http.HandlerFunc(db.HandleGetRow))
	defer tsRow.Close()
	res, err = http.Get(tsRow.URL + "/table1/abc1?withRefTable")
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `{"oid":"abc1","table2_RefTable":[],"text":"ABC 1"}`, strings.TrimSpace(string(b)))

	// GetRow() returns all fields @TODO add select support
	// res, err = http.Get(tsRow.URL + "/table1/abc1?select=oid")
	// assert.NoError(t, err)
	// b, err = io.ReadAll(res.Body)
	// res.Body.Close()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// assert.Equal(t, `{"oid":"abc1"}`, strings.TrimSpace(string(b)))

	// Update row
	tsPut := httptest.NewServer(http.HandlerFunc(db.HandlePutRow))
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

	// Post SQL
	tsPostSQL := httptest.NewServer(http.HandlerFunc(db.HandlePostSQL))
	defer tsPostSQL.Close()
	buf := bytes.NewBufferString("SELECT * FROM table1")
	req, err = http.NewRequest(http.MethodPost, tsPostSQL.URL, buf)
	assert.NoError(t, err, "post SQL")
	res, err = client.Do(req)
	assert.NoError(t, err, "post SQL")
	assert.Equal(t, http.StatusOK, res.StatusCode)

	assert.NoError(t, err)
	m := make([]map[string]interface{}, 0)
	assert.NoError(t, json.NewDecoder(res.Body).Decode(&m))
	assert.Len(t, m, 1)
	assert.Equal(t, "abc1", m[0]["oid"])

	// Post and create a second row
	res, err = http.Post(tsPost.URL+"/table1", "Content-Type: application/json",
		bytes.NewBufferString(`{"oid":"xyz","text":"XYZ"}`))
	assert.NoError(t, err, "posting data to create second row")
	assert.Equal(t, http.StatusOK, res.StatusCode)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.Equal(t, []byte("2"), b)

	res, err = http.Get(tsRows.URL + "/table1?where=" + url.QueryEscape("text=\"XYZ\""))
	assert.NoError(t, err)
	b, err = io.ReadAll(res.Body)
	res.Body.Close()
	assert.NoError(t, err)
	assert.Equal(t, "[{\"oid\":\"xyz\",\"text\":\"XYZ\"}]", string(b))

	// Delete row
	tsDelete := httptest.NewServer(http.HandlerFunc(db.HandleDelRow))
	defer tsDelete.Close()
	req, err = http.NewRequest(http.MethodDelete, tsDelete.URL+"/table1/abc1", nil)
	assert.NoError(t, err, "delete data")
	res, err = client.Do(req)
	assert.NoError(t, err, "delete data")
	assert.Equal(t, http.StatusOK, res.StatusCode)

}
