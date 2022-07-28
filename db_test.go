package gdb

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	db, err := NewDatabase("file::memory:")
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

func TestJoinMultipleLabels(t *testing.T) {
	db, err := NewDatabase("file::memory:")
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
					Field{Name: "t1Id", References: "t1.id/id,name"},
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
	assert.Contains(t, b.String(), "1|T1 Row 1")
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

func TestNoneIdPrimaryKey(t *testing.T) {
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
		YamlConfig([]byte(yaml)))
	assert.NoError(t, err)
	defer db.Close()

	// t.Log(db.config.String())

	_, err = db.insertMap("table1", map[string]interface{}{
		"text": "Dummy",
	}, nil)
	assert.Error(t, err)

	_, err = db.insertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 1",
	}, nil)
	assert.NoError(t, err)

	row := db.DB.QueryRowx("SELECT * FROM table1")
	assert.NoError(t, row.Err())

	_, err = db.insertMap("table2", map[string]interface{}{
		"oid": "abc1",
	}, nil)
	assert.NoError(t, err)

	m := make(map[string]interface{})

	assert.NoError(t, row.MapScan(m))

	assert.Equal(t, "abc1", m["oid"])

	// Get table2 row joined with table1 and check label name is in the result
	bcq := BuildQueryConfig{
		Table: "table2",
		Key:   "1",
	}
	q, args, err := db.BuildQuery(bcq)
	assert.NoError(t, err)

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, q, args))
	assert.Contains(t, b.String(), "ABC 1")

	_, err = db.insertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 2",
	}, nil)
	assert.Error(t, err, "duplicated oid should not be allowed")

}

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
