package sqliteapi

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared",
		Log(log.Default()))
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
	db, err := NewDatabase("file::memory:") // Log(log.Default()),
	// DebugLog(log.Default()),

	assert.NoError(t, err)
	defer db.Close()

	cfg := &Config{
		Tables: []ConfigTable{
			ConfigTable{
				Name: "t1",
				Fields: []ConfigField{
					ConfigField{Name: "id", PrimaryKey: 1, Type: "INTEGER"},
					ConfigField{Name: "name"},
				},
			},
			ConfigTable{
				Name: "t2",
				Fields: []ConfigField{
					ConfigField{Name: "id", PrimaryKey: 1, Type: "INTEGER"},
					ConfigField{Name: "name"},
					ConfigField{Name: "t1Id", References: "t1.id/name"},
				},
			},
		},
	}

	assert.NoError(t, db.ApplyConfig(cfg, nil))
	db.Refresh()

	_, err = db.InsertMap("t1", map[string]interface{}{"name": "T1 Row 1"}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("t1", map[string]interface{}{"name": "T1 Row 2"}, nil)
	assert.NoError(t, err)
	t2r1Id, err := db.InsertMap("t2", map[string]interface{}{"name": "T2 Row 1", "t1Id": 1}, nil)
	assert.NoError(t, err)

	// Get t2 row joined with t1 and check t1 name is in the result
	// bcq := &BuildQueryConfig{
	// 	Table:   "t2",
	// 	PkValue: fmt.Sprintf("%d", t2r1Id),
	// }
	// q, args, err := db.BuildQuery(bcq)
	// assert.NoError(t, err)
	sb := NewSelectBuilder("t2", []string{"id", "name", "t1Id"})
	// sb := &SelectBuilder{
	// 	Select: []string{"id", "name"},
	// 	From:   "t2",
	// 	Where:  []string{"`t2`.`id`=?"},
	// }
	sb.Where = []string{"`t2`.`id`=?"}

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, sb, []interface{}{t2r1Id}))
	assert.Contains(t, b.String(), "T1 Row 1")
}

func TestJoinMultipleLabels(t *testing.T) {
	db, err := NewDatabase("file::memory:") // Log(log.Default()),

	assert.NoError(t, err)
	defer db.Close()

	cfg := &Config{
		Tables: []ConfigTable{
			ConfigTable{
				Name: "t1",
				Fields: []ConfigField{
					ConfigField{Name: "id", PrimaryKey: 1, Type: "INTEGER"},
					ConfigField{Name: "name"},
				},
			},
			ConfigTable{
				Name: "t2",
				Fields: []ConfigField{
					ConfigField{Name: "id", PrimaryKey: 1, Type: "INTEGER"},
					ConfigField{Name: "name"},
					ConfigField{Name: "t1Id", References: "t1.id/id,name"},
				},
			},
		},
	}

	assert.NoError(t, db.ApplyConfig(cfg, nil))
	db.Refresh()

	_, err = db.InsertMap("t1", map[string]interface{}{"name": "T1 Row 1"}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("t1", map[string]interface{}{"name": "T1 Row 2"}, nil)
	assert.NoError(t, err)
	t2r1Id, err := db.InsertMap("t2", map[string]interface{}{"name": "T2 Row 1", "t1Id": 1}, nil)
	assert.NoError(t, err)

	// Get t2 row joined with t1 and check t1 name is in the result
	// bcq := &BuildQueryConfig{
	// 	Table:   "t2",
	// 	PkValue: fmt.Sprintf("%d", t2r1Id),
	// }
	// q, args, err := db.BuildQuery(bcq)
	// assert.NoError(t, err)

	sb := &SelectBuilder{
		From:  "t2",
		Where: []string{"`t2`.`id`=?"},
	}

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, sb, []interface{}{t2r1Id}))
	assert.Contains(t, b.String(), "1|T1 Row 1")
}

func TestNoneIdPrimaryKey(t *testing.T) {
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
		YamlConfig([]byte(yaml)),
		Log(log.Default()))
	assert.NoError(t, err)
	defer db.Close()

	// t.Log(db.config.String())

	_, err = db.InsertMap("table1", map[string]interface{}{
		"text": "Dummy",
	}, nil)
	assert.Error(t, err)

	_, err = db.InsertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 1",
	}, nil)
	assert.NoError(t, err)

	row := db.DB.QueryRowx("SELECT * FROM table1")
	assert.NoError(t, row.Err())

	_, err = db.InsertMap("table2", map[string]interface{}{
		"oid": "abc1",
	}, nil)
	assert.NoError(t, err)

	m := make(map[string]interface{})

	assert.NoError(t, row.MapScan(m))

	assert.Equal(t, "abc1", m["oid"])

	// Get table2 row joined with table1 and check label name is in the result
	// bcq := &BuildQueryConfig{
	// 	Table:   "table2",
	// 	PkValue: "1",
	// }
	// q, args, err := db.BuildQuery(bcq)
	// assert.NoError(t, err)

	sb := &SelectBuilder{
		From:  "table2",
		Where: []string{"id=?"},
	}

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, sb, []interface{}{1}))

	// var b bytes.Buffer
	// assert.NoError(t, db.queryJsonWriterRow(&b, bcq))
	assert.Contains(t, b.String(), "ABC 1")

	_, err = db.InsertMap("table1", map[string]interface{}{
		"oid":  "abc1",
		"text": "ABC 2",
	}, nil)
	assert.Error(t, err, "duplicated oid should not be allowed")
}

func TestMany2Many(t *testing.T) {
	const yaml = `
tables:
  t1:
    id:
    title:
  t2:
    id:
    title2:
  t1T2:
    id:
    t1Id:
      ref: t1.id/title
    t2Id:
      ref: t2.id/title2
`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()),
		// DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	// t.Log(db.config.String())

	// Populate t1
	_, err = db.InsertMap("t1", map[string]interface{}{
		"title": "T1 1",
	}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("t1", map[string]interface{}{
		"title": "T1 2",
	}, nil)
	assert.NoError(t, err)

	// Populate t2
	_, err = db.InsertMap("t2", map[string]interface{}{
		"title2": "T2 1",
	}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("t2", map[string]interface{}{
		"title2": "T2 2",
	}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("t2", map[string]interface{}{
		"title2": "T2 3",
	}, nil)
	assert.NoError(t, err)

	// Insert joined data
	id, err := db.InsertMap("t1", map[string]interface{}{
		"title": "Joined",
		"t1T2" + RefTableSuffix: []map[string]interface{}{
			{"t2Id": 1},
			{"t2Id": 2},
		},
	}, nil)
	assert.NoError(t, err)
	// t.Logf("Inserted t1 row with id %d that is linked to t2 1 & 2", id)

	var c int
	assert.NoError(t, db.DB.Get(&c, "SELECT COUNT(*) FROM t1T2 WHERE t1Id = ?", id))
	assert.Equal(t, 2, c)

	rows, err := db.DB.Queryx("SELECT * FROM t1T2")
	assert.NoError(t, err)
	for rows.Next() {
		m := make(map[string]interface{})
		rows.MapScan(m)
		// t.Logf("t1T2: %#v", m)
	}

	// Get table2 row joined with table1 and check label name is in the result
	// bqc := &BuildQueryConfig{
	// 	Table:            "t1",
	// 	PkValue:          id,
	// 	IncludeJunctions: true,
	// }
	// q, args, err := db.BuildQuery(bcq)
	// assert.NoError(t, err)
	// t.Log(q)

	sb := &SelectBuilder{
		From:  "t1",
		Where: []string{"id=?"},
	}

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, sb, []interface{}{id}))

	// var b bytes.Buffer
	// // @TODO
	// assert.NoError(t, db.queryJsonWriterRow(&b, bqc))
	assert.Contains(t, b.String(), `"t2Id":"2"`)
	assert.Contains(t, b.String(), `"t1Id":"3"`)
	// t.Log(b.String())
}

func TestOne2ManyAkaInvoiceAndItems(t *testing.T) {
	const yaml = `
tables:
  inv:
    id:
    title:
  item:
    id:
    invId:
      ref: inv.id/title
    text:
    qty:
      type: integer
      min: 0
      default: 1
`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()),
		// DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	// Populate inv
	_, err = db.InsertMap("inv", map[string]interface{}{
		"title": "Invoice 1",
	}, nil)
	assert.NoError(t, err)
	_, err = db.InsertMap("inv", map[string]interface{}{
		"title": "Invoice 2",
		"item" + RefTableSuffix: []map[string]interface{}{
			{"text": "Item 2.1"},
			{"text": "Item 2.2", "qty": 25},
		},
	}, nil)
	assert.NoError(t, err)

	// Insert joined data
	id, err := db.InsertMap("inv", map[string]interface{}{
		"title": "Joined",
		"item" + RefTableSuffix: []map[string]interface{}{
			{"text": "Item 3.1"},
			{"text": "Item 3.2", "qty": 25},
		},
	}, nil)
	assert.NoError(t, err)
	// t.Logf("Inserted t1 row with id %d that is linked to t2 1 & 2", id)

	var c int
	assert.NoError(t, db.DB.Get(&c, "SELECT COUNT(*) FROM item"))
	assert.Equal(t, 4, c)

	// Get inv row joined with item and check label name is in the result
	// bqc := &BuildQueryConfig{
	// 	Table:            "inv",
	// 	PkValue:          id,
	// 	IncludeJunctions: true,
	// }
	// q, args, err := db.BuildQuery(bcq)
	// assert.NoError(t, err)
	// t.Log(q)

	sb := &SelectBuilder{
		From:  "inv",
		Where: []string{"id=?"},
	}

	var b bytes.Buffer
	assert.NoError(t, db.queryJsonWriterRow(&b, sb, []interface{}{id}))

	// var b bytes.Buffer
	// // @TODO
	// assert.NoError(t, db.queryJsonWriterRow(&b, bqc))
	assert.Equal(t, `{"id":3,"item_RefTable":[{"id":3,"invId":"3","qty":1,"text":"Item 3.1"},{"id":4,"invId":"3","qty":25,"text":"Item 3.2"}],"title":"Joined"}`, b.String())
	// t.Log(b.String())
}

func TestMultipleCallsToAddRefFields(t *testing.T) {
	const yaml = `
tables:
  inv:
    id:
    title:
  item:
    id:
    invId:
      ref: inv.id/title
    text:
    qty:
      type: integer
      min: 0
      default: 1
`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()),
		// DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	sb := NewSelectBuilder("item", []string{"id", "invId", "title"})
	sb2 := NewSelectBuilder("item", []string{"id", "invId", "title"})
	assert.Equal(t, sb, sb2)

	db.AddRefLabels(sb, "")
	assert.NotEqual(t, sb, sb2)

	db.AddRefLabels(sb2, "")
	assert.Equal(t, sb, sb2)

	// Add again!
	db.AddRefLabels(sb2, "")
	assert.Equal(t, sb, sb2)

}

func WIPTestUniqueIndex(t *testing.T) {
	const yaml = `
tables:
  table1:
    id:
    text:`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()),
		// DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	data := map[string]interface{}{"text": "TEST"}

	_, err = db.InsertMap("table1", data, nil)
	assert.NoError(t, err)

	row2id, err := db.InsertMap("table1", data, nil)
	assert.NoError(t, err)

	// Add a unique index
	c, err := NewConfigFromYaml([]byte(yaml + `
          unique: true`))
	assert.NoError(t, err)

	assert.Error(t, db.ApplyConfig(c, nil), "should fail as table1.text is duplicated")

	assert.NoError(t, db.Delete("table1", row2id, nil))

	assert.NoError(t, db.ApplyConfig(c, nil))

}
