package sqliteapi

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func InsertTable1Data(d *Database) error {
	for i := 1; i < 1000; i++ {
		_, err := d.DB.Exec("INSERT INTO table1 (title) VALUES (?)", fmt.Sprintf("Table1 Title %d", i))
		if err != nil {
			return err
		}
	}
	return nil
}

func GetConfig1(t *testing.T) *Config {
	c, err := NewConfigFromYaml([]byte(`
tables:
  table1:
    id:
    createdAt:
    title:
      type: text
      label: Title
      min: 3
      max: 24
  table2:
    id:
    t1Id:
      label: Table 1 reference
      ref: table1.id
`))
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func GetConfig2(t *testing.T) *Config {
	config1 := GetConfig1(t)
	var config2 = &Config{
		Tables: []ConfigTable{
			config1.Tables[0],
		},
	}
	return config2
}

var OPT = &ConfigOptions{
	RetainUnmanaged: false,
	DryRun:          false,
}

func TestConfigApplyNew(t *testing.T) {
	db, err := NewDatabase("file::memory:")
	assert.NoError(t, err)
	defer db.Close()

	config1 := GetConfig1(t)

	assert.NotNil(t, config1.GetTable("table1"))

	err = db.ApplyConfig(config1, nil)
	assert.NoError(t, err)
	assert.NoError(t, InsertTable1Data(db))

	assert.NoError(t, db.Refresh())

	// Check tables exists
	for _, table := range config1.Tables {
		info, ok := db.dbInfo[table.Name]
		if assert.True(t, ok) {
			assert.False(t, info.IsView)
		}
	}

	ret := db.DB.QueryRowx("SELECT * FROM gdb_config")
	var row GdbConfigRow
	assert.NoError(t, ret.StructScan(&row))
	assert.True(t, row.ID == 1)

	err = db.ApplyConfig(config1, nil)
	assert.NoError(t, err)
	err = db.ApplyConfig(config1, nil)
	assert.NoError(t, err)
	err = db.ApplyConfig(config1, nil)
	assert.NoError(t, err)

	ret = db.DB.QueryRowx("SELECT * FROM gdb_config")
	assert.NoError(t, ret.StructScan(&row))
	assert.True(t, row.ID == 1)
}

func TestConfigApplyTriggers(t *testing.T) {
	cfg := []byte(`
tables:
  table1:
    id:
    text:
    
  history:
    id:
    createdAt:
    table1Id:
      type: integer
      ref: table1.id/text
    text:
    
triggers:
  triggerTable1Insert:
    table: table1
    event: after insert
    statement: INSERT INTO history (table1Id, text) VALUES (new.id, new.text)
  triggerTable1Update:
    table: table1
    event: update of text
    statement: INSERT INTO history (table1Id, text) VALUES (new.id, new.text)
`)

	db, err := NewDatabase("file::memory:",
		// Log(log.Default()),
		// DebugLog(log.Default()),
		YamlConfig(cfg),
	)
	assert.NoError(t, err)
	defer db.Close()

	var i int
	assert.NoError(t, db.DB.Get(&i, "SELECT COUNT(*) FROM history"))
	assert.Equal(t, 0, i)

	id, err := db.InsertMap("table1", map[string]interface{}{"text": "Text 1"}, nil)
	assert.NoError(t, err)

	err = db.UpdateMap("table1", map[string]interface{}{"id": id, "text": "Text 1"}, nil)
	assert.NoError(t, err)

	assert.NoError(t, db.DB.Get(&i, "SELECT COUNT(*) FROM history"))
	assert.Equal(t, 2, i)

	assert.NoError(t, db.DB.Get(&i, "SELECT table1Id FROM history ORDER BY id DESC LIMIT 1"))
	assert.Equal(t, 1, i)

	cfg2, err := NewConfigFromYaml(
		[]byte(strings.Replace(string(cfg), "event: update of text", "event: UPDATE", 1)))
	assert.NoError(t, err)
	assert.NoError(t, db.ApplyConfig(cfg2, nil))

	id, err = db.InsertMap("table1", map[string]interface{}{"text": "Text 2"}, nil)
	assert.NoError(t, err)

	assert.NoError(t, db.DB.Get(&i, "SELECT COUNT(*) FROM history"))
	assert.Equal(t, 3, i)

	assert.NoError(t, db.DB.Get(&i, "SELECT table1Id FROM history ORDER BY id DESC LIMIT 1"))
	assert.Equal(t, 2, i)

}

func TestConfigApplyYamlMultipleTimes(t *testing.T) {
	cfg := []byte(`
tables:
  table1:
    id:
    createdAt:
    text:
      type: text
      notnull: true
      readonly: true
      min: 1
      max: 999
      regex: \w+
      ref: table2.id/title
      control: select
  table2:
    id:
    title:
`)

	defer os.Remove("temp.db")

	expectId := 1
	for i := 1; i < 10; i++ {
		if i == 2 {
			cfg = append(cfg, []byte("      unique: true")...)
			expectId++
		}
		db, err := NewDatabase("temp.db",
			// Log(log.Default()),
			// DebugLog(log.Default()),
			YamlConfig(cfg),
		)
		assert.NoError(t, err)

		ret := db.DB.QueryRowx("SELECT * FROM gdb_config ORDER BY id DESC")
		var row GdbConfigRow
		assert.NoError(t, ret.StructScan(&row))

		assert.Equal(t, expectId, row.ID)

		db.Close()
	}
}

func TestConfigApplyRemoveTable(t *testing.T) {
	db, err := NewDatabase("file::memory:")
	assert.NoError(t, err)
	defer db.Close()

	config1 := GetConfig1(t)
	err = db.ApplyConfig(config1, OPT)
	assert.NoError(t, err)
	assert.NoError(t, InsertTable1Data(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 2) // After refresh

	// Check tables exists
	for _, table := range config1.Tables {
		info, ok := db.dbInfo[table.Name]
		if assert.True(t, ok) {
			assert.False(t, info.IsView)
		}
	}

	db.Refresh()

	config2 := GetConfig2(t)
	err = db.ApplyConfig(config2, OPT)
	db.Refresh()
	assert.NoError(t, err)
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
}

func TestConfigApply1234(t *testing.T) {
	var db *Database
	var err error
	if true {
		db, err = NewDatabase("file::memory:") // Log(log.Default()),
	} else {
		os.Remove("test.db")
		db, err = NewDatabase("test.db")
	}
	assert.NoError(t, err)
	defer db.Close()

	assert.Len(t, db.dbInfo, 0)

	// t.Log(">>>>>>>>>>>>> Config 1")
	config1 := GetConfig1(t)
	assert.NoError(t, db.ApplyConfig(config1, OPT))
	assert.NoError(t, InsertTable1Data(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 2)

	// Test Foreign key
	_, err = db.DB.Exec("INSERT INTO table2 (t1Id) VALUES (1)") // Should be ok
	assert.NoError(t, err)
	_, err = db.DB.Exec("INSERT INTO table2 (t1Id) VALUES (9999999)") // Should fail
	assert.Error(t, err)

	// t.Log(">>>>>>>>>>>>> Config 2")
	config2 := GetConfig2(t)
	assert.NoError(t, db.ApplyConfig(config2, OPT))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)

	// t.Log(">>>>>>>>>>>>> Config 3")
	assert.NoError(t, db.ApplyConfig(&config3, OPT))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
	assert.Len(t, db.dbInfo[config3.Tables[0].Name].Fields, 4)

	// t.Log(">>>>>>>>>>>>> Config 4")
	assert.True(t, config4.Tables[0].Fields[2].NotNull)
	assert.NoError(t, db.ApplyConfig(&config4, OPT))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
	assert.Len(t, db.dbInfo[config3.Tables[0].Name].Fields, 5)
	assert.True(t, db.dbInfo[config3.Tables[0].Name].Fields[2].NotNull)

}

func TestConfigMarshal(t *testing.T) {
	config1 := GetConfig1(t)
	b, err := yaml.Marshal(config1)
	assert.NoError(t, err)
	assert.True(t, len(b) > 20)
}

func TestConfigGetTable(t *testing.T) {
	config1 := GetConfig1(t)
	assert.Len(t, config1.Tables, 2)
	assert.NotNil(t, config1.GetTable(config1.Tables[0].Name))
	assert.Nil(t, config1.GetTable("dummy"))
	var c Config
	assert.Nil(t, c.GetTable("dummy"))
}

func TestRemoveQuotes(t *testing.T) {
	assert.Equal(t, removeQuotesIfString("'ABC'"), "ABC")
	assert.Equal(t, removeQuotesIfString(`"ABC"`), "ABC")
	assert.Equal(t, removeQuotesIfString(`"ABC`), `"ABC`)
}

func TestParseYaml(t *testing.T) {
	s := `tables:
  slideshow:
    id:
    title:
      type: text
      notnull: true
    age:
      type: integer
      #unique: true`

	c, err := NewConfigFromYaml([]byte(s))
	assert.NoError(t, err)
	assert.NotNil(t, c.GetTable("slideshow"))
	// assert.Len(t, c.Tables["slideshow"], 3)
	// assert.True(t, c.Tables["slideshow"]["title"].NotNull)
}

var config3 = Config{
	Tables: []ConfigTable{
		{
			Name: "table1",
			Fields: []ConfigField{
				{Name: "id"},
				{Name: "createdAt"},
				{
					Name:  "title",
					Type:  TypeText,
					Label: "Title",
					Min:   3,
					Max:   24,
					Regex: "[a-z].*",
				},
				{ // CHANGED
					Name:  "age",
					Type:  TypeInteger,
					Label: "Age",
					Min:   0,
				},
			},
		},
	},
}

var config4 = Config{
	Tables: []ConfigTable{
		{
			Name: "table1",
			Fields: []ConfigField{
				{Name: "id"},
				{Name: "createdAt"},
				{
					Name:    "title",
					Type:    TypeText,
					NotNull: true, // CHANGED
					Label:   "Title",
					Min:     3,
					Max:     24,
					Regex:   "[a-z].*",
				},
				{
					Name:  "age",
					Type:  TypeInteger,
					Label: "Age",
					Min:   0,
				},
				{
					Name:  "color",
					Type:  TypeText,
					Label: "Color",
					Min:   6,
				},
			},
		},
	},
}
