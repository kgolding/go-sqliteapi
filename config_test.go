package gdb

import (
	"fmt"
	"log"
	"os"
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
		Tables: []Table{
			config1.Tables[0],
		},
	}
	return config2
}

var OPT = &ConfigOptions{
	RetainUnmanaged: false,
	DryRun:          false,
	Logger:          log.Default(),
}

func TestConfigApplyNew(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=private")
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
}

func TestConfigApplyRemoveTable(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=private")
	assert.NoError(t, err)
	defer db.Close()

	config1 := GetConfig1(t)
	err = db.ApplyConfig(config1, OPT)
	assert.NoError(t, err)
	assert.NoError(t, InsertTable1Data(db))
	assert.Len(t, db.dbInfo, 0) // Before refresh
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
		db, err = NewDatabase("file::memory:?cache=shared") // Log(log.Default()),
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

// var config1 = Config{
// 	Tables: []Table{
// 		{
// 			Name: "table1",
// 			Fields: []Field{
// 				{Name: "id"},
// 				{Name: "createdAt"},
// 				{
// 					Name:   "title",
// 					Type:   TypeText,
// 					Label:  "Title",
// 					Min:    3,
// 					Max:    24,
// 					RegExp: "[a-z].*",
// 				},
// 			},
// 		},
// 		{
// 			Name: "table2",
// 			Fields: []Field{
// 				{Name: "id"},
// 				{
// 					Name:       "t1Id",
// 					Label:      "Table 1 reference",
// 					References: "table1.id",
// 				},
// 			},
// 		},
// 	},
// }

// var config2 = Config{
// 	Tables: []Table{
// 		config1.Tables[0],
// 	},
// }

var config3 = Config{
	Tables: []Table{
		{
			Name: "table1",
			Fields: []Field{
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
	Tables: []Table{
		{
			Name: "table1",
			Fields: []Field{
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
