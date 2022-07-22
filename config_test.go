package gdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestConfigApplyNew(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=private")
	assert.NoError(t, err)
	defer db.Close()

	err = config1.Apply(db)
	assert.NoError(t, err)

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

	err = config1.Apply(db)
	assert.NoError(t, err)
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

	err = config2.Apply(db)
	db.Refresh()
	assert.NoError(t, err)
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
}

func TestConfigApply1234(t *testing.T) {
	db, err := NewDatabase("file::memory:?cache=shared")
	// os.Remove("test.db")
	// db, err := NewDatabase("test.db")
	assert.NoError(t, err)
	defer db.Close()

	println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Config 1")
	assert.Len(t, db.dbInfo, 0)
	assert.NoError(t, config1.Apply(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 2)

	println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Config 2")
	assert.NoError(t, config2.Apply(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)

	println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Config 3")
	assert.NoError(t, config3.Apply(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
	assert.Len(t, db.dbInfo[config3.Tables[0].Name].Fields, 4)

	println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Config 4")
	assert.True(t, config4.Tables[0].Fields[2].NotNull)
	assert.NoError(t, config4.Apply(db))
	assert.NoError(t, db.Refresh())
	assert.Len(t, db.dbInfo, 1)
	assert.Len(t, db.dbInfo[config3.Tables[0].Name].Fields, 5)
	assert.True(t, db.dbInfo[config3.Tables[0].Name].Fields[2].NotNull)

}

func TestConfigMarshal(t *testing.T) {
	b, err := yaml.Marshal(config1)
	assert.NoError(t, err)
	assert.True(t, len(b) > 20)
}

func TestConfigGetTable(t *testing.T) {
	assert.Len(t, config1.Tables, 2)
	assert.NotNil(t, config1.GetTable(config1.Tables[0].Name))
	assert.Nil(t, config1.GetTable("dummy"))
	var c Config
	assert.Nil(t, c.GetTable("dummy"))
}

var config1 = Config{
	Tables: []Table{
		{
			Name: "table1",
			Fields: []Field{
				{Name: "id"},
				{Name: "createdAt"},
				{
					Name:   "title",
					Type:   TypeText,
					Label:  "Title",
					Min:    3,
					Max:    24,
					RegExp: "[a-z].*",
				},
			},
		},
		{
			Name: "table2",
			Fields: []Field{
				{Name: "id"},
				{
					Name:       "t1Id",
					Label:      "Table 1 reference",
					References: "table1.id",
				},
			},
		},
	},
}

var config2 = Config{
	Tables: []Table{
		config1.Tables[0],
	},
}

var config3 = Config{
	Tables: []Table{
		{
			Name: "table1",
			Fields: []Field{
				{Name: "id"},
				{Name: "createdAt"},
				{
					Name:   "title",
					Type:   TypeText,
					Label:  "Title",
					Min:    3,
					Max:    24,
					RegExp: "[a-z].*",
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
					RegExp:  "[a-z].*",
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
