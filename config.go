package gdb

import (
	"fmt"
	"strings"
)

/*

tables:
- name: table1
  fields:
    - name: id
    - name: createdAt
    - name: title
      type: text
      label: Title

*/

var SpecialFields = map[string]TableFieldInfo{
	"id": {
		Name:       "id",
		Type:       "INTEGER",
		NotNull:    true,
		PrimaryKey: 1,
	},
	"createdAt": {
		Name:         "createdAt",
		Type:         "DATETIME",
		NotNull:      true,
		DefaultValue: "CURRENT_TIMESTAMP",
	},
}

const (
	TypeText    = "text"
	TypeInteger = "integer"
	TypeReal    = "real"
	TypeBlob    = "blob"
)

type Config struct {
	Tables []Table `yaml:"tables,omitempty"`
}

type Table struct {
	Name   string  `yaml:"name"`
	Fields []Field `yaml:"fields"`
}

type Field struct {
	// Database
	Name       string      `yaml:"name"`
	Type       string      `yaml:"type,omitempty"`
	NotNull    bool        `yaml:"notnull,omitempty"`
	Default    interface{} `yaml:"default,omitempty"`
	References string      `yaml:"references,omitempty"` // e.g. driver.id // FOREIGN KEY("driverId") REFERENCES "driver"("id")
	PrimaryKey int         `yaml:"pk,omitempty"`

	// UI
	Label    string `yaml:"label,omitempty"`
	Hidden   bool   `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	ReadOnly bool   `yaml:"readonly,omitempty" json:"readonly,omitempty"`

	// User interface
	Control string `yaml:"control,omitempty" json:"control,omitempty"`
	// LookupUri string          `json:"lookupUri,omitempty"`
	// Options   []*SelectOption `json:"options,omitempty"`

	// Validation
	Min    int    `yaml:"min,omitempty" json:"min,omitempty"`
	Max    int    `yaml:"max,omitempty" json:"max,omitempty"`
	RegExp string `yaml:"regexp,omitempty" json:"regexp,omitempty"`
	// RegExpHint string         `json:"regexphint,omitempty"`
	// regexp *regexp.Regexp `yaml:"-" json:"-"`
}

func (table *Table) CreateSQL() (string, error) {
	coldefs := []string{}
	for _, f := range table.Fields {
		coldef, err := f.ColDef()
		if err != nil {
			return "", err
		}
		coldefs = append(coldefs, coldef)
		if f.References != "" {
			r := strings.Split(f.References, ".")
			if len(r) != 2 {
				return "", fmt.Errorf("invalid reference: '%s'", f.References)
			}
			fk := fmt.Sprintf("FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`)", f.Name, r[0], r[1])
			coldefs = append(coldefs, fk)
		}
	}

	sql := "CREATE TABLE " + table.Name + "(\n\t"
	sql += strings.Join(coldefs, ",\n\t")
	sql += ");"

	return sql, nil
}

// Equals compares the database relative fields
func (f *Field) Equals(f2 *Field) bool {
	// fmt.Printf("Equals:\na: %+v\nb: %+v\n", f, f2)
	return f.Name == f2.Name &&
		f.Type == f2.Type &&
		f.NotNull == f2.NotNull &&
		f.Default == f2.Default &&
		f.References == f2.References
}

func (f *Field) ColDef() (string, error) {

	lf := f.applySpecialFields()

	// default:
	datatype := strings.ToUpper(lf.Type)
	if datatype == "" {
		datatype = "TEXT" // Default
	}
	s := fmt.Sprintf("`%s` %s", lf.Name, datatype)
	if lf.NotNull {
		s += " NOT NULL"
	}
	if lf.Default != nil {
		s += " DEFAULT " + fmt.Sprintf("%s", lf.Default)
	}
	if lf.PrimaryKey > 0 {
		s += " PRIMARY KEY "
	}
	return s, nil
}

func (f Field) applySpecialFields() Field {
	if sf, ok := SpecialFields[f.Name]; ok {
		f.Type = sf.Type
		f.NotNull = sf.NotNull
		f.Default = sf.DefaultValue
		f.PrimaryKey = sf.PrimaryKey
	}
	return f
}

func (c *Config) GetTable(name string) *Table {
	for _, table := range c.Tables {
		if table.Name == name {
			return &table
		}
	}
	return nil
}
