package gdb

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

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
	Name       string      `yaml:"name" json:"-"`
	Type       string      `yaml:"type,omitempty" json:"-"`
	NotNull    bool        `yaml:"notnull,omitempty" json:"-"`
	Default    interface{} `yaml:"default,omitempty" json:"-"`
	References string      `yaml:"references,omitempty" json:"references,omitempty"` // e.g. driver.id // FOREIGN KEY("driverId") REFERENCES "driver"("id")
	PrimaryKey int         `yaml:"pk,omitempty" json:"-"`

	// UI
	Label    string `yaml:"label,omitempty" json:"label,omitempty"`
	Hidden   bool   `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	ReadOnly bool   `yaml:"readonly,omitempty" json:"readonly,omitempty"`
	Hint     string `yaml:"hint,omitempty" json:"hint,omitempty"`

	// User interface
	Control string `yaml:"control,omitempty" json:"control,omitempty"`
	// Lookup  struct {
	// 	Table   string `yaml:"table"`
	// 	Field   string `yaml:"field"`
	// 	Display string `yaml:"display"` // e.g. "title"
	// } `json:"lookup,omitempty"`
	// Options   []*SelectOption `json:"options,omitempty"`

	// Validation
	Min    int            `yaml:"min,omitempty" json:"min,omitempty"`
	Max    int            `yaml:"max,omitempty" json:"max,omitempty"`
	Regex  string         `yaml:"regex,omitempty" json:"regex,omitempty"`
	regexp *regexp.Regexp `yaml:"-" json:"-"`
}

func (c *Config) String() string {
	s := "Config:\n"

	for _, t := range c.Tables {
		q, err := t.CreateSQL()
		if err != nil {
			q = "Error: " + err.Error()
		}

		s += fmt.Sprintf("Table `%s`: SQL: %s\n", t.Name, q)

		a := make([][]string, 0)
		a = append(a, []string{
			"Name",
			"Label",
			"Control",
			"Readonly",
			"Hidden",
			"Min",
			"Max",
			"References",
			"Default",
		})
		for _, f := range t.Fields {
			ro := "-"
			if f.ReadOnly {
				ro = "Readonly"
			}
			hidden := "-"
			if f.Hidden {
				ro = "Hidden"
			}
			a = append(a, []string{
				f.Name,
				f.Label,
				f.Control,
				ro,
				hidden,
				fmt.Sprintf("%d", f.Min),
				fmt.Sprintf("%d", f.Max),
				f.References,
				fmt.Sprintf("%v", f.Default),
			})
		}
		s += tabular(a)
	}

	return s
}

func tabular(input [][]string) string {
	colWidths := make([]int, 0)

	for _, row := range input {
		for i, c := range row {
			w := len(c)
			if i >= len(colWidths) {
				colWidths = append(colWidths, w)
			} else {
				if w > colWidths[i] {
					colWidths[i] = w
				}
			}
		}
	}

	s := ""
	for _, row := range input {
		for i, c := range row {
			s += strings.Repeat(" ", colWidths[i]-len(c)) + c + "\t"
		}
		s += "\n"
	}
	return s
}

var regexRef = regexp.MustCompile(`(?m)(\w+)\.(\w+)(?:[/\\](\w+))?`)

func (table *Table) CreateSQL() (string, error) {
	coldefs := []string{}
	forkeys := []string{}
	for _, f := range table.Fields {
		coldef, err := f.ColDef()
		if err != nil {
			return "", err
		}
		coldefs = append(coldefs, coldef)
		if f.References != "" {
			r := regexRef.FindStringSubmatch(f.References)
			if len(r) != 4 {
				return "", fmt.Errorf("invalid reference: '%s'", f.References)
			}
			fk := fmt.Sprintf("FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`)", f.Name, r[1], r[2])
			forkeys = append(forkeys, fk)
		}
	}

	sql := "CREATE TABLE " + table.Name + "(\n\t"
	sql += strings.Join(append(coldefs, forkeys...), ",\n\t")
	sql += ");"

	return sql, nil
}

func (a *Field) CompareDbFields(b *TableFieldInfo) error {
	if a.Name != b.Name {
		return fmt.Errorf("name: '%s' != '%s'", a.Name, b.Name)
	}
	if !strings.EqualFold(a.Type, b.Type) {
		return fmt.Errorf("type: '%s' != '%s'", a.Type, b.Type)
	}
	if fmt.Sprintf("%v", a.Default) != fmt.Sprintf("%v", b.DefaultValue) {
		return fmt.Errorf("default: '%v' != '%v'", a.Default, b.DefaultValue)
	}
	if a.NotNull != b.NotNull {
		return fmt.Errorf("notnull: '%t' != '%t'", a.NotNull, b.NotNull)
	}
	if a.PrimaryKey != b.PrimaryKey {
		return fmt.Errorf("primary key: '%d' != '%d'", a.PrimaryKey, b.PrimaryKey)
	}
	return nil
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
		v := fmt.Sprintf("%v", lf.Default)
		if strings.HasPrefix(v, "#") {
			v = `'` + v + `'`
		}
		s += " DEFAULT " + v
	}
	if lf.PrimaryKey > 0 {
		s += " PRIMARY KEY"
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
	if c == nil {
		return nil
	}
	for _, table := range c.Tables {
		if table.Name == name {
			return &table
		}
	}
	return nil
}

func NewConfigFromYaml(b []byte) (*Config, error) {
	type Fields map[string]Field

	var c struct {
		Tables map[string]yaml.MapSlice
	}
	err := yaml.Unmarshal(b, &c)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}

	for tableName, fields := range c.Tables {
		t := Table{
			Name: tableName,
		}
		for _, x := range fields {
			f := Field{ // Defaults
				Type: "text",
			}

			var ok bool
			f.Name, ok = x.Key.(string)
			if !ok {
				return nil, fmt.Errorf("%s: field name is not a string! It is a %T (%v)", tableName, x.Key, x.Key)
			}

			f = f.applySpecialFields()
			if x.Value != nil {
				m, ok := x.Value.(yaml.MapSlice)
				if !ok {
					return nil, fmt.Errorf("%s.%s: expected map, got %T", tableName, f.Name, x.Value)
				}
				for _, y := range m {
					name, ok := y.Key.(string)
					if !ok {
						return nil, fmt.Errorf("%s.%s: field param is not a string", tableName, f.Name)
					}
					var s string
					var tf bool
					var i int
					switch x := y.Value.(type) {
					case string:
						s = x
					case []byte:
						s = string(x)
					case bool:
						tf = x
					case int:
						i = x
					case int64:
						i = int(x)
					case nil:
						return nil, fmt.Errorf("%s.%s: field param missing", tableName, f.Name)
					default:
						s = fmt.Sprintf("%v", y.Value)
					}
					// slower := strings.ToLower(s)
					// tf = slower == "1" || slower == "on" || slower == "yes" || slower == "true"
					if i == 0 {
						i, _ = strconv.Atoi(s)
					}
					switch name {
					case "type":
						f.Type = s
					case "notnull":
						f.NotNull = tf
					case "default":
						f.Default = removeQuotesIfString(y.Value)
					case "references", "ref":
						f.References = s
					case "pk", "primarykey":
						f.PrimaryKey = i
					case "label":
						f.Label = s
					case "hidden":
						f.Hidden = tf
					case "readonly", "ro":
						f.ReadOnly = tf
					case "hint":
						f.Hint = s
					case "control":
						f.Control = s
					case "min":
						f.Min = i
					case "max":
						f.Max = i
					case "regexp", "regex", "reg":
						f.Regex = s
						f.regexp, err = regexp.Compile(s)
						if err != nil {
							return nil, fmt.Errorf("%s.%s: Regexp error: %w", tableName, f.Name, err)
						}
					default:
						return nil, fmt.Errorf("%s.%s: unknown field '%s'", tableName, f.Name, name)
					}
				}
			}

			t.Fields = append(t.Fields, f)
		}
		cfg.Tables = append(cfg.Tables, t)
	}

	return cfg, nil
}
