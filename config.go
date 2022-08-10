package sqliteapi

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

var SpecialFields = map[string]ConfigField{
	"id": {
		Name:       "id",
		Type:       "INTEGER",
		NotNull:    true,
		PrimaryKey: 1,
		ReadOnly:   true,
	},
	"createdAt": {
		Name:     "createdAt",
		Type:     "DATETIME",
		NotNull:  true,
		Default:  "CURRENT_TIMESTAMP",
		ReadOnly: true,
	},
}

const (
	TypeText    = "text"
	TypeInteger = "integer"
	TypeReal    = "real"
	TypeBlob    = "blob"
)

type Config struct {
	Tables []ConfigTable `yaml:"tables,omitempty"`
}

type ConfigTable struct {
	Name   string        `yaml:"name"`
	Fields []ConfigField `yaml:"fields"`
}

type Reference struct {
	Table      string
	KeyField   string
	LabelField string
}

type BackReference struct {
	SourceTable string
	SourceField string
	Reference
}

// https://regex101.com/r/wpohXh/1
var regexRef = regexp.MustCompile(`(\w+)\.(\w+)\s*(?:[/\\]\s*(\w+\s*(?:,\s*\w+)*))?`)

func NewReference(s string) (*Reference, error) {
	r := regexRef.FindStringSubmatch(s)
	if len(r) != 4 {
		return nil, fmt.Errorf("invalid reference: '%s'", s)
	}
	return &Reference{
		Table:      r[1],
		KeyField:   r[2],
		LabelField: r[3],
	}, nil
}

func (r Reference) ResultColLabel(as string) ResultColumn {
	return ResultColumn{
		Table: r.Table,
		Field: r.LabelField,
		As:    as,
	}
}

func (r Reference) ResultColKey(as string) ResultColumn {
	return ResultColumn{
		Table: r.Table,
		Field: r.KeyField,
		As:    as,
	}
}

type ConfigField struct {
	// Database
	Name       string      `json:"-"`
	Type       string      `json:"-"`
	NotNull    bool        `json:"-"`
	Default    interface{} `json:"-"`
	References string      `json:"ref,omitempty"` // e.g. driver.id // FOREIGN KEY("driverId") REFERENCES "driver"("id")
	PrimaryKey int         `json:"-"`
	Unique     bool        `json:"unique,omitempty"`

	// UI
	Label    string `json:"label,omitempty"`
	Hidden   bool   `json:"hidden,omitempty"`
	ReadOnly bool   `json:"readonly,omitempty"`
	Hint     string `json:"hint,omitempty"`

	// User interface
	Control string `json:"control,omitempty"`
	// Options   []*SelectOption `json:"options,omitempty"`

	// Validation
	Min    int            `json:"min,omitempty"`
	Max    int            `json:"max,omitempty"`
	Regex  string         `json:"regex,omitempty"`
	regexp *regexp.Regexp `json:"-"`
}

func (c *Config) String() string {
	s := "Config:\n"

	for _, t := range c.Tables {
		q, err := t.CreateSQL()
		if err != nil {
			q = "Error: " + err.Error()
		}

		s += fmt.Sprintf("Table `%s`\n%s\n", t.Name, q)

		a := make([][]string, 0)
		a = append(a, []string{
			"Name",
			"PK",
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
				fmt.Sprintf("%d", f.PrimaryKey),
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

func (table *ConfigTable) CreateSQL() (string, error) {
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

func (table *ConfigTable) PrimaryKey() string {
	for _, f := range table.Fields {
		if f.PrimaryKey > 0 {
			return f.Name
		}
	}
	return ""
}

func (a *ConfigField) CompareDbFields(b *TableFieldInfo) error {
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

func (f *ConfigField) ColDef() (string, error) {

	// lf := f.applySpecialFields()

	// default:
	datatype := strings.ToUpper(f.Type)
	if datatype == "" {
		datatype = "TEXT" // Default
	}
	s := fmt.Sprintf("`%s` %s", f.Name, datatype)
	if f.NotNull {
		s += " NOT NULL"
	}
	if f.Default != nil {
		v := fmt.Sprintf("%v", f.Default)
		if strings.HasPrefix(v, "#") {
			v = `'` + v + `'`
		}
		s += " DEFAULT " + v
	}
	if f.PrimaryKey > 0 {
		s += " PRIMARY KEY"
	}
	return s, nil
}

func newConfigFieldWithDefaults(name string) ConfigField {
	if sf, ok := SpecialFields[name]; ok {
		sf.Name = name
		return sf
	}
	return ConfigField{
		Name: name,
		Type: "TEXT",
	}
}

// func (f ConfigField) applySpecialFields() ConfigField {
// 	if sf, ok := SpecialFields[f.Name]; ok {
// 		f.Type = sf.Type
// 		f.NotNull = sf.NotNull
// 		f.Default = sf.DefaultValue
// 		f.PrimaryKey = sf.PrimaryKey
// 	}
// 	return f
// }

func (c *Config) GetTable(name string) *ConfigTable {
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

func (c *Config) GetBackReferences(name string) []*BackReference {
	ret := make([]*BackReference, 0)
	for _, table := range c.Tables {
		if table.Name != name {
			for _, f := range table.Fields {
				if f.References != "" {
					ref, err := NewReference(f.References)
					if err == nil && ref.Table == name {
						ret = append(ret, &BackReference{
							Reference:   *ref,
							SourceTable: table.Name,
							SourceField: f.Name,
						})
					}
				}
			}
		}
	}
	return ret
}

func NewConfigFromYaml(b []byte) (*Config, error) {
	type Fields map[string]ConfigField

	var c struct {
		Tables map[string]yaml.MapSlice
	}
	err := yaml.Unmarshal(b, &c)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}

	for tableName, fields := range c.Tables {
		t := ConfigTable{
			Name: tableName,
		}
		for _, x := range fields {
			name, ok := x.Key.(string)
			if !ok {
				return nil, fmt.Errorf("%s: field name is not a string! It is a %T (%v)", tableName, x.Key, x.Key)
			}

			f := newConfigFieldWithDefaults(name)

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
						if x {
							i = 1
						}
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
					case "ref":
						f.References = s
					case "pk":
						f.PrimaryKey = i
					case "unique":
						f.Unique = tf
					case "label":
						f.Label = s
					case "hidden":
						f.Hidden = tf
					case "readonly":
						f.ReadOnly = tf
					case "hint":
						f.Hint = s
					case "control":
						f.Control = s
					case "min":
						f.Min = i
					case "max":
						f.Max = i
					case "regex":
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

	sort.Slice(cfg.Tables, func(a, b int) bool {
		return cfg.Tables[a].Name < cfg.Tables[b].Name
	})

	return cfg, nil
}
