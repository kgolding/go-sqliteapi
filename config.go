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
	Tables   []ConfigTable   `yaml:"tables,omitempty"`
	Triggers []ConfigTrigger `yaml:"triggers,omitempty"`
}

type ConfigTable struct {
	Name   string        `yaml:"name"`
	Fields []ConfigField `yaml:"fields"`
}

type ConfigTrigger struct {
	Name      string `yaml:"name"`
	Event     string `yaml:"event"` // DELETE, INSERT, UPDATE
	Table     string `yaml:"table"`
	Statement string `yaml:"statement"`
}

type Reference struct {
	Table      string `yaml:"table"`
	KeyField   string `yaml:"key"`
	LabelField string `yaml:"label"`
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
	Name       string      `yaml:"name" json:"-"`
	Type       string      `yaml:"type" json:"-"`
	NotNull    bool        `yaml:"notnull" json:"-"`
	Default    interface{} `yaml:"default" json:"-"`
	References string      `yaml:"ref" json:"ref,omitempty"` // e.g. driver.id // FOREIGN KEY("driverId") REFERENCES "driver"("id")
	PrimaryKey int         `yaml:"pk" json:"-"`
	Unique     bool        `yaml:"unique" json:"unique,omitempty"`

	// UI
	Label    string `yaml:"label" json:"label,omitempty"`
	Hidden   bool   `yaml:"hidden" json:"hidden,omitempty"`
	ReadOnly bool   `yaml:"readonly" json:"readonly,omitempty"`
	Hint     string `yaml:"hint" json:"hint,omitempty"`

	// User interface
	Control string `yaml:"control" json:"control,omitempty"`
	// Options   []*SelectOption `json:"options,omitempty"`

	// Validation
	Min    int            `yaml:"min" json:"min,omitempty"`
	Max    int            `yaml:"max" json:"max,omitempty"`
	Regex  string         `yaml:"regex" json:"regex,omitempty"`
	regexp *regexp.Regexp `yaml:"-" json:"-"`
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
			"Type",
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
				f.Type,
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

	for _, t := range c.Triggers {
		s += fmt.Sprintf("Trigger %s\n", t.CreateSQL())
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

func (trigger *ConfigTrigger) CreateSQL() string {
	statement := trigger.Statement
	if !strings.HasSuffix(statement, ";") {
		statement += ";"
	}

	sql := "CREATE TRIGGER " + trigger.Name + "\n"
	sql += trigger.Event + " ON " + trigger.Table + "\n"
	sql += "FOR EACH ROW BEGIN\n"
	sql += "\t" + statement + "\n"
	sql += "END;"

	return sql
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

	sql := "CREATE TABLE \"" + table.Name + "\" (\n\t"
	sql += strings.Join(append(coldefs, forkeys...), ",\n\t")
	sql += ")"

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
	if f.Unique {
		s += " UNIQUE"
	}
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

type YConfig struct {
	Tables   map[string]map[string]ConfigField `yaml:"tables"`
	Triggers map[string]ConfigTrigger          `yaml:"triggers"`
}

func NewConfigFromYaml(b []byte) (*Config, error) {
	type Fields map[string]ConfigField

	var c struct {
		Tables   map[string]yaml.MapSlice
		Triggers map[string]yaml.MapSlice
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

	for triggerName, fields := range c.Triggers {
		trigger := ConfigTrigger{
			Name: triggerName,
		}
		for _, x := range fields {
			name, ok := x.Key.(string)
			if !ok {
				return nil, fmt.Errorf("%s: field name is not a string! It is a %T (%v)", triggerName, x.Key, x.Key)
			}
			if x.Value != nil {
				var s string
				switch v := x.Value.(type) {
				case string:
					s = v
				case []byte:
					s = string(v)
				case nil:
					return nil, fmt.Errorf("%s.%s: field param missing", triggerName, name)
				default:
					s = fmt.Sprintf("%v", x.Value)
				}
				switch name {
				case "event":
					trigger.Event = s // No ToUpper as might have a field name in it e.g. "INSERT ON fieldName"
				case "table":
					trigger.Table = s
				case "statement":
					trigger.Statement = s
				default:
					return nil, fmt.Errorf("%s: unknown field '%s'", triggerName, name)
				}
			}
		}
		cfg.Triggers = append(cfg.Triggers, trigger)
	}

	sort.Slice(cfg.Tables, func(a, b int) bool {
		return cfg.Tables[a].Name < cfg.Tables[b].Name
	})

	sort.Slice(cfg.Triggers, func(a, b int) bool {
		return cfg.Triggers[a].Name < cfg.Tables[b].Name
	})

	// println("=========================================================")
	// println(string(b))
	// println("---------------------------------------------------------")
	// println(cfg.String())
	// println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	return cfg, nil
}
