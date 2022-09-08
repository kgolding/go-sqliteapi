package sqliteapi

import (
	"errors"
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
	Tables    []ConfigTable    `yaml:"tables,omitempty"`
	Triggers  []ConfigTrigger  `yaml:"triggers,omitempty"`
	Functions []ConfigFunction `yaml:"functions,omitempty"`
	Views     []ConfigView     `yaml:"views,omitempty"`
}

type ConfigTable struct {
	Name   string        `yaml:"name"`
	Fields []ConfigField `yaml:"fields"`
}

type ConfigTrigger struct {
	Name      string `yaml:"name"`
	Event     string `yaml:"event"` // DELETE, INSERT, UPDATE
	Table     string `yaml:"table"`
	When      string `yaml:"when"`
	Statement string `yaml:"statement"`
}

type ConfigFunction struct {
	Name       string                `yaml:"name"`
	Params     []ConfigFunctionParam `yaml:"params"`
	Statements []string              `yaml:"statements"`
}

type ConfigView struct {
	Name      string `yaml:"name"`
	Statement string `yaml:"statement"`
}

type ConfigFunctionParam struct {
	Name    string `yaml:"name"`
	Notnull bool   `yaml:"notnull"`
	Min     int64  `yaml:"min"`
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

	// Indirectly database related
	Indexed bool `yaml:"indexed" json:"indexed,omitempty"`

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
	dashes := strings.Repeat("-", 80) + "\n"

	for _, t := range c.Tables {
		s += dashes
		q, err := t.CreateSQL()
		if err != nil {
			q = "Error: " + err.Error()
		}

		s += fmt.Sprintf("Table: %s\n", t.Name)

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
			"Indexed",
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
				fmt.Sprintf("%t", f.Indexed),
				f.References,
				fmt.Sprintf("%v", f.Default),
			})
		}
		s += tabular(a) + "\n"
		s += q + "\n"
	}

	for _, t := range c.Triggers {
		s += dashes
		s += fmt.Sprintf("Trigger %s\n\n", t.CreateSQL())
	}

	for _, f := range c.Functions {
		s += dashes
		s += fmt.Sprintf("Function %s\n\tParams: ", f.Name)
		for i, p := range f.Params {
			if i > 0 {
				s += ", "
			}
			s += p.Name
		}
		for _, x := range f.Statements {
			s += "\n\t- " + x
		}
		s += "\n"
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
	sql += "FOR EACH ROW" // Optional, same as the default behaviour
	if trigger.When != "" {
		sql += " WHEN " + trigger.When
	}
	sql += "\nBEGIN\n"
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
	if c != nil {
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
		Tables    map[string]yaml.MapSlice
		Triggers  map[string]yaml.MapSlice
		Functions map[string]yaml.MapSlice
		Views     map[string]string
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
					case "indexed":
						f.Indexed = tf
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
				case "when":
					trigger.When = s
				case "statement":
					trigger.Statement = s
				default:
					return nil, fmt.Errorf("%s: unknown field '%s'", triggerName, name)
				}
			}
		}
		cfg.Triggers = append(cfg.Triggers, trigger)
	}

	/*
	   function:
	     setThingStatus:  								- functionName
	       params:										- ps
	         statusId:									- paramFields
	           notnull: true								- f/v
	           min: 1									- f/v
	         thingId:
	           notnull: true
	           min: 1
	         userId:
	       statements:									- statements
	         - UPDATE thing SET statusId=$1 WHERE id=$2
	*/

	for functionName, mFunction := range c.Functions {
		function := ConfigFunction{
			Name: functionName,
		}
		forEachMapSlice(mFunction, func(ps string, psValue interface{}) error {
			switch ps {
			case "params":
				err = forEachMapSlice(psValue, func(f string, paramFields interface{}) error {
					param := ConfigFunctionParam{
						Name: f,
					}
					if paramFields != nil {
						err = forEachMapSlice(paramFields, func(f string, v interface{}) error {
							switch f {
							case "notnull":
								param.Notnull, err = toBool(v)
								if err != nil {
									return err
								}
							case "min":
								param.Min, err = toInt(v)
								if err != nil {
									return err
								}
							}
							return nil
						})
						if err != nil {
							return err
						}
					}
					function.Params = append(function.Params, param)
					return nil
				})
				if err != nil {
					return err
				}

			case "statements":
				stmts, ok := psValue.([]interface{})
				if !ok {
					return fmt.Errorf("function %s.statements: not an array of strings: %#v", functionName, psValue)
				}
				for _, x := range stmts {
					s, ok := x.(string)
					if !ok {
						return fmt.Errorf("function %s.statements: not a strings: %#v", functionName, x)
					}
					function.Statements = append(function.Statements, s)
				}
			}
			return nil
		})
		cfg.Functions = append(cfg.Functions, function)
	}
	// VIEWS
	for viewName, viewStatement := range c.Views {
		view := ConfigView{
			Name:      viewName,
			Statement: viewStatement,
		}
		cfg.Views = append(cfg.Views, view)
	}
	if err != nil {
		return nil, err
	}

	sort.Slice(cfg.Tables, func(a, b int) bool {
		return cfg.Tables[a].Name < cfg.Tables[b].Name
	})

	sort.Slice(cfg.Triggers, func(a, b int) bool {
		return cfg.Triggers[a].Name < cfg.Tables[b].Name
	})

	sort.Slice(cfg.Views, func(a, b int) bool {
		return cfg.Views[a].Name < cfg.Views[b].Name
	})

	// println("=========================================================")
	// println(string(b))
	// println("---------------------------------------------------------")
	// println(cfg.String())
	// println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	return cfg, nil
}

func toInt(i interface{}) (int64, error) {
	switch x := i.(type) {
	case string:
		return strconv.ParseInt(x, 10, 64)
	case []byte:
		return strconv.ParseInt(string(x), 10, 64)
	case bool:
		if x {
			return 1, nil
		} else {
			return 0, nil
		}
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case nil:
		return 0, errors.New("nil")
	}
	return 0, fmt.Errorf("unknown type %T: %v", i, i)
}

func toBool(i interface{}) (bool, error) {
	switch x := i.(type) {
	case string:
		switch strings.ToLower(x) {
		case "true", "1", "yes":
			return true, nil
		case "false", "0", "no":
			return false, nil
		}
		return false, fmt.Errorf("unable to convert to bool: '%s'", x)
	case []byte:
		return toBool(string(x))
	case bool:
		return x, nil
	case int:
		return x != 0, nil
	case int64:
		return x != 0, nil
	case nil:
		return false, errors.New("nil")
	}
	return false, fmt.Errorf("unknown type %T: %v", i, i)
}

func forEachMapSlice(ms interface{}, fn func(name string, value interface{}) error) error {
	x, ok := ms.(yaml.MapSlice)
	if !ok {
		return fmt.Errorf("not a mapslice: %#v", ms)
	}
	for _, ks := range x {
		key, ok := ks.Key.(string)
		if !ok {
			return fmt.Errorf("not a string: %#v", key)
		}
		err := fn(key, ks.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
