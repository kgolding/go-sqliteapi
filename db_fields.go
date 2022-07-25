package gdb

import (
	"fmt"
	"regexp"
)

const (
	CONTROL_INPUT    = "text" // Default
	CONTROL_NUMBER   = "number"
	CONTROL_DATE     = "date"
	CONTROL_TIME     = "time"
	CONTROL_DATETIME = "datetime"
	CONTROL_TEXTAREA = "textarea"
	CONTROL_CHECKBOX = "checkbox"
	CONTROL_HTML     = "html"
	CONTROL_SELECT   = "select" // Requires options and/or LookupUri
	CONTROL_RADIO    = "radio"  // Requires options and/or LookupUri
)

type SelectOption struct {
	Key   interface{} `json:"k"`
	Value interface{} `json:"v"`
}

type TableFieldMetaData struct {
	Hidden         bool `json:"hidden,omitempty"`
	WriteProtected bool `json:"writeprotected,omitempty"`

	// User interface
	Control   string          `json:"control,omitempty"`
	LookupUri string          `json:"lookupUri,omitempty"`
	Options   []*SelectOption `json:"options,omitempty"`

	// Validation
	MinLen     int            `json:"minlen,omitempty"`
	MaxLen     int            `json:"maxlen,omitempty"`
	RegExp     string         `json:"regexp,omitempty"`
	RegExpHint string         `json:"regexphint,omitempty"`
	regexp     *regexp.Regexp `json:"-"`
}

func (d *Database) CheckTableNameGetFields(table string) ([]TableFieldInfo, error) {
	info, ok := d.dbInfo[table]
	if !ok {
		return nil, fmt.Errorf("invalid table name '%s'", table)
	}
	return info.Fields, nil
}

func NewTableFieldMetaData() *TableFieldMetaData {
	return &TableFieldMetaData{}
}

// // SetFieldMetaData to set or clear by passing a nil
// func (d *Database) SetFieldMetaData(table string, field string, md *TableFieldMetaData) error {
// 	if md == nil {
// 		if f, ok := d.tableFieldsMetaData[field]; ok {
// 			if _, ok := f[table]; ok {
// 				delete(f, table)
// 			}
// 		}
// 		return nil
// 	}

// 	if md.RegExp != "" {
// 		var err error
// 		md.regexp, err = regexp.Compile(md.RegExp)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	if _, ok := d.tableFieldsMetaData[field]; !ok {
// 		d.tableFieldsMetaData[field] = make(map[string]*TableFieldMetaData)
// 	}
// 	d.tableFieldsMetaData[field][table] = md
// 	return nil
// }

// func (d *Database) GetFieldMetaData(table string, field string) *TableFieldMetaData {
// 	if f, ok := d.tableFieldsMetaData[field]; ok {
// 		// Use table & field as primary match
// 		if md, ok := f[table]; ok {
// 			cp := *md
// 			return &cp
// 		}
// 		// Else use table wildcard
// 		if md, ok := f[""]; ok {
// 			cp := *md
// 			return &cp
// 		}
// 	}
// 	return &TableFieldMetaData{}
// }

func (d *Database) FieldValidation(table string, field string, value interface{}) error {
	if d.config == nil {
		return nil
	}

	t := d.config.GetTable(table)
	if t == nil {
		return nil
	}

	for _, tf := range t.Fields {
		if tf.Name == field {
			s := fmt.Sprintf("%v", value)
			if tf.Min > 0 && len(s) < tf.Min {
				return fmt.Errorf("%s: too short, must be at least %d chars", field, tf.Min)
			}
			if tf.Max > 0 && len(s) > tf.Max {
				return fmt.Errorf("%s: too long, must be no more than %d chars", field, tf.Max)
			}
			if tf.RegExp != "" && !tf.regexp.MatchString(s) {
				// if md.RegExpHint != "" {
				// 	return fmt.Errorf("%s: invalid format: "+md.RegExpHint, field)
				// }
				return fmt.Errorf("%s: invalid format", field)
			}
		}
	}
	return nil
}

func (d *Database) IsFieldWritable(table string, field string) bool {
	if d.config == nil {
		return true
	}

	t := d.config.GetTable(table)
	if t == nil {
		return true
	}

	for _, tf := range t.Fields {
		if tf.Name == field {
			return !tf.ReadOnly
		}
	}
	return true // Default
}

func (d *Database) IsFieldReadable(table string, field string) bool {
	if d.config == nil {
		return true
	}

	t := d.config.GetTable(table)
	if t == nil {
		return true
	}

	for _, tf := range t.Fields {
		if tf.Name == field {
			return !tf.Hidden
		}
	}
	return true // Default
}
