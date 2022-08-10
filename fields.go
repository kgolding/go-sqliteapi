package sqliteapi

import (
	"fmt"
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
	CONTROL_SELECT   = "select"
	CONTROL_RADIO    = "radio"
)

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
		checkagain:
			switch v := value.(type) {
			case int:
				if tf.Min > 0 && v <= tf.Min {
					return fmt.Errorf("%s: value must be greater or equal to %d", field, tf.Min)
				}
				if tf.Max > 0 && v >= tf.Max {
					return fmt.Errorf("%s: value must be less than or equal to %d", field, tf.Max)
				}

			case float64:
				if tf.Min != 0 && v <= float64(tf.Min) {
					return fmt.Errorf("%s: value must be greater or equal to %d", field, tf.Min)
				}
				if tf.Max != 0 && v >= float64(tf.Max) {
					return fmt.Errorf("%s: value must be less than or equal to %d", field, tf.Max)
				}

			case string:
				if v == "" && tf.NotNull {
					return fmt.Errorf("%s: missing value", field)
				}
				if tf.Min > 0 && len(v) < tf.Min {
					return fmt.Errorf("%s: too short, must be at least %d chars", field, tf.Min)
				}
				if tf.Max > 0 && len(v) > tf.Max {
					return fmt.Errorf("%s: too long, must be no more than %d chars", field, tf.Max)
				}
				if tf.regexp != nil && v != "" && !tf.regexp.MatchString(v) {
					// if md.RegExpHint != "" {
					// 	return fmt.Errorf("%s: invalid format: "+md.RegExpHint, field)
					// }
					return fmt.Errorf("%s: invalid format in value '%s'", field, v)
				}
			default:
				// Convert to string and check it again
				if value == nil {
					value = ""
				} else {
					fmt.Sprintf("FieldValidate: Convert to string ftom %T\n", value)
					value = fmt.Sprintf("%v", value)
				}
				goto checkagain
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
			// Primary keys are always readable
			return tf.PrimaryKey > 0 || !tf.Hidden
		}
	}
	return true // Default
}
