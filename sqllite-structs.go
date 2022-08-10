package sqliteapi

import (
	"fmt"
	"strings"
)

// ResultColumn is a `table`.`field` string without the quotes
// https://www.sqlite.org/syntax/result-column.html
type ResultColumn struct {
	Table string
	Field string
	As    string
}

// String returns a fully qualified string e.g. `table1`.`field1`
// Including concating comma seperated fields
func (rc ResultColumn) String() string {
	fields := strings.Split(rc.Field, ",")
	s := []string{}
	for _, f := range fields {
		f = strings.TrimSpace(f)
		s = append(s, fmt.Sprintf("`%s`.`%s`", rc.Table, f))
	}
	return strings.Join(s, " || '|' ||")
}

// String returns a fully qualified string with the optional alias
// e.g. `table1`.`field1` AS `alias1`
func (rc ResultColumn) StringAs() string {
	if rc.As == "" {
		return rc.String()
	} else {
		return fmt.Sprintf("%s AS `%s`", rc.String(), rc.As)
	}
}

// type ResultColumns []ResultColumn

// // String returns a comma seperated fully qualified string including aliases
// // e.g. `table1`.`field1`, `table1`.`field2` AS `alias1`
// func (rcs ResultColumns) String() string {
// 	s := make([]string, 0)
// 	for _, rc := range rcs {
// 		s = append(s, rc.StringAs())
// 	}
// 	return strings.Join(s, ", ")
// }
