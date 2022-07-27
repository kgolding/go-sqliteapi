package gdb

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

func (rc ResultColumn) String() string {
	return fmt.Sprintf("`%s`.`%s`", rc.Table, rc.Field)
}

func (rc ResultColumn) StringAs() string {
	if rc.As == "" {
		return fmt.Sprintf("`%s`.`%s`", rc.Table, rc.Field)
	} else {
		return fmt.Sprintf("`%s`.`%s` AS `%s`", rc.Table, rc.Field, rc.As)
	}
}

type ResultColumns []ResultColumn

func (rcs ResultColumns) String() string {
	s := make([]string, 0)
	for _, rc := range rcs {
		s = append(s, rc.StringAs())
	}
	return strings.Join(s, ", ")
}

type TableName string

func (tn TableName) String() string {
	return string("`" + tn + "`")
}
