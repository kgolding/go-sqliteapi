package gdb

import (
	"fmt"
	"regexp"
	"strings"
)

type SelectBuilder struct {
	// Fully quailified `table`.`field`
	Select []string

	// From table name
	From string

	// Joins
	Joins []Join

	// Where
	Where []string

	// Group
	// @TODO

	// Order by
	OrderBy []OrderBy

	// Limit default 1000
	Limit uint

	// Offset default 0
	Offset uint
}

type Join struct {
	Type  JoinType
	Table string
	On    []JoinOn
}

type JoinOn struct {
	Field       string
	ParentField string
}

type JoinType string

type OrderBy struct {
	Field     string
	Ascending bool
}

const (
	LEFT_OUTER = JoinType("LEFT OUTER")
)

func (sb *SelectBuilder) ToSql() (string, error) {
	// args := make([]interface{}, 0)

	// SELECT
	s := "SELECT "
	if len(sb.Select) == 0 {
		s += fmt.Sprintf("`%s`.*", sb.From)
	} else {
		for i, f := range sb.Select {
			if i > 0 {
				s += ", "
			}
			if strings.HasPrefix(f, "`") {
				// fmt.Printf("SB: '%s' => '%s'\n", f, f)
				s += f
			} else {
				// fmt.Printf("SB: '%s' => '%s'\n", f, tableFieldWrapped(sb.From, f))
				s += tableFieldWrapped(sb.From, f)
			}
		}
	}

	// FROM
	s += "\nFROM `" + sb.From + "`"

	// JOINS
	joins := make([]string, 0)
	for _, j := range sb.Joins {
		tmp := "\n" + string(j.Type) + " JOIN `" + j.Table + "` ON "
		for i, on := range j.On {
			if i > 0 {
				tmp += " AND "
			}
			tmp += tableFieldWrapped(j.Table, on.Field) + "=" + tableFieldWrapped(sb.From, on.ParentField)
		}
		exists := false
		for _, s := range joins {
			if s == tmp {
				exists = true
				break
			}
		}
		if !exists {
			joins = append(joins, tmp)
		}
	}
	s += strings.Join(joins, "")

	// WHERE
	if len(sb.Where) > 0 {
		s += "\nWHERE "
		for i, w := range sb.Where {
			if i > 0 {
				s += " AND "
			}
			s += w
		}

	}

	// ORDER BY
	if len(sb.OrderBy) > 0 {
		s += "\nORDER BY "
		for i, ob := range sb.OrderBy {
			if i > 0 {
				s += ", "
			}
			s += tableFieldWrapped(sb.From, ob.Field)
			if ob.Ascending {
				s += " ASC"
			} else {
				s += " DESC"
			}
		}
	}

	// LIMIT/OFFSET
	if sb.Limit+sb.Offset > 0 {
		limit := sb.Limit
		if limit == 0 {
			limit = 1000
		}
		s += fmt.Sprintf("\nLIMIT %d,%d", sb.Offset, limit)
	}

	return s, nil
}

func tableFieldWrapped(table string, field string) string {
	if table == "" {
		return fmt.Sprintf("`%s`", field)
	}
	return fmt.Sprintf("`%s`.`%s`", table, field)
}

func tableFieldUnWrapped(tf string) (string, string) {
	m := regTableField.FindStringSubmatch(tf)
	if len(m) == 3 {
		return m[1], m[2]
	}
	return "", tf
}

var regTableField = regexp.MustCompile(`\x60?(\w+)\x60?.\x60?(\w+)\x60?`)

// WHERE HELPERS

func EqualsArg(table, field string) string {
	return tableFieldWrapped(table, field) + "=?"
}

func ConditionArg(table, field, test string) string {
	return tableFieldWrapped(table, field) + " " + test + " ?"
}

func LikeArg(table, field string) string {
	return tableFieldWrapped(table, field) + " LIKE ?"
}

func EqualsField(table, field, table2, field2 string) string {
	return tableFieldWrapped(table, field) + " = " + tableFieldWrapped(table2, field2)
}

func IsNull(table, field string) string {
	return tableFieldWrapped(table, field) + " ISNULL"
}

func NotNull(table, field string) string {
	return tableFieldWrapped(table, field) + " NOTNULL"
}
