package sqliteapi

import (
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// SelectBuilderFromRequest returns a populated SelectBuilder from a give http request
func (d *Database) SelectBuilderFromRequest(r *http.Request, withKey bool) (*SelectBuilder, []interface{}, error) {

	GetQueryUint := func(param string, defValue uint) (uint, error) {
		if v := r.URL.Query().Get(param); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return defValue, err
			}
			if n < 0 {
				return defValue, fmt.Errorf("not a positive number '%s'", v)
			}
			return uint(n), nil
		}
		return defValue, nil
	}

	var err error

	sb := &SelectBuilder{}
	args := make([]interface{}, 0)

	sb.From = path.Base(r.URL.Path)
	if withKey {
		pkValue := sb.From
		sb.From = path.Base(path.Dir(r.URL.Path))
		ct := d.config.GetTable(sb.From)
		if ct != nil {
			sb.Where = append(sb.Where, EqualsArg(sb.From, ct.PrimaryKey()))
			args = append(args, pkValue)
		}
	}

	tableInfo := d.dbInfo.GetTableInfo(sb.From)

	if s := r.URL.Query().Get("select"); s != "" && s != "*" {
		sb.SetSelectFieldsWithTable(strings.Split(s, ","))
	}

	if s := r.URL.Query().Get("search"); s != "" {
		fields := sb.Select
		if len(fields) == 0 {
			for _, f := range tableInfo.Fields {
				fields = append(fields, tableFieldWrapped(tableInfo.Name, f.Name))
			}
		}

		conditions := make([]string, 0)
		for _, f := range fields {
			conditions = append(conditions, f+" LIKE ?")
			args = append(args, s)
		}
		sb.Where = append(sb.Where, "("+strings.Join(conditions, " OR ")+")")
	}

	if s := r.URL.Query().Get("where"); s != "" {
		sb.Where = append(sb.Where, s)
	}

	if s := r.URL.Query().Get("sort"); s != "" {
		for _, e := range strings.Split(s, ",") {
			m := regOrderBy.FindStringSubmatch(e)
			if len(m) == 4 {
				ob := OrderBy{
					Field:     m[2],
					Ascending: strings.ToLower(m[3]) == "asc",
				}
				sb.OrderBy = append(sb.OrderBy, ob)
			}
		}
	}

	sb.Limit, err = GetQueryUint("limit", 1000)
	if err != nil {
		return nil, nil, err
	}

	sb.Offset, err = GetQueryUint("offset", 0)
	if err != nil {
		return nil, nil, err
	}

	return sb, args, nil
}

// https://regex101.com/r/9n82vv/1
var regOrderBy = regexp.MustCompile(`(?i)(-?)(\w+) *(?:(asc|desc|))`)
