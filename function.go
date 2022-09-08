package sqliteapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
)

var regdollarParam = regexp.MustCompile(`\$[a-zA-Z]\w*`)

func (d *Database) CallFunction(function string, data map[string]interface{}, user User) (err error) {
	if d.config == nil {
		return errors.New("missing database config")
	}

	for _, cf := range d.config.Functions {
		if cf.Name == function {
			args := make([]interface{}, 0)
			for _, p := range cf.Params {
				if x, ok := data[p.Name]; ok {
					args = append(args, x)
				} else {
					args = append(args, nil)
				}
			}

			ctx, _ := context.WithTimeout(context.Background(), d.timeout)
			var tx *sqlx.Tx
			tx, err = d.DB.BeginTxx(ctx, nil)
			if err != nil {
				return
			}

			hparams := HookParams{
				Action: HookBeforeFunction,
				Table:  function,
				Data:   data,
				Tx:     tx,
				User:   user,
			}
			d.runHooks(function, hparams)

			defer func() {
				if err != nil {
					tx.Rollback()
				} else {
					err = tx.Commit()
					if err == nil {
						hparams := HookParams{
							Action: HookAfterFunction,
							Table:  function,
							Data:   data,
							User:   user,
						}
						d.runHooks(function, hparams)
					}
				}
			}()

			logs := []string{}
			for _, stmt := range cf.Statements {
				pargs := make([]interface{}, 0)
				m := regdollarParam.FindAllStringSubmatch(stmt, -1)
				if len(m) > 0 {
					// fmt.Printf("m: %#v\n", m)
					for _, matches := range m {
						if len(matches) > 0 {
							p := matches[0]
							// fmt.Printf("p: %s\n", p)
							paramExists := false
							for _, param := range cf.Params {
								if p == "$"+param.Name {
									paramExists = true
									break
								}
							}
							if !paramExists {
								return fmt.Errorf("unknown parameter in statement '%s'", p)
							}

							stmt = strings.ReplaceAll(stmt, p, "?")
							v, _ := data[p[1:]]
							pargs = append(pargs, v)
						}
					}
				} else {
					pargs = args
				}

				var res sql.Result
				res, err = tx.Exec(stmt, pargs...)
				if err != nil {
					err = fmt.Errorf("%s: %s: %w", function, stmt, err)
					return
				}
				effected, _ := res.RowsAffected()
				logs = append(logs, fmt.Sprintf("SQL: %s\n\tArgs: %v\n\tRows effected: %d", stmt, args, effected))
			}
			d.debugLog.Printf("CallFunction(%s):\n - %s", function, strings.Join(logs, "\n - "))

			return nil
		}
	}
	return fmt.Errorf("unknown function '%s'", function)
}
