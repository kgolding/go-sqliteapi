package sqliteapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

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

			for _, stmt := range cf.Statements {
				d.debugLog.Printf("SQL: %s\nArgs: %v\n", stmt, args)
				_, err = tx.Exec(stmt, args...)
				if err != nil {
					return
				}
			}
			return nil
		}
	}
	return fmt.Errorf("unknown function '%s'", function)
}
