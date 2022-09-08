package sqliteapi

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"gopkg.in/yaml.v2"
)

const ConfigCreateSql = `
CREATE TABLE IF NOT EXISTS "gdb_config" (
	"id"			INTEGER	PRIMARY KEY AUTOINCREMENT,
	"createdAt"	DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	"config"		TEXT NOT NULL,
	"hash"		TEXT NOT NULL
);`

type GdbConfigRow struct {
	ID        int       `db:"id"`
	CreatedAt time.Time `db:"createdAt"`
	Config    []byte    `db:"config"`
	Hash      []byte    `sb:"hash`
}

type ConfigOptions struct {
	RetainUnmanaged bool
	DryRun          bool
}

// ApplyConfig applies the config to the database - note that specialfields are not automatically expanded
func (d *Database) ApplyConfig(c *Config, opts *ConfigOptions) (err error) {
	debugf := func(format string, args ...interface{}) {
		d.debugLog.Printf("ApplyConfig: "+format, args...)
	}

	sqlLog := []string{}
	slog := func(s string) string {
		sqlLog = append(sqlLog, s)
		return s
	}

	// debugf("START")
	err = d.Refresh()
	if err != nil {
		err = fmt.Errorf("error reading curent database state: %w", err)
		return
	}

	// Make sure the config table exists
	_, err = d.DB.Exec(ConfigCreateSql)
	if err != nil {
		err = fmt.Errorf("error creating config table: %w", err)
		return
	}

	if opts == nil {
		opts = &ConfigOptions{
			RetainUnmanaged: true,
			DryRun:          false,
		}
	}

	debugf("dbInfo:\n%+v\n", d.dbInfo)

	// Store all associated triggers/views in case we need to recreate them
	var rows *sqlx.Rows
	rows, err = d.DB.Queryx("SELECT type, name, sql FROM sqlite_master WHERE type!='table' AND sql IS NOT NULL")
	if err != nil {
		err = fmt.Errorf("get existing triggers/views: %w", err)
		return
	}
	if rows.Err() != nil {
		err = fmt.Errorf("get existing triggers/views: %w", rows.Err())
		return
	}
	associatedSchemes := make([]*AssociatedScheme, 0)
	for rows.Next() {
		if rows.Err() != nil {
			err = fmt.Errorf("get existing triggers/views: %w", rows.Err())
			return
		}
		var x AssociatedScheme
		err = rows.StructScan(&x)
		if err != nil {
			err = fmt.Errorf("get existing triggers/views: %w", rows.Err())
			return
		}
		associatedSchemes = append(associatedSchemes, &x)
	}
	rows.Close()

	// Disabling foreign keys must be done outside the transaction
	s := "PRAGMA foreign_keys=OFF"
	_, err = d.DB.Exec(slog(s))
	if err != nil {
		err = fmt.Errorf("foreign_keys = OFF: %w", err)
		return
	}
	// debugf("APPLY: EXEC SQL:\n%s", s)

	defer func() {
		s := "PRAGMA foreign_keys=ON"
		// debugf("APPLY: EXEC SQL:\n%s", s)
		d.DB.Exec(slog(s))
		d.debugLog.Printf("ApplyConfig, SQL log:\n" + strings.Join(sqlLog, "\n"))
		d.Refresh()
	}()

	tx, err := d.DB.Beginx()
	if err != nil {
		return
	}

	changes := make([]string, 0)

	defer func() {
		if err != nil || opts.DryRun {
			debugf("FINISHED: err: %v", err)
			tx.Rollback()
			return
		}
		err = tx.Commit()
		debugf("FINISHED: err: %v", err)
	}()

	// Remove tables
	if !opts.RetainUnmanaged {
		for _, oldTable := range d.dbInfo {
			if t := c.GetTable(oldTable.Name); t == nil {
				// debugf("APPLY: dropping table %s\n", oldTable.Name)
				s := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", oldTable.Name)
				// debugf("APPLY: EXEC SQL:\n%s", s)
				_, err = tx.Exec(slog(s))
				if err != nil {
					err = fmt.Errorf("error dropping table '%s': %w", oldTable.Name, err)
					return
				}
				changes = append(changes, "removed unmanaged table "+oldTable.Name)
				delete(d.dbInfo, oldTable.Name)
			}
		}
	}

	deletedSchemas := false

	// Add/modify tables
	for _, table := range c.Tables {
		ot := d.dbInfo.GetTableInfo(table.Name)
		s, err = table.CreateSQL()
		if err != nil {
			err = fmt.Errorf("error creating table '%s': %w", table.Name, err)
			return
		}
		if ot == nil { // CREATE TABLE
			// debugf("create new table '%s'\n", table.Name)
			// debugf("APPLY: EXEC SQL:\n%s", s)
			_, err = tx.Exec(slog(s))
			if err != nil {
				err = fmt.Errorf("error creating table '%s': %w\n%s", table.Name, err, s)
				return
			}
			changes = append(changes, "created new table "+table.Name)
		} else { // Check for a difference in the create SQL
			if strings.TrimSpace(s) != strings.TrimSpace(ot.SQL) { // MODIFY TABLE
				debugf("APPLY: modify table %s: Big change\nA: %s\nB: %s", table.Name, s, ot.SQL)
				// See item 7 at https://www.sqlite.org/lang_altertable.html
				if !deletedSchemas {
					deletedSchemas = true
					for _, sch := range associatedSchemes {
						s := fmt.Sprintf("DROP %s %s", sch.Type, sch.Name)
						// debugf("APPLY: EXEC SQL:\n%s", s)
						_, err = tx.Exec(slog(s))
						if err != nil {
							err = fmt.Errorf("error dropping %s %s: %w\n%s", sch.Type, sch.Name, err, s)
							return
						}
					}
				}
				// @TODO use a temporary table?
				// 1. Create new table with tmp name
				tmpName := table.Name + "_tmp"
				tableName := table.Name
				// debugf("create temp table %s\n", tmpName)

				// Create temp table SQL
				table.Name = tmpName // Temp change name
				s, err = table.CreateSQL()
				table.Name = tableName // Change name back
				if err != nil {
					err = fmt.Errorf("error creating temporary table SQL '%s': %w\n%s", tmpName, err, s)
					return
				}
				// debugf("APPLY: EXEC SQL:\n%s", s)
				_, err = tx.Exec(slog(s))
				if err != nil {
					err = fmt.Errorf("error creating temporary table '%s': %w\n%s", tmpName, err, s)
					return
				}

				// 2. Copy data
				commonFields := []string{}
				for _, f := range ot.Fields {
					for _, f2 := range table.Fields {
						if f.Name == f2.Name {
							commonFields = append(commonFields, f.Name)
						}
					}
				}
				if len(commonFields) > 0 {
					fcvs := "`" + strings.Join(commonFields, "`,`") + "`"
					s = fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s`",
						tmpName, fcvs, fcvs, ot.Name)
					// debugf("APPLY: EXEC SQL:\n%s", s)
					_, err = tx.Exec(slog(s))
					if err != nil {
						err = fmt.Errorf("error copying data from '%s' to '%s': %w\n%s", ot.Name, tmpName, err, s)
						return
					}
				}

				// 3. Drop old table
				s = fmt.Sprintf("DROP TABLE `%s`", ot.Name)
				// debugf("APPLY: EXEC SQL:\n%s", s)
				_, err = tx.Exec(slog(s))
				if err != nil {
					err = fmt.Errorf("error dropping old table '%s': %w\n%s", ot.Name, err, s)
					return
				}

				// 4. Rename tmp
				s = fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", tmpName, tableName)
				// debugf("APPLY: EXEC SQL:\n%s", s)
				_, err = tx.Exec(slog(s))
				if err != nil {
					err = fmt.Errorf("error renaming table '%s' to '%s': %w\n%s", tmpName, tableName, err, s)
					return
				}

				changes = append(changes, "updated table "+tableName)
			}
		}

		// ===================== Handle INDEXED fields =========================
		indexes := make(map[string]string) // Create a list of indexes and their create statements
		for _, f := range table.Fields {
			if f.Indexed {
				name := fmt.Sprintf("gdb_idx_%s_%s", table.Name, f.Name)
				indexes[name] = fmt.Sprintf(`CREATE INDEX "%s" ON "%s" ("%s" ASC)`,
					name, table.Name, f.Name,
				)
			}
		}

		// Get current indexes for this table
		var rows *sql.Rows
		rows, err = tx.Query("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?", table.Name)
		if err != nil {
			err = fmt.Errorf("error reading existing indexes: %w", err)
			return
		}
		for rows.Next() { // Remove from indexes any that exist
			var name sql.NullString
			var q sql.NullString
			err = rows.Scan(&name, &q)
			if err != nil {
				err = fmt.Errorf("error reading existing index row: %w", err)
				return
			}
			if name.Valid {
				if _, ok := indexes[name.String]; ok {
					// if strings.TrimSpace(q) == SQL {
					delete(indexes, name.String) // Index exists
					// }
				}
			}
		}
		rows.Close()
		for name, q := range indexes { // Create new indexes
			changes = append(changes, "create index "+name)
			_, err = tx.Exec(slog(q))
			if err != nil {
				err = fmt.Errorf("error creating index: %w\n%s", err, q)
				return
			}
		}
	}

	if deletedSchemas {
		for _, sch := range associatedSchemes {
			// tx.Exec(slog("DROP " + sch.Type + " " + sch.Name))
			// debugf("APPLY: EXEC SQL:\n%s", sch.SQL)
			changes = append(changes, "dropped trigger "+sch.Name)
			_, err = tx.Exec(slog(sch.SQL))
			if err != nil {
				return
			}
		}
	}

	// Drop old triggers not in use
	for _, sch := range associatedSchemes {
		if sch.Type == "trigger" {
			del := true
			for _, trigger := range c.Triggers {
				if trigger.Name == sch.Name {
					del = false
					break
				}
			}
			if del {
				s := "DROP TRIGGER `" + sch.Name + "`"
				_, err = tx.Exec(slog(s))
				if err != nil {
					return
				}
				// debugf("APPLY: EXEC SQL:\n%s", s)
			}
		}
	}

	// Triggers
	if len(c.Triggers) > 0 {
	triggerLoop:
		for _, trigger := range c.Triggers {
			// Find existing and compare if same skip drop/create
			dropped := false
			for _, sch := range associatedSchemes {
				if sch.Name == trigger.Name {
					t1 := strings.TrimSuffix(strings.TrimSpace(sch.SQL), ";")
					t2 := strings.TrimSuffix(strings.TrimSpace(trigger.CreateSQL()), ";")
					// fmt.Println("@@@@@@@@@@@@@@@@@\n", t1, "----------------\n", t2)
					if t1 == t2 {
						continue triggerLoop // Nothing to do
					}
					debugf("DROPPING existing trigger: %s", trigger.Name)
					_, err = tx.Exec(slog("DROP TRIGGER `" + trigger.Name + "`"))
					if err != nil {
						return err
					}
					dropped = true
					// delete(ts, trigger.Name)
					changes = append(changes, "update trigger "+trigger.Name)
				}
			}
			if !dropped {
				changes = append(changes, "create new trigger "+trigger.Name)
			}
			s := trigger.CreateSQL()
			// debugf("APPLY: EXEC SQL:\n%s", s)
			_, err = tx.Exec(slog(s))
			if err != nil {
				return
			}
		}
	}

	// Views
	if len(c.Views) > 0 {
		for _, view := range c.Views {
			// Find existing and compare if same skip drop/create
			dropped := false
			for _, sch := range associatedSchemes {
				if sch.Type == "view" && sch.Name == view.Name {
					t1 := strings.TrimSuffix(strings.TrimSpace(sch.SQL), ";")
					t2 := strings.TrimSuffix(strings.TrimSpace(view.Statement), ";")
					// fmt.Println("@@@@@@@@@@@@@@@@@\n", t1, "----------------\n", t2)
					if t1 != t2 {
						debugf("DROPPING existing view: %s", view.Name)
						_, err = tx.Exec(slog("DROP VIEW `" + view.Name + "`"))
						if err != nil {
							return err
						}
						dropped = true
						changes = append(changes, "update view "+view.Name)
					}
				}
			}
			if !dropped {
				changes = append(changes, "create new view "+view.Name)
			}
			_, err = tx.Exec(slog("CREATE VIEW '" + view.Name + "' AS " + view.Statement))
			if err != nil {
				return
			}
		}
	}

	b, err := yaml.Marshal(c)
	if err != nil {
		return
	}

	if len(changes) == 0 {
		// debugf(" no changes :)")
		err = nil
		// Compare with previous config
		var oldYaml []byte
		err = tx.Get(&oldYaml, "SELECT config FROM gdb_config ORDER BY id DESC LIMIT 1")
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				debugf("No schema changes, no existing configs")
			} else {
				debugf("error getting old config: %s", err)
			}
		} else if bytes.Compare(b, oldYaml) == 0 {
			debugf("No schema changes, old config matches new config")
			// No need to save the config
			d.config = c
			return
		}
	}

	h := sha256.New()
	h.Write(b)

	var res sql.Result
	s = "INSERT INTO gdb_config (config, hash) VALUES (?, ?)"
	// debugf("APPLY: EXEC SQL:\n%s\n", s)
	res, err = tx.Exec(s, b, h.Sum(nil))
	if err != nil {
		debugf("error storing config: %s", err)
		return
	}

	version, _ := res.LastInsertId()
	s = fmt.Sprintf("PRAGMA user_version=%d", version)
	// debugf("APPLY: EXEC SQL:\n%s", s)
	tx.Exec(slog(s))

	if len(changes) == 0 {
		d.log.Printf("Database version %d (no changes)", version)
	} else {
		d.log.Printf("Database version is now %d:\n\t - "+strings.Join(changes, "\n\t - "), version)
	}

	d.config = c

	return
}

type AssociatedScheme struct {
	Type string `db:"type"`
	Name string `db:"name"`
	SQL  string `db:"sql"`
}
