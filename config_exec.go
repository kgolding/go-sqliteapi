package gdb

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	err = d.Refresh()
	if err != nil {
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

	debugf := func(format string, args ...interface{}) {
		d.debugLog.Printf("ApplyConfig: "+format, args...)
	}

	debugf("dbInfo: %+v\n", d.dbInfo)

	s := "PRAGMA foreign_keys=OFF"
	_, err = d.DB.Exec(s)
	if err != nil {
		err = fmt.Errorf("foreign_keys = OFF: %w", err)
		return
	}
	debugf("APPLY: EXEC SQL: '%s'", s)

	defer func() {
		s := "PRAGMA foreign_keys = ON"
		_, err = d.DB.Exec(s)
		if err != nil {
			return
		}
		debugf("APPLY: EXEC SQL: '%s'", s)
	}()

	tx, err := d.DB.Beginx()
	if err != nil {
		return
	}

	noChanges := true

	defer func() {
		if err != nil || opts.DryRun {
			tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			d.Refresh()
		}
	}()

	// Remove tables
	if !opts.RetainUnmanaged {
		for _, oldTable := range d.dbInfo {
			if t := c.GetTable(oldTable.Name); t == nil {
				debugf("APPLY: dropping table %s\n", oldTable.Name)
				s := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", oldTable.Name)
				debugf("APPLY: EXEC SQL: '%s'", s)
				_, err = tx.Exec(s)
				if err != nil {
					err = fmt.Errorf("error dropping table '%s': %w", oldTable.Name, err)
					return
				}
				noChanges = false
				delete(d.dbInfo, oldTable.Name)
			}
		}
	}

	// Add/modify tables
	for _, table := range c.Tables {
		ot := d.dbInfo.GetTableInfo(table.Name)
		if ot == nil { // CREATE TABLE
			debugf("APPLY: create table %s\n", table.Name)
			s, err = table.CreateSQL()
			if err != nil {
				return
			}
			debugf("APPLY: EXEC SQL: '%s'", s)
			_, err = tx.Exec(s)
			if err != nil {
				err = fmt.Errorf("error creating table '%s': %w\n%s", table.Name, err, s)
				return
			}
			noChanges = false

		} else { // MODIFY TABLE
			change := false
			// Check no change, same number of fields & all fields the same
			if len(ot.Fields) == len(table.Fields) {
				for i, of := range ot.Fields {
					nf := table.Fields[i]
					err := nf.CompareDbFields(&of)
					if err != nil {
						debugf("%s.%s: change trigger by: %s", table.Name, nf.Name, err)
						change = true
						break
					}
				}
			} else {
				change = true
			}

			if change {
				debugf("APPLY: modify table %s\n", table.Name)
				// Can we get away with just adding new fields?
				// debugf("Old fields %d, New fields %d\n", len(ot.Fields), len(table.Fields))
				bigChange := true
				if len(ot.Fields) < len(table.Fields) {
					justAdd := true
					for i, of := range ot.Fields {
						// debugf("Compare '%s':\na: %+v\nb: %+v\n", table.Name, of, table.Fields[i])
						nf := table.Fields[i]
						err := nf.CompareDbFields(&of)
						if err != nil {
							debugf("APPLY: MODIFY table %s: Big change because of field '%s': %s\n", table.Name, of.Name, err)
							// debugf("nf: %+v\nof: %+v\n", nf, of)
							justAdd = false
							break
						}
					}

					if justAdd {
						var coldef string
						debugf("APPLY: modify table %s: Just adding coldefs\n", table.Name)
						for _, f := range table.Fields[len(ot.Fields):] {
							coldef, err = f.ColDef()
							if err != nil {
								tx.Rollback()
								return fmt.Errorf("bad field definition '%s' to '%s': %w", f.Name, table.Name, err)
							}
							s = "ALTER TABLE `" + table.Name + "` ADD COLUMN " + coldef
							// fmt.Println(sql)
							_, err = tx.Exec(s)
							if err != nil {
								err = fmt.Errorf("error adding column '%s' to '%s': %w", f.Name, table.Name, err)
								return
							}
							noChanges = false
						}
						bigChange = false
					}
				}

				if bigChange {
					debugf("APPLY: modify table %s: Big change\n", table.Name)
					// See item 7 at https://www.sqlite.org/lang_altertable.html

					// 1. Create new table with tmp name
					tmpName := table.Name + "_tmp"
					tableName := table.Name
					debugf("APPLY: create table %s\n", tmpName)
					table.Name = tmpName // Temp change name
					s, err = table.CreateSQL()
					table.Name = tableName // Change name back
					if err != nil {
						return
					}
					debugf("APPLY: EXEC SQL: '%s'", s)
					_, err = tx.Exec(s)
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
						debugf("APPLY: EXEC SQL: '%s'", s)
						_, err = tx.Exec(s)
						if err != nil {
							err = fmt.Errorf("error copying data from '%s' to '%s': %w\n%s", ot.Name, tmpName, err, s)
							return
						}
					}

					// 3. Drop old table
					s = fmt.Sprintf("DROP TABLE `%s`", ot.Name)
					debugf("APPLY: EXEC SQL: '%s'", s)
					_, err = tx.Exec(s)
					if err != nil {
						err = fmt.Errorf("error dropping old table '%s': %w\n%s", ot.Name, err, s)
						return
					}

					// 4. Rename tmp
					s = fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", tmpName, tableName)
					debugf("APPLY: EXEC SQL: '%s'", s)
					_, err = tx.Exec(s)
					if err != nil {
						err = fmt.Errorf("error renaming table '%s' to '%s': %w\n%s", tmpName, tableName, err, s)
						return
					}
					noChanges = false
				}
			}
		}
	}

	b, err := yaml.Marshal(c)
	if err != nil {
		return
	}

	if noChanges {
		debugf("No schema changes :)")
		err = nil
		// Compare with previous config
		var oldYaml []byte
		err = d.DB.Get(&oldYaml, "SELECT config FROM gdb_config ORDER BY id DESC LIMIT 1")
		if err != sql.ErrNoRows {
			// No existing config
		} else if err != nil {
			return
		} else if bytes.Compare(b, oldYaml) == 0 {
			// No need to save the config
			return
		}
	}

	h := sha256.New()
	h.Write(b)

	var res sql.Result
	res, err = tx.Exec("INSERT INTO gdb_config (config, hash) VALUES (?, ?)", b, h.Sum(nil))
	if err != nil {
		return
	}

	version, _ := res.LastInsertId()
	tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", version))

	if noChanges {
		d.log.Printf("Database version is now %d (no schema changes)", version)
	} else {
		d.log.Printf("Database version is now %d (schema has changed)", version)
	}

	d.config = c

	return
}
