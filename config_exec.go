package gdb

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const ConfigCreateSql = `CREATE TABLE IF NOT EXISTS "gdb_config" (
	"id"			INTEGER	PRIMARY KEY AUTOINCREMENT,
	"createdAt"	DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	"config"	TEXT NOT NULL);`

type GdbConfigRow struct {
	ID        int       `db:"id"`
	CreatedAt time.Time `db:"createdAt"`
	Config    []byte    `db:"config"`
}

func debugf(format string, args ...interface{}) {
	// fmt.Printf(format, args...)
}

func (c *Config) Apply(d *Database) error {
	debugf("================ APPLY START ===================\n%+v\n", c)
	defer debugf("================ APPLY END ===================\n")

	err := d.Refresh()
	if err != nil {
		return err
	}

	debugf("dbInfo: %+v\n", d.dbInfo)

	_, err = d.DB.Exec("PRAGMA foreign_keys=OFF")
	if err != nil {
		return fmt.Errorf("foreign_keys=OFF: %w", err)
	}

	defer d.DB.Exec("PRAGMA foreign_keys=ON")

	tx, err := d.DB.Beginx()
	if err != nil {
		return err
	}

	// Make sure the config table exists
	_, err = tx.Exec(ConfigCreateSql)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error creating config table: %w", err)
	}

	// Remove tables
	for _, oldTable := range d.dbInfo {
		if t := c.GetTable(oldTable.Name); t == nil {
			debugf("APPLY: DROP table %s\n", oldTable.Name)
			_, err = tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", oldTable.Name))
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error dropping table '%s': %w", oldTable.Name, err)
			}
			delete(d.dbInfo, oldTable.Name)
		}
	}

	// Add/modify tables
	for _, table := range c.Tables {
		ot := d.dbInfo.GetTableInfo(table.Name)

		if ot == nil { // CREATE TABLE
			debugf("APPLY: CREATE table %s\n", table.Name)
			sql, err := table.CreateSQL()
			if err != nil {
				tx.Rollback()
				return err
			}
			debugf("APPLY: %s", sql)
			_, err = tx.Exec(sql)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error creating table '%s': %w\n%s", table.Name, err, sql)
			}

		} else { // MODIFY TABLE
			change := false
			// Check no change
			if len(ot.Fields) == len(table.Fields) {
				for i, of := range ot.Fields {
					nf := table.Fields[i].applySpecialFields()
					if nf.Name != of.Name ||
						!strings.EqualFold(nf.Type, of.Type) ||
						nf.Default != of.DefaultValue ||
						nf.NotNull != of.NotNull ||
						nf.PrimaryKey != of.PrimaryKey {
						change = true
						break
					}
				}
			} else {
				change = true
			}

			if change {
				debugf("APPLY: MODIFY table %s\n", table.Name)
				// Can we get away with just adding new fields?
				debugf("Old fields %d, New fields %d\n", len(ot.Fields), len(table.Fields))
				bigChange := true
				if len(ot.Fields) < len(table.Fields) {
					justAdd := true
					for i, of := range ot.Fields {
						debugf("Compare '%s':\na: %+v\nb: %+v\n", table.Name, of, table.Fields[i])
						nf := table.Fields[i].applySpecialFields()
						if nf.Name != of.Name ||
							!strings.EqualFold(nf.Type, of.Type) ||
							nf.Default != of.DefaultValue ||
							nf.NotNull != of.NotNull ||
							nf.PrimaryKey != of.PrimaryKey {
							debugf("APPLY: MODIFY table %s: Big change because of field '%s'\n", table.Name, of.Name)
							debugf("nf: %+v\nof: %+v\n", nf, of)
							justAdd = false
							break
						}
					}

					if justAdd {
						debugf("APPLY: MODIFY table %s: Just add\n", table.Name)
						for _, f := range table.Fields[len(ot.Fields):] {
							coldef, err := f.ColDef()
							if err != nil {
								tx.Rollback()
								return fmt.Errorf("bad field definition '%s' to '%s': %w", f.Name, table.Name, err)
							}
							sql := "ALTER TABLE `" + table.Name + "` ADD COLUMN " + coldef
							// fmt.Println(sql)
							_, err = tx.Exec(sql)
							if err != nil {
								tx.Rollback()
								return fmt.Errorf("error adding column '%s' to '%s': %w", f.Name, table.Name, err)
							}
						}
						bigChange = false
					}
				}

				if bigChange {
					debugf("APPLY: MODIFY table %s: Big change\n", table.Name)
					// See item 7 at https://www.sqlite.org/lang_altertable.html

					// 1. Create new table with tmp name
					tmpName := table.Name + "_tmp"
					tableName := table.Name
					debugf("APPLY: CREATE table %s\n", tmpName)
					table.Name = tmpName // Temp change name
					sql, err := table.CreateSQL()
					table.Name = tableName // Change name back
					if err != nil {
						tx.Rollback()
						return err
					}
					debugf("APPLY: %s", sql)
					_, err = tx.Exec(sql)
					if err != nil {
						tx.Rollback()
						return fmt.Errorf("error creating temporary table '%s': %w\n%s", tmpName, err, sql)
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
						sql = fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s`",
							tmpName, fcvs, fcvs, ot.Name)
						debugf("APPLY: %s", sql)
						_, err = tx.Exec(sql)
						if err != nil {
							tx.Rollback()
							return fmt.Errorf("error copying data from '%s' to '%s': %w\n%s", ot.Name, tmpName, err, sql)
						}
					}

					// 3. Drop old table
					sql = fmt.Sprintf("DROP TABLE `%s`", ot.Name)
					debugf("APPLY: %s", sql)
					_, err = tx.Exec(sql)
					if err != nil {
						tx.Rollback()
						return fmt.Errorf("error dropping old table '%s': %w\n%s", ot.Name, err, sql)
					}

					// 4. Rename tmp
					sql = fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", tmpName, tableName)
					debugf("APPLY: %s", sql)
					_, err = tx.Exec(sql)
					if err != nil {
						tx.Rollback()
						return fmt.Errorf("error renaming table '%s' to '%s': %w\n%s", tmpName, tableName, err, sql)
					}
				}
			}
		}
	}

	b, err := yaml.Marshal(c)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("INSERT INTO gdb_config (config) VALUES (?)", b)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	return err
}
