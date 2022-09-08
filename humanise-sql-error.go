package sqliteapi

import (
	"strings"
)

func (d *Database) humaniseSqlError(err error) string {
	if err == nil || d.config == nil {
		return ""
	}

	splt := strings.SplitN(err.Error(), ":", 2)
	if len(splt) == 2 {
		label := strings.TrimSpace(splt[1]) // Default
		tf := strings.SplitN(label, ".", 2)
		if len(tf) == 2 {
			table := d.config.GetTable(tf[0])
			if table != nil {
				for _, f := range table.Fields {
					if f.Name == tf[1] {
						if f.Label != "" {
							label = f.Label
						}
						break
					}
				}
			}
		}

		switch strings.TrimSpace(splt[0]) {
		case "UNIQUE constraint failed":
			return label + " is already used and this field must be unique"
		case "NOT NULL constraint failed":
			return label + " can not be empty"
		}
	}

	return err.Error()
}

/*
	Example errors

	UNIQUE constraint failed: thing.tag
	NOT NULL constraint failed: thing.name
*/
