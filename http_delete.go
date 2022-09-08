package sqliteapi

import (
	"errors"
	"net/http"
	"path"
)

func (d *Database) HandleDelRow(w http.ResponseWriter, r *http.Request) {
	table := path.Base(path.Dir(r.URL.Path))
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	key := path.Base(r.URL.Path)

	// user := auth.GetUser(r)

	err := d.Delete(table, key, nil)
	if err != nil {
		if errors.Is(err, ErrUnknownKey) {
			http.Error(w, d.humaniseSqlError(err), http.StatusNotFound)
			return
		}
		http.Error(w, d.humaniseSqlError(err), http.StatusBadRequest)
	}
}
