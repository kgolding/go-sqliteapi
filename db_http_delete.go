package gdb

import (
	"errors"
	"net/http"
	"path"
)

func (d *Database) DelRow(w http.ResponseWriter, r *http.Request) {
	table := path.Base(path.Dir(r.URL.Path))
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	key := path.Base(r.URL.Path)

	// user := auth.GetUser(r)
	var user User // BLANK USER

	err := d.delete(table, key, user)
	if err != nil {
		if errors.Is(err, ErrUnknownKey) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
}
