package gdb

import (
	"errors"
	"net/http"
	"path"
	"strconv"
)

func (d *Database) DelRow(w http.ResponseWriter, r *http.Request) {
	table := path.Base(path.Dir(r.URL.Path))
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(path.Base(r.URL.Path))
	if err != nil {
		http.Error(w, "invalid row ID", http.StatusBadRequest)
		return
	}

	// user := auth.GetUser(r)
	var user User // BLANK USER

	err = d.delete(table, id, user)
	if err != nil {
		if errors.Is(err, ErrUnknownID) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
}
