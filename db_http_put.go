package gdb

import (
	"encoding/json"
	"net/http"
	"path"
	"strconv"
)

func (d *Database) PutRow(w http.ResponseWriter, r *http.Request) {
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

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})
	err = dec.Decode(&data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	// @TODO Should I check the data id?

	data["id"] = id

	// user := auth.GetUser(r)
	var user User // BLANK USER

	err = d.updateMap(table, data, user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
