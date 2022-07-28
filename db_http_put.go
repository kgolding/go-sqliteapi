package gdb

import (
	"encoding/json"
	"net/http"
	"path"
)

func (d *Database) PutRow(w http.ResponseWriter, r *http.Request) {
	table := path.Base(path.Dir(r.URL.Path))
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	tableInfo := d.dbInfo.GetTableInfo(table)
	if tableInfo == nil {
		http.Error(w, "unknown table/view", http.StatusBadRequest)
		return
	}

	key := path.Base(r.URL.Path)

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})
	err := dec.Decode(&data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	// @TODO Should I check the data id?

	data[tableInfo.GetPrimaryKey().Field] = key

	// user := auth.GetUser(r)
	var user User // BLANK USER

	err = d.updateMap(table, data, user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
