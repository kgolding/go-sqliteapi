package gdb

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
)

func (d *Database) PostTable(w http.ResponseWriter, r *http.Request) {
	table := path.Base(r.URL.Path)
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})
	err := dec.Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// user := auth.GetUser(r)
	var user User // BLANK USER

	id, err := d.insertMap(table, data, user)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write([]byte(strconv.Itoa(id)))
}

func (d *Database) PostSQL(w http.ResponseWriter, r *http.Request) {
	sql, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// @TODO Restrict/Sanitise SQL ?

	w.Header().Set("Content-Type", "application/json")
	err = d.QueryJsonWriter(w, string(sql), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
