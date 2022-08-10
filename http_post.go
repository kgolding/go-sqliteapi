package sqliteapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/jmoiron/sqlx"
)

func (d *Database) HandlePostTable(w http.ResponseWriter, r *http.Request) {
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

	id, err := d.InsertMap(table, data, user)
	if err != nil {
		d.log.Printf("%s: Error creating row: %v", table, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write([]byte(fmt.Sprintf("%d", id)))

	d.log.Printf("%s: Created row %d", table, id)
}

type PostSQLStruct struct {
	SQL  string        `json:"sql"`
	Args []interface{} `json:"args"`
}

func (d *Database) HandlePostSQL(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := PostSQLStruct{
		SQL: string(body),
	}

	if bytes.HasPrefix(body, []byte("{")) { // We've got JSON
		err := json.Unmarshal(body, &data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Handle array args as used by IN ?
		data.SQL, data.Args, err = sqlx.In(data.SQL, data.Args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// @TODO Restrict/Sanitise SQL more ?

	d.debugLog.Printf("PostSQL: SQL:\n%s\nArgs (%d): %s", data.SQL, len(data.Args), data.Args)

	w.Header().Set("Content-Type", "application/json")
	err = d.QueryJsonWriter(w, data.SQL, data.Args)
	if err != nil {
		d.log.Printf("Error executing SQL: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
