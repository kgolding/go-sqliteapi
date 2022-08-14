package sqliteapi

import (
	"encoding/json"
	"net/http"
	"path"
)

func (d *Database) HandlePostFunction(w http.ResponseWriter, r *http.Request) {
	function := path.Base(r.URL.Path)

	dec := json.NewDecoder(r.Body)
	data := make(map[string]interface{})
	err := dec.Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// user := auth.GetUser(r)

	err = d.CallFunction(function, data, nil)
	if err != nil {
		d.log.Printf("%s: Error calling function: %v", function, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
