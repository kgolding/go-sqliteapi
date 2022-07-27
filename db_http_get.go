package gdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
)

func (d *Database) GetRow(w http.ResponseWriter, r *http.Request) {
	bqc, err := BuildQueryConfigFromRequest(r, true)
	if err != nil {
		d.log.Printf("GetRow: bad request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	q, args, err := d.BuildQuery(bqc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d.log.Printf("GetRow: SQL: %s [%s]", q, args)

	err = d.queryJsonWriterRow(w, q, args)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

func (d *Database) GetRows(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery == "info" {
		d.GetRowsInfo(w, r)
		return
	}

	bqc, err := BuildQueryConfigFromRequest(r, false)
	if err != nil {
		d.log.Printf("GetRows: bad request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	q, args, err := d.BuildQuery(bqc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d.log.Printf("GetRows: SQL: %s [%s]", q, args)

	// Change this to use Content-Type
	switch strings.ToLower(r.URL.Query().Get("format")) {
	case "csv":
		if fname := r.URL.Query().Get("filename"); fname != "" {
			w.Header().Set("Content-Disposition", `attachment; filename="`+fname+`"`)
		}
		w.Header().Set("Content-Type", "text/csv")
		err = d.QueryCsvWriter(w, q, args)

	case "array":
		w.Header().Set("Content-Type", "application/json")
		err = d.QueryJsonArrayWriter(w, q, args)

	default:
		w.Header().Set("Content-Type", "application/json")
		err = d.QueryJsonWriter(w, q, args)
	}

	if err != nil {
		d.log.Printf("GetRows: error: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (d *Database) GetRowsInfo(w http.ResponseWriter, r *http.Request) {
	table := path.Base(r.URL.Path)
	tableFields, err := d.CheckTableNameGetFields(table)
	if err != nil {
		d.log.Printf("GetRows: error fetching table info: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get the actual database info from dbinfo, and then add the extra info from config
	ct := d.config.GetTable(table)
	ret := make([]TableFieldInfoWithMetaData, 0)
	for _, tf := range tableFields {
		x := TableFieldInfoWithMetaData{
			TableFieldInfo: tf,
		}
		if ct != nil {
			for _, ctf := range ct.Fields {
				if ctf.Name == tf.Name {
					x.Field = ctf
				}
			}

		}
		ret = append(ret, x)
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(ret)
	return
}
