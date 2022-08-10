package gdb

import (
	"encoding/json"
	"net/http"
	"path"
	"sort"
	"strings"
)

func (d *Database) GetTableNames(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ret := []string{}
	for _, info := range d.dbInfo {
		ret = append(ret, info.Name)
	}
	sort.Strings(ret)
	enc := json.NewEncoder(w)
	enc.Encode(ret)
}

func (d *Database) GetRow(w http.ResponseWriter, r *http.Request) {
	sb, args, err := d.SelectBuilderFromRequest(r, true)
	if err != nil {
		d.log.Printf("GetRow: bad request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d.debugLog.Printf("GetRow: sb: %#v\nArgs: %s\n", sb, args)

	w.Header().Set("Content-Type", "application/json")
	err = d.queryJsonWriterRow(w, sb, args)
	if err != nil {
		d.log.Printf("GetRow: Error: %s", err)
		w.Write([]byte(err.Error()))
	}
}

func (d *Database) GetRows(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery == "info" {
		d.GetRowsInfo(w, r)
		return
	}

	sb, args, err := d.SelectBuilderFromRequest(r, false)
	if err != nil {
		d.log.Printf("GetRows: bad request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d.debugLog.Printf("GetRows: sb: %#v\nArgs: %s\n", sb, args)

	d.AddRefLabels(sb, "")

	q, err := sb.ToSql()

	// bqc, err := BuildQueryConfigFromRequest(r, false)
	if err != nil {
		d.log.Printf("GetRows: bad request: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d.debugLog.Printf("GetRows: SQL:\n%s\nArgs: %s", q, args)

	// @TODO Maybe change this to use Content-Type ?
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

type TableFieldInfoWithMetaData struct {
	TableFieldInfo
	ConfigField
}

func (d *Database) GetRowsInfo(w http.ResponseWriter, r *http.Request) {
	table := path.Base(r.URL.Path)
	tableInfo, ok := d.dbInfo[table]
	if !ok {
		http.Error(w, ErrUnknownTable.Error(), http.StatusBadRequest)
		return
	}

	// Get the actual database info from dbinfo, and then add the extra info from config
	ct := d.config.GetTable(table)
	ret := make([]TableFieldInfoWithMetaData, 0)
	for _, tf := range tableInfo.Fields {
		x := TableFieldInfoWithMetaData{
			TableFieldInfo: tf,
		}
		if ct != nil {
			for _, ctf := range ct.Fields {
				if ctf.Name == tf.Name {
					x.ConfigField = ctf
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
