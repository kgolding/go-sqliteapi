package gdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-chi/chi"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

// regName validates a string as being a safe table/field name
var regName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

type Database struct {
	DB                  *sqlx.DB
	log                 *log.Logger
	hooks               []Hook
	tableFieldsMetaData map[string]map[string]*TableFieldMetaData
	dbInfo              map[string]*TableInfo
	sync.Mutex
}

func NewDatabase(file string) (*Database, error) {
	var err error
	d := &Database{
		log:                 log.New(),
		hooks:               make([]Hook, 0),
		tableFieldsMetaData: make(map[string]map[string]*TableFieldMetaData),
		dbInfo:              make(map[string]*TableInfo),
	}
	d.log.SetLevel(log.DebugLevel)
	d.DB, err = sqlx.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}

	d.Refresh()

	return d, nil
}

func (d *Database) GetObjects(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ret := []string{}
	for _, info := range d.dbInfo {
		ret = append(ret, info.Name)
	}
	sort.Strings(ret)
	enc := json.NewEncoder(w)
	enc.Encode(ret)
}

func (d *Database) PostSQL(w http.ResponseWriter, r *http.Request) {
	sql, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// @TODO Restrict/Sanitise SQL ?

	w.Header().Set("Content-Type", "application/json")
	err = d.QueryJsonWriter(
		w, string(sql),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type TableFieldInfoWithMetaData struct {
	TableFieldInfo
	TableFieldMetaData
	// Hidden         bool `json:"hidden,omitempty"`
	// WriteProtected bool `json:"writeprotected,omitempty"`
}

func (d *Database) GetRows(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

	tableFields, err := d.CheckTableNameGetFields(table)
	if err != nil {
		log.WithError(err).Error("GetRows - error fetching table info")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if r.URL.RawQuery == "info" {
		ret := make([]TableFieldInfoWithMetaData, 0)
		for _, tf := range tableFields {
			x := TableFieldInfoWithMetaData{
				TableFieldInfo: *tf,
			}
			x.TableFieldMetaData = *d.GetFieldMetaData(table, tf.Name)
			ret = append(ret, x)
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode(ret)
		return
	}

	selectArray, err := d.SanitiseSelectByTable(r.URL.Query().Get("select"), table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	where := r.URL.Query().Get("where")
	sort := r.URL.Query().Get("sort")

	offset, limit, err := d.GetLimitOffset(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	q := "SELECT " + strings.Join(selectArray, ",") + " FROM " + table
	if where != "" {
		q += " WHERE " + where
	}
	if sort != "" {
		q += " ORDER BY " + sort
	}
	q += fmt.Sprintf(" LIMIT %d, %d", offset, limit)

	log.WithFields(log.Fields{
		"sql": q,
	}).Infof("GetRows")

	// Change this to use Content-Type
	switch strings.ToLower(r.URL.Query().Get("format")) {
	case "csv":
		if fname := r.URL.Query().Get("filename"); fname != "" {
			w.Header().Set("Content-Disposition", `attachment; filename="`+fname+`"`)
		}
		w.Header().Set("Content-Type", "text/csv")
		err = d.QueryCsvWriter(w, q)

	default:
		w.Header().Set("Content-Type", "application/json")
		err = d.QueryJsonWriter(w, q)
	}

	if err != nil {
		log.WithField("SQL", q).Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (d *Database) GetLimitOffset(r *http.Request) (int, int, error) {
	offset := 0
	limit := 1000

	if v := r.URL.Query().Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return offset, limit, err
		}
		if n < 0 {
			return offset, limit, errors.New("invalid offset")
		}
	}

	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return offset, limit, err
		}
		if n < 0 {
			return offset, limit, errors.New("invalid offset")
		}
	}
	return offset, limit, nil
}

func (d *Database) PostTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

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

func (d *Database) GetRow(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

	_, err := d.CheckTableNameGetFields(table)
	if err != nil {
		log.WithError(err).Error("GetRow - error fetching table info")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "invalid row ID", http.StatusBadRequest)
		return
	}

	selectArray, err := d.SanitiseSelectByTable(r.URL.Query().Get("select"), table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	q := "SELECT " + strings.Join(selectArray, ",") + " FROM `" + table + "` WHERE id=?"
	log.Info(q)
	err = d.queryJsonWriterRow(w, q, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

func (d *Database) PutRow(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(chi.URLParam(r, "id"))
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

func (d *Database) DelRow(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	if !regName.MatchString(table) {
		http.Error(w, "invalid table/view", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(chi.URLParam(r, "id"))
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

func (d *Database) Close() {
	d.DB.Close()
}

// type TableField struct {
// 	CID          int
// 	Name         string
// 	Type         string
// 	NotNull      bool
// 	DefaultValue interface{}
// 	PrimaryKey   bool
// }

// func (d *Database) TableInfo(table string) (map[string]TableField, error) {
// 	rows, err := d.DB.Query("PRAGMA TABLE_INFO(`" + table + "`);")
// 	if err != nil {
// 		return nil, err
// 	}
// 	tableFields := make(map[string]TableField)
// 	for rows.Next() {
// 		tf := TableField{}
// 		err = rows.Scan(&tf.CID, &tf.Name, &tf.Type, &tf.NotNull, &tf.DefaultValue, &tf.PrimaryKey)
// 		if err != nil {
// 			return nil, err
// 		}
// 		tableFields[tf.Name] = tf
// 	}
// 	if len(tableFields) == 0 {
// 		return nil, errors.New("Unknown table or no fields")
// 	}
// 	return tableFields, nil
// }

func (d *Database) SanitiseSelectByTable(selectStr string, table string) ([]string, error) {
	invalidFields := []string{}
	selectArray := []string{}

	selectStr = strings.TrimSpace(selectStr)
	if selectStr == "" || selectStr == "*" {
		tableFields, err := d.CheckTableNameGetFields(table)
		if err != nil {
			return nil, err
		}
		for _, f := range tableFields {
			if d.IsFieldReadable(table, f.Name) {
				selectArray = append(selectArray, f.Name)
			}
		}
	} else {
		for _, f := range strings.Split(selectStr, ",") {
			f = strings.TrimSpace(f)
			if !regName.MatchString(f) {
				invalidFields = append(invalidFields, f)
			} else if d.IsFieldReadable(table, f) {
				selectArray = append(selectArray, f)
			}
		}
	}
	if len(invalidFields) > 0 {
		return selectArray, fmt.Errorf("invalid field(s): %s", strings.Join(invalidFields, ", "))
	}
	if len(selectArray) == 0 {
		return selectArray, fmt.Errorf("no valid fields")
	}
	return selectArray, nil
}
