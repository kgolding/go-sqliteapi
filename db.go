package gdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// regName validates a string as being a safe table/field name
var regName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

type Database struct {
	DB    *sqlx.DB
	log   SimpleLogger
	hooks []Hook
	// tableFieldsMetaData map[string]map[string]*TableFieldMetaData
	dbInfo  TableInfos
	config  *Config
	timeout time.Duration
	sync.Mutex
}

type Option func(*Database) error

func YamlConfig(b []byte) Option {
	return func(d *Database) error {
		c, err := NewConfigFromYaml(b)
		if err != nil {
			return err
		}
		err = d.ApplyConfig(c, &ConfigOptions{
			RetainUnmanaged: true,
			// DryRun:          true,
			Logger: log.Default(),
		})
		if err != nil {
			return err
		}
		d.log.Println("Config:\n" + c.String())
		d.config = c
		return nil
	}
}

type SimpleLogger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

func Log(logger SimpleLogger) Option {
	return func(d *Database) error {
		d.log = logger
		return nil
	}
}

func NewDatabase(file string, opts ...Option) (*Database, error) {
	var err error
	d := &Database{
		log:   log.New(ioutil.Discard, "", 0),
		hooks: make([]Hook, 0),
		// tableFieldsMetaData: make(map[string]map[string]*TableFieldMetaData),
		dbInfo:  make(TableInfos),
		timeout: time.Second * 30,
	}
	d.DB, err = sqlx.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}

	d.DB.Exec("PRAGMA foreign_keys=ON")

	for _, opt := range opts {
		err = opt(d)
		if err != nil {
			d.Close()
			return nil, err
		}
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
	err = d.QueryJsonWriter(w, string(sql), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

type TableFieldInfoWithMetaData struct {
	TableFieldInfo
	Field
}

func (d *Database) Close() {
	d.DB.Close()
}

// SanitiseSelectByTable takes a comma seperated list of fields, and returns an
// array of ResultColumn's, removing any hidden fields
func (d *Database) SanitiseSelectByTable(selectStr string, table string) (ResultColumns, error) {
	invalidFields := []string{}
	selectArray := make(ResultColumns, 0)

	selectStr = strings.TrimSpace(selectStr)
	if selectStr == "" || selectStr == "*" {
		tableFields, err := d.CheckTableNameGetFields(table)
		if err != nil {
			return nil, err
		}
		for _, f := range tableFields {
			if d.IsFieldReadable(table, f.Name) {
				selectArray = append(selectArray, ResultColumn{Table: table, Field: f.Name})
			}
		}
	} else {
		for _, f := range strings.Split(selectStr, ",") {
			f = strings.TrimSpace(f)
			if !regName.MatchString(f) {
				invalidFields = append(invalidFields, f)
			} else if d.IsFieldReadable(table, f) {
				selectArray = append(selectArray, ResultColumn{Table: table, Field: f})
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
