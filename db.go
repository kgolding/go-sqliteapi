package sqliteapi

import (
	"io/ioutil"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const (
	RefLabelSuffix = "_RefLabel"
	RefTableSuffix = "_RefTable"
)

type Database struct {
	DB       *sqlx.DB
	log      SimpleLogger
	debugLog SimpleLogger
	hooks    []Hook
	dbInfo   TableInfos
	config   *Config
	timeout  time.Duration
	sync.Mutex
}

// NewDatabase opens/creates database
func NewDatabase(file string, opts ...Option) (*Database, error) {
	var err error
	d := &Database{
		log:      log.New(ioutil.Discard, "", 0),
		debugLog: log.New(ioutil.Discard, "", 0),
		hooks:    make([]Hook, 0),
		dbInfo:   make(TableInfos),
		timeout:  time.Second * 30,
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

func (d *Database) Close() {
	d.DB.Close()
}

// regName validates a string as being a safe table/field name
var regName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
