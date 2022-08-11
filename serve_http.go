package sqliteapi

import (
	"net/http"
	"strings"
)

type muxServer struct {
	prefix string
	d      *Database
}

// RegisterHandles add all the required GET/POST/PUT/DELETE handlers on the mux using
// the given prefix/pattern
func (d *Database) RegisterHandles(prefix string, mux *http.ServeMux) {
	handler := muxServer{
		prefix: prefix,
		d:      d,
	}

	mux.Handle(prefix+"/", handler)
}

// Handler is called to add CRUD handlers to an existing mux mux.Handle("/api/", db.Handler("/api/"))
func (d *Database) Handler(prefix string) http.Handler {
	handler := muxServer{
		prefix: prefix,
		d:      d,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("Handler: %s '%s'\n", r.Method, r.URL.String())
		handler.ServeHTTP(w, r)
	})
}

func (s muxServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// fmt.Printf("ServeHTTP: %s '%s' '%s'\n", r.Method, r.URL.Path, s.prefix)
	path := r.URL.Path
	path = strings.TrimPrefix(path, s.prefix)
	path = strings.Trim(path, "/")

	parts := []string{}
	if path != "" {
		parts = strings.Split(path, "/")
	}

	d := s.d
	switch r.Method {
	case http.MethodGet:
		switch len(parts) {
		case 0:
			d.HandleGetTableNames(w, r)
		case 1:
			d.HandleGetRows(w, r)
		case 2:
			d.HandleGetRow(w, r)
		default:
			http.Error(w, "too many path elements", http.StatusBadRequest)
		}

	case http.MethodPost:
		switch len(parts) {
		case 0:
			d.HandlePostSQL(w, r)
		case 1:
			d.HandlePostTable(w, r)
		default:
			http.Error(w, "too many path elements", http.StatusBadRequest)
		}

	case http.MethodPut:
		d.HandlePutRow(w, r)

	case http.MethodDelete:
		d.HandleDelRow(w, r)
	}
}
