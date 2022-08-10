package sqliteapi

import (
	"fmt"
	"net/http"
	"strings"
)

type muxServer struct {
	d *Database
}

// RegisterHandles add all the required GET/POST/PUT/DELETE handlers on the mux using
// the given prefix/pattern
func (d *Database) RegisterHandles(prefix string, mux *http.ServeMux) {
	handler := muxServer{d}

	mux.Handle(prefix+"/", http.StripPrefix(prefix, handler))
}

// Handler is called to add CRUD handlers to an existing mux mux.Handle("/api/", db.Handler("/api/"))
func (d *Database) Handler(prefix string) http.Handler {
	handler := muxServer{d}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("Handler: %s '%s'\n", r.Method, r.URL.String())
		http.StripPrefix(prefix, handler).ServeHTTP(w, r)
	})
}

func (s muxServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// fmt.Printf("ServeHTTP: %s '%s'\n", r.Method, r.URL.String())

	path := strings.Trim(r.URL.Path, "/")

	parts := []string{}
	if path != "" {
		parts = strings.Split(path, "/")
	}

	// fmt.Printf("ServeHTTP: '%s' [%d] |%s|\n", path, len(parts), strings.Join(parts, "|"))

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
		d.HandlePostTable(w, r)

	case http.MethodPut:
		d.HandlePutRow(w, r)

	case http.MethodDelete:
		d.HandleDelRow(w, r)
	}
}
