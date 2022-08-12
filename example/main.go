package main

import (
	"log"
	"net/http"

	"github.com/kgolding/go-sqliteapi"
)

const cfg = `
tables:
  invoice:
    id:
    customer:
      notnull: true
      min: 4
  invoiceItem:
    id:
    invoiceId:
      type: integer
      ref: invoice.id/customer
      notnull: true
    qty:
      type: integer
      min: 0
      notnull: true
    item:
      min: 3
      notnull: true
    cost:
      type: number
    	  notnull: true
`

func main() {
	db, err := sqliteapi.NewDatabase("test.db",
		sqliteapi.YamlConfig([]byte(cfg)),
		sqliteapi.Log(log.Default()),
		// sqliteapi.DebugLog(log.Default()),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert a sample invoice
	db.InsertMap("invoice", map[string]interface{}{
		"customer": "Fred Blogs",
		"invoiceItem_RefTable": []map[string]interface{}{
			{
				"qty":  5,
				"item": "Item A",
				"cost": 1.90,
			},
			{
				"qty":  200,
				"item": "Item B",
				"cost": 0.32,
			},
		},
	}, nil)

	// Create the http server
	mux := http.NewServeMux()
	mux.Handle("/api/", db.Handler("/api/"))

	// Visit http://localhost:8090/api/invoice

	err = http.ListenAndServe(":8090", mux)
	if err != nil {
		panic(err)
	}
}
