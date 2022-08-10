package main

import (
	"log"
	"net/http"

	"github.com/kgolding/go-sqlapi"
)

const cfg = `
tables:
  invoice:
    id:
    customer:
  invoiceItem:
    id:
    invoiceId:
      type: integer
      ref: invoice.id/customer
    qty:
      type: integer
      min: 0
    item:
      min: 3
    cost:
      type: number
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

	db.RegisterHandles("/api", mux)

	err = http.ListenAndServe(":8090", mux)
	if err != nil {
		panic(err)
	}
}
