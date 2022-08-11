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

	// Setup hooks
	db.AddHook("", func(p sqliteapi.HookParams) error {
		log.Printf("Hook: %s\n", p.String())
		return nil
	})

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

	err = http.ListenAndServe(":8090", mux)
	if err != nil {
		panic(err)
	}
}
