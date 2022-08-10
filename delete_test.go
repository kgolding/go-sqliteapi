package sqliteapi

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteInvoiceAndItems(t *testing.T) {
	const yaml = `
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
    cost:
      type: number
`
	db, err := NewDatabase("file::memory:?cache=shared",
		YamlConfig([]byte(yaml)),
		Log(log.Default()),
		DebugLog(log.Default()),
	)
	assert.NoError(t, err)
	defer db.Close()

	inv1 := map[string]interface{}{
		"customer": "Fred Blogs"}

	inv1["invoiceItem_RefTable"] = []map[string]interface{}{
		{
			"qty":  int64(10),
			"item": "Item A",
			"cost": 19.99,
		},
		{
			"qty":  int64(20),
			"item": "Item B",
			"cost": 99999.99,
		},
	}

	id, err := db.InsertMap("invoice", inv1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)

	id2, err := db.InsertMap("invoice", inv1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id2)

	err = db.Delete("invoice", id, nil)
	assert.NoError(t, err)

	row, err := db.GetMap("invoice", id2)
	assert.NoError(t, err)
	assert.Equal(t, inv1, row)

}
