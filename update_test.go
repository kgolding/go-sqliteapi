package sqliteapi

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateInvoiceAndItems(t *testing.T) {
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

	inv1 := map[string]interface{}{"customer": "Fred Blogs"}

	twoItems := []map[string]interface{}{
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
	inv1["invoiceItem_RefTable"] = twoItems

	id, err := db.InsertMap("invoice", inv1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)
	assert.Equal(t, inv1["id"], id)

	err = db.UpdateMap("invoice", map[string]interface{}{"id": id, "customer": "ACME Inc."}, nil)
	assert.NoError(t, err)
	row, err := db.GetMap("invoice", id)
	assert.NoError(t, err)
	assert.NotEqual(t, inv1, row)

	oneItem := []map[string]interface{}{{
		"qty":  int64(500),
		"item": "Item C",
		"cost": 39.95,
	}}
	inv1["invoiceItem_RefTable"] = oneItem

	err = db.UpdateMap("invoice", inv1, nil)
	assert.NoError(t, err)

	row, err = db.GetMap("invoice", id)
	assert.NoError(t, err)
	assert.Equal(t, inv1, row)

	items1 := row["invoiceItem_RefTable"].([]map[string]interface{})
	assert.Equal(t, int64(500), items1[0]["qty"])
	assert.Equal(t, "Item C", items1[0]["item"])
	assert.Equal(t, 39.95, items1[0]["cost"])
}
