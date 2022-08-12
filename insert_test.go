package sqliteapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertInvoiceAndItems(t *testing.T) {
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
		// Log(log.Default()),
		// DebugLog(log.Default()),
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

	inv1["id"] = id

	row, err := db.GetMap("invoice", id, true)
	assert.NoError(t, err)
	assert.Equal(t, inv1, row)

	items := row["invoiceItem_RefTable"].([]map[string]interface{})
	assert.Equal(t, int64(1), items[0]["id"])
	assert.Equal(t, int64(2), items[1]["id"])

	id, err = db.InsertMap("invoice", row, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id)

	row2, err := db.GetMap("invoice", id, true)
	assert.NoError(t, err)
	assert.NotEqual(t, inv1, row2)

	items2 := row["invoiceItem_RefTable"].([]map[string]interface{})
	assert.Equal(t, int64(3), items2[0]["id"])
	assert.Equal(t, int64(10), items2[0]["qty"])
	assert.Equal(t, int64(4), items2[1]["id"])
	assert.Equal(t, int64(20), items2[1]["qty"])

}
