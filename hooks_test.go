package sqliteapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHooks(t *testing.T) {
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

	hkCh := make(chan HookParams, 10)
	hkExpectCh := make(chan string, 10)
	defer close(hkExpectCh)
	defer close(hkCh)

	db.AddHook("", func(p HookParams) error {
		t.Logf(p.String())
		select {
		case hkCh <- p:
		default:
			t.Errorf("unexpected hook: %s", p.String())
		}
		return nil
	})

	go func() {
		for msg := range hkExpectCh {
			select {
			case <-time.After(time.Second * 1):
				t.Errorf("timeout waiting for hook: %s", msg)

			case <-hkCh:
				// t.Logf("got expected hook: %s", msg)
			}
		}
		// if len(hkExpectCh) > 0 {
		// 	t.Errorf("am still waiting for %d hooks to fire!", len(hkExpectCh))
		// }
	}()

	hkExpectCh <- ("before insert")
	hkExpectCh <- ("after insert")

	id, err := db.InsertMap("invoice", inv1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)

	hkExpectCh <- ("before insert")
	hkExpectCh <- ("after insert")

	id2, err := db.InsertMap("invoice", inv1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id2)

	hkExpectCh <- ("before delete")
	hkExpectCh <- ("after delete")

	err = db.Delete("invoice", id, nil)
	assert.NoError(t, err)

	row, err := db.GetMap("invoice", id2)
	assert.NoError(t, err)
	assert.Equal(t, inv1, row)

}
