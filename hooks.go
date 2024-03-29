package sqliteapi

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type User interface {
	IsAdmin() bool
	GetUsername() string
}

type HookFn func(HookParams) error

type HookAction int

type Map map[string]interface{}

type HookParams struct {
	Table  string
	Key    interface{}
	Data   Map
	Action HookAction
	Tx     *sqlx.Tx
	User   User
}

func (a HookAction) String() string {
	if action, ok := HookActionMap[a]; ok {
		return action
	}
	return fmt.Sprintf("Action %d", a)
}

func (p *HookParams) String() string {
	s := fmt.Sprintf("HookParams: Table '%s', Action: %s", p.Table, p.Action.String())
	if p.Key != nil {
		s += fmt.Sprintf(", Key value: %v", p.Key)
	}
	s += fmt.Sprintf("\nData: %v", p.Data)
	return s
}

func (m Map) Set(field string, value interface{}) {
	if m == nil {
		return
	}
	m[field] = value
}

func (m Map) Clear(field string) {
	if m == nil {
		return
	}
	delete(m, field)
}

func (m Map) Get(field string) interface{} {
	if m == nil {
		return ""
	}
	if v, ok := m[field]; ok {
		return v
	}
	return nil
}

func (m Map) GetString(field string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[field]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		switch v.(type) {
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%d", v)
		case float32, float64:
			return fmt.Sprintf("%f", v)
		}
	}
	return ""
}

const (
	HookAllAction = HookAction(iota)
	HookBeforeInsert
	HookBeforeUpdate
	HookBeforeDelete
	HookAfterInsert
	HookAfterUpdate
	HookAfterDelete
	HookBeforeFunction
	HookAfterFunction
)

var HookActionMap = map[HookAction]string{
	HookAllAction:      "All",
	HookBeforeInsert:   "Before insert",
	HookBeforeUpdate:   "Before update",
	HookBeforeDelete:   "Before delete",
	HookAfterInsert:    "After insert",
	HookAfterUpdate:    "After update",
	HookAfterDelete:    "After delete",
	HookBeforeFunction: "Before function",
	HookAfterFunction:  "After function",
}

type Hook struct {
	Table string
	Fn    HookFn
}

func (p *HookParams) IsBefore() bool {
	return p.Action < HookAfterInsert
}

func (p *HookParams) IsAfter() bool {
	return !p.IsBefore()
}

func (d *Database) AddHook(table string, fn HookFn) {
	d.Lock()
	defer d.Unlock()
	d.hooks = append(d.hooks, Hook{
		Table: table,
		Fn:    fn,
	})
}

func (d *Database) runHooks(table string, params HookParams) error {
	var err error
	for _, hook := range d.hooks {
		if hook.Table == "" || hook.Table == "*" || hook.Table == table {
			err = hook.Fn(params)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
