# Go-SQLITEAPI

## Golang powered SQLite database access layer with API, migrations, foreign key lookup and joined tables

* YAML/Json configuration that provides:
  * Database migrations
  * Automatic joins for references/foreign keys
  * Field validation
* API includes metadata to facilitate dynamic GUI's
* Live backup's
* NoSQL like data when for individual items (GET, PUT, POST), including \*_RefTable's. e.g. when retrieving a single `invoice` all the `invoiceItem`'s would be returned in a virtual `invoiceItems_RefTable` field. Posting/Putting the same data back will update both the `Invoice` and `invoiceItem` tables (*_RefTable data replace all existing rows in the joined table, exclude the field to retain existing data).

See [API Reference](API.html)

### Work in progress

Many core features are working, with the following key items outstanding:

* Indexes
* User-based access control

### Configuration

Example configuration (YAML) for two tables, storing an invoice and it's items in a referenced table.

```
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
```

* `invoice.customer` is required and must be at least 4 chars in length
* `invoiceItem.invoiceId` is tied to `invoice.id` using a Foreign key

#### Fields

Fields can have the following attributes:

##### Database related

* `type` SQLite type, defaults to `TEXT` see https://www.sqlite.org/datatype3.html
* `pk` if true this field will be the primary key
* `notnull` if true this field cannot be null
* `unique` if true this field will have a unique index
* `indexed` if true this field will be indexed
* `default` the SQLite default value
* `ref` Foreign key/reference in the format `tableName`.`keyField`/`labelField`. e.g. `tableA.id/text`
  * `labelField` is one or more comma seperated fields from the referenced table that will be returned using an automatic join as a new field with the `keyField` name and a `_RefLabel` suffix e.g. `keyField_RefLabel`. Multiple labelField's will be separated with a `|`

##### Validation related

* `min` Minimum value to accept or minimum length for none numeric fields (e.g. TEXT)
* `max` Maximum value to accept or maximum length for none numeric fields (e.g. TEXT)
* `regex` Regular expression

##### API related

* `hidden` prevents the field from being returned via the API (useful for password fields)
* `readonly` prevents the field from being changed

##### User interface related

* `label` User friendly display label
* `hint` User hint to display as a placeholder/help text
* `control` To advise a GUI what control to use (GUI's should use `type` as well):
  * `text` - Default single line text
  * `textarea` - Multiline
  * `checkbox`
  * `select`

##### Special fields

`id` and `createdAt` are special fields that have common default settings (but these can be overridden):

* `id` is set as `INTEGER PRIMARY KEY NOT NULL` and thus is mapped to SQLites internal
  ROWID which provides autoincrement numbering.
* `createdAt` is set as `DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP` and thus is automatically
  set to the timestamp of creation.

Additional special fields can be added via the exported `SpecialFields` map

## Triggers

Define SQLite triggers to run statements automatically.

````
  triggers:
    triggerTable1Update:
      table: table1
      event: after update of text
      when: new.text != old.text || "dummy"
      statement: INSERT INTO history (table1Id, text) VALUES (new.id, new.text)
````

## Functions

Functions are akin to a stored procedure, where one or more SQL statements can be run within a database transaction
(i.e. all statements must succeed else all are rolled back). Functions are call directly or via an API endpoint.

Example function:

````
functions:
  PayInvoiceInFull:
    params:
      invoiceId:
        notnull: true
        min: 1
    statements:
      - UPDATE invoice SET paid=true WHERE invoiceId=$invoiceId 
````

## Migrations

Configuration changes are automatically detected, and the database schema will be modified accordingly.
Such migrations will retain data unless a table’s field is deleted, in which case that field's data will be lost.

When a migration occurs, the `gdb_config` table will be updated to include a copy of the new configuration. The `PRAGMA user_version` will be set to match the `id` of the `gdb_config` table row.

The `gdb_config` table and the `sqlite_*` tables are protected from use via the API.

## Backups

A live backup can be performed by calling the `Backup(path)` method where path is the path/filename to write too.

## Updating API.html

1. Install aglio if not already installed `npm install -g aglio`
1. Edit `API.apib`
1. Run `aglio -i API.apib -o API.html`
