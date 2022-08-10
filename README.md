# Go-SQLAPI

## Golang powered SQLite database access layer with API, migrations, foreign key lookup and joined tables

* YAML/Json configuration that provides:
  * Database migrations
  * Automatic joins for references/foreign keys
  * Field validation
* API includes metadata to facilate dynamic GUI's
* Live backup's
* NoSQL like data when retriving an individual item, including sub RefLabel's. e.g. when retriving a single "Invoice" all the InvoiceItems would be returned in a virtual `InvoiceItems_RefTable` field. Posting the same data back will update both the "Invoice" and "InvoiceItem" tables

See [API Reference](API.html)

### Work in progress

Many core features are working, with the following key items outstanding:

* `unique` support when creating a table
* Indexes
* Hooks
* User based access control

### Configuration

Example configuration (YAML) for two tables
* `tableA` has an autoincremented `id` primary key and a `text` field that must have a least 10 chars
* `tableB` has a `code` primary key and a `aId` field that is a reference to the `tableA`.`id` field, that must be provided (notnull), and must exist in `tableA`

```
tables:
  tableA:
    id:
    createdAt:
    text:
      label: Text description
      min: 10
  tableB:
    code:
      pk: true
      notnull: true
    aId:
      label: Table A reference
      ref: tableA.id/text
      notnull: true
```

#### Fields

Fields can have the following attributes:

##### Database related

* `type` SQLite type, defaults to `TEXT` see https://www.sqlite.org/datatype3.html
* `pk` if true this field will be the primary key
* `notnull` if true this field cannot be null
* `default` the SQLite default value
* `ref` Foreign key/reference in the format `tableName`.`keyField`/`labelField`. e.g. `tableA.id/text`
  * `labelField` is one or more comma seperated fields from the referenced table that will be returned using an automatic join as a new field with the keyField name and a `_RefLabel` suffix e.g. `keyField_RefLabel`. Multiple labelField's will be seperated with a `|`

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

`id` and `createdAt` are special fields that have common default settings (but these can be overriden):

* `id` is set as `INTEGER PRIMARY KEY NOT NULL` and thus is mapped to SQLites internal
ROWID which provides autoincrement numbering.
* `createdAt` is set as `DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP` and thus is automatically
set to the timestamp of creation.

Additional special fields can be added via the exported `SpecialFields` map

## Migrations

Configuration changes are automatically detected, and the database schema will be modifed accordingly.
Such migrations will retain data unless a tables field is deleted in which case that fields data will be lost.

When a migration occurs, the `gdb_config` table will be updated to include a copy of the new configuration. The `PRAGMA user_version` will be set to match the `id` of the `gdb_config` table row.

The `gdb_config` table and the `sqlite_*` tables are protected from use via the API.

## Backups

A live backup can be performed by calling the `Backup(path)` method where path is the path/filename to write too.

## Updating API.html

1. Install aglio if not already installed `npm install -g aglio`
1. Edit `API.apib`
1. Run `aglio -i API.apib -o API.html`
