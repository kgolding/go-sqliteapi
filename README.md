# Database API

## List collections

`GET /api` Returns json array

`200 OK`: JSON: `["tablename","viewname"]`

## Raw SQL query

`POST /api`

`200 OK`: JSON: `[{"id":1,"name":"Hello world!"}]`
`400 Bad Request`: TEXT: `no such table: notatable`

## Collection (table or view) information

`GET /api/[collection_name]?info`

`200 OK`: JSON:
```
[
  {
    "name": "id",
    "type": "INTEGER",
    "pk": 1,
    "writeprotected": true
  },
  {
    "name": "createdAt",
    "type": "DATETIME",
    "notnull": true,
    "default": "CURRENT_TIMESTAMP",
    "writeprotected": true
  },
  {
    "name": "name",
    "type": "TEXT",
    "notnull": true
  },
  {
    "name": "login",
    "type": "TEXT",
    "notnull": true,
    "minlen": 3,
    "regexp": "^[a-z][a-z0-9]+$",
    "regexphint": "must start with a letter followed by letters/number only"
  },
]
```

`400 BadRequest`: TEXT: `invalid table name 'notatable`


## Collection (table or view) items

`GET /api/[collection_name]`

`200 OK`: JSON: `[{"id":1,"name":"Hello world!"}]`|

`400 BadRequest`: TEXT: `invalid table name 'notatable``

*Query parameters*

* `select=id,name` return only id & name fields
* `sort=name desc` order by `name	` descending
* `where=name LIKE "K%"` filter using SQL where (must be url encoded so the `%` becomes `%25`)
* `limit=2` limit to a max of 2 rows
* `offset=5` offset results by 5

Example with all parameters `/api/driver?select=id,name&sort=name desc&where=name LIKE "K%"&limit=10&offset=0` to return the id & name of the first 10 rows who's drivers name starts with a `K` sorted by name


## Collection add item

`POST /api/[collection_name]`

Post body must be JSON format. Unknown & write protected fields will be ignored.

`200 OK`: TEXT: Returns new ID e.g.`3`

`400 Bad Request`: TEXT: Examples:
* `no values to store`
* `NOT NULL constraint failed: table.field`
* `cdsid: too short, must be at least 3 chars`

## Collection update item

`PUT /api/[collection_name]/[id]`

* Post body must be JSON format
* Unknown & write protected fields will be ignored
* Only the given fields will be updated, other fields will retain their current values

`200 OK`:

`400 Bad Request`: TEXT: Examples:
* `invalid row ID`
* `no values to store`
* `NOT NULL constraint failed: table.field`
* `cdsid: too short, must be at least 3 chars`

## Collection delete item

`DELETE /api/[collection_name]/[id]`

`200 OK`

`400 Bad Request`: TEXT: Examples:
* `invalid row ID`

`404 Not found`
