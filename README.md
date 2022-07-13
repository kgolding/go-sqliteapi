# Go-SQLAPI

## Golang powered SQLite database API

* **Driven by the database schema** - add a table, add a field and the API will use it after a call to `Refresh()`
* API include metadata for **support dynamic GUI's**
* Super simple **migrations**

See [API Reference](API.html)

## Updating API.html

1. Install aglio if not already installed `npm install -g aglio`
1. Edit `API.apib`
1. Run `aglio -i API.apib -o API.html`
