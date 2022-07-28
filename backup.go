package gdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
)

// https://github.com/rqlite/rqlite/blob/master/db/db.go#L648

// Backup writes a consistent snapshot of the database to the given file.
// This function can be called when changes to the database are in flight.
func (d *Database) Backup(path string) error {
	dstDB, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return fmt.Errorf("create backup database: %s", err)
	}

	dstConn, err := dstDB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer dstConn.Close()
	srcConn, err := d.DB.DB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer srcConn.Close()

	var dstSQLiteConn *sqlite3.SQLiteConn

	// Define the backup function.
	bf := func(driverConn interface{}) error {
		srcSQLiteConn := driverConn.(*sqlite3.SQLiteConn)
		return copyDatabaseConnection(dstSQLiteConn, srcSQLiteConn)
	}

	return dstConn.Raw(
		func(driverConn interface{}) error {
			dstSQLiteConn = driverConn.(*sqlite3.SQLiteConn)
			return srcConn.Raw(bf)
		})
}

const bkDelay = 250

func copyDatabaseConnection(dst, src *sqlite3.SQLiteConn) error {
	bk, err := dst.Backup("main", src, "main")
	if err != nil {
		return err
	}

	for {
		done, err := bk.Step(-1)
		if err != nil {
			bk.Finish()
			return err
		}
		if done {
			break
		}
		time.Sleep(bkDelay * time.Millisecond)
	}
	return bk.Finish()
}
