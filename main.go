package main

import (
	"errors"
	"log"
	"os"

	"github.com/dgraph-io/badger"
)

func main() {

	if _, err := os.Stat("to_json.parquet"); errors.Is(err, os.ErrNotExist) {
		log.Println("writing sample")
		WriteSample()
	}

	fname := "to_json.parquet"

	// open the parquet file and group by a key
	gb, err := NewGroupBy(fname)

	options := badger.DefaultOptions("/tmp/badger")
	options.Logger = nil
	db, err := badger.Open(options)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}

	// commit the group by to disk
	gb.Commit(db)

}
