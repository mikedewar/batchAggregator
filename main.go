package main

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/dgraph-io/badger"
)

func main() {

	if _, err := os.Stat("sample_1.parquet"); errors.Is(err, os.ErrNotExist) {
		log.Println("writing sample")
		WriteSample(2)
	}

	fname := "sample_1.parquet"

	gb := NewGroupBy(fname)
	// set the groupby running
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gb.AsyncBuildGroup()
	}()

	// open the parquet file and group by a key
	err := gb.ReadFiles(fname)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()
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
