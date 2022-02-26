package main

import (
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/vbauerster/mpb/v7"
)

type DB struct {
	mergeOperators map[string]*badger.MergeOperator
	db             *badger.DB
	f              badger.MergeFunc
}

func NewDB(path string, f badger.MergeFunc) DB {
	options := badger.DefaultOptions(path)
	options.Logger = nil
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}
	return DB{
		mergeOperators: make(map[string]*badger.MergeOperator),
		db:             db,
		f:              f,
	}

}

func (db *DB) GetMO(key string) *badger.MergeOperator {
	mo, ok := db.mergeOperators[key]

	if !ok {
		// new key, make a new MergeOperatr
		mo = db.db.GetMergeOperator([]byte(key), db.f, 60*time.Second)
		db.mergeOperators[key] = mo
	}

	return mo

}

func (db *DB) Stop(progressBar *mpb.Bar) {
	log.Println("stopping", len(db.mergeOperators), "merge operators")

	var wg sync.WaitGroup

	for _, mo := range db.mergeOperators {

		wg.Add(1)
		go func() {
			defer wg.Done()
			mo.Stop()
			progressBar.Increment()
		}()
		time.Sleep(1 * time.Millisecond)
	}
	// once the MOs are stopped, let's sync the db so we leave it in a stable
	// state
	err := db.db.Sync()
	if err != nil {
		log.Fatal(err)
	}
	// and tidy up the garbage
	err = db.db.RunValueLogGC(0.7)
	if err != nil {
		log.Println("Warn:", err) // this will spit an error if it didn't find any cleaning up to do
	}

}
