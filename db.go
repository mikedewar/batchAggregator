package main

import (
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

type DB struct {
	mergeOperators map[string]*badger.MergeOperator
	db             *badger.DB
	f              badger.MergeFunc
}

func NewDB(path string, f badger.MergeFunc) DB {
	options := badger.DefaultOptions("/tmp/badger")
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

func (db *DB) Stop() {
	log.Println("stopping ", len(db.mergeOperators), " merge operators")
	for _, mo := range db.mergeOperators {
		mo.Stop()
	}

}
