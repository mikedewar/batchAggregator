package main

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/v7"
)

func mergeFunc(a, b []byte) []byte {
	return append(a, b...)
}

func CheckValueEqual(db DB, t *testing.T, key, expectedValue string) {

	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		item.Value(func(val []byte) error {
			assert.Equal(t, val, []byte(expectedValue))
			return nil
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewDB(t *testing.T) {
	db := NewDB("/tmp/testbadger", mergeFunc)
	db.db.Close()
}

func TestGetMO(t *testing.T) {
	// first call will get a new MO
	db := NewDB("/tmp/testbadger", mergeFunc)
	defer db.db.Close()
	newmo := db.GetMO("bob")
	// second call should get the same MO
	samemo := db.GetMO("bob")
	assert.Equal(t, newmo, samemo, "the merge operator returned by GetMO under the same key should always be the same operator")
}

func TestStop(t *testing.T) {
	// Stop should cause all the mergeoperators to flush
	// so let's set two going, and then stop them.  They by default flush at 60s
	// intervals so as long as we call Stop within 60s the test should be good
	db := NewDB("/tmp/testbadger", mergeFunc)
	db.db.DropAll()     // so we don't go nuts
	defer db.db.Close() //
	foo := db.GetMO("foo")
	bar := db.GetMO("bar")

	// our mergefunc just appends bytes to bytes so let's just makes some bytes
	hi := []byte("hi")
	there := []byte("there")

	foo.Add(hi)
	foo.Add(there)
	bar.Add(there)
	bar.Add(hi)

	// so these additions aren't all gonna be available to Get just yet

	_ = db.db.View(func(txn *badger.Txn) error {
		item, _ := txn.Get([]byte("foo"))
		item.Value(func(val []byte) error {
			assert.NotEqual(t, val, []byte("hithere"))
			return nil
		})
		return nil
	})

	// but once we call Stop they should make sense

	// gotta make a little progress bar!
	p := mpb.New(mpb.WithWidth(64))
	pbar := p.New(int64(2), mpb.BarStyle())

	db.Stop(pbar)

	time.Sleep(500 * time.Millisecond) // <- annoying; db.Sync() doesn't do what I think it should

	CheckValueEqual(db, t, "foo", "hithere")
	CheckValueEqual(db, t, "bar", "therehi")

}
