package main

import (
	"log"

	"github.com/dgraph-io/badger"
)

type Window struct {
	key    GroupByField
	events []Event
	mo     badger.MergeOperator
}

func NewWindow(k GroupByField) *Window {
	return &Window{
		key: k,
	}
}

// UpdateStorage
func (w *Window) UpdateStorage() error {
	return nil
}

func (w *Window) LoadIntoMemory() error {
	return nil
}

// Values returns an orderd array of Events
// if you need access to the values themselves, use this method
// Note that to update a window with new values, you don't need to explicitly
// retrieve the values, use UpdateStorage instead.
func (w Window) Values() []Event {
	err := w.LoadIntoMemory()
	if err != nil {
		log.Fatal(err)
	}
	return w.events
}
