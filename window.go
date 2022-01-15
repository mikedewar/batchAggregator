package main

import (
	"log"

	"github.com/dgraph-io/badger"
)

type Window struct {
	key    GroupByField
	events []Student
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

func (w Window) Values() []Student {
	err := w.LoadIntoMemory()
	if err != nil {
		log.Fatal(err)
	}
	return w.events
}
