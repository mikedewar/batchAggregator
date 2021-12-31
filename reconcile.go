package main

import (
	"log"
	"sync"
)

type identifier struct {
	id       string
	filename string
}

type Reconciler struct {
	sync.RWMutex
	register map[string]map[string]bool
	idChan   chan identifier
	files    map[string]bool
}

func NewReconciler() *Reconciler {
	return &Reconciler{
		register: make(map[string]map[string]bool), // true if event has been committed
		idChan:   make(chan identifier),
		files:    make(map[string]bool), // true if all txns have been registered
	}
}

// registerEvent listens for filenane/id pairs on idChan and registers them in
// a map
func (r *Reconciler) registerEvent(id identifier) {
	r.Lock()
	ids, ok := r.register[id.filename]
	if !ok {
		// new file
		ids = make(map[string]bool)
		r.files[id.filename] = false // register the new file
	}
	ids[id.id] = false // false until the transaction has been committed
	r.register[id.filename] = ids
}

// checkEvent returns true if the event has been registered already
func (r *Reconciler) checkEvent(id identifier) bool {
	r.RLock()
	defer r.RUnlock()
	ids, ok := r.register[id.filename]
	if !ok {
		return false
	}
	_, ok = ids[id.id]
	return ok
}

// checkFile returns true if the file has been registered already
func (r *Reconciler) checkFile(id identifier) bool {
	_, ok := r.register[id.filename]
	return ok
}

// checkFileComplete returns true if all the transactions in a file have been
// registered and committed
func (r *Reconciler) checkFileComplete(id identifier) bool {
	if !r.files[id.filename] {
		return false // we've not got all the events from the file yet
	}
	ids, ok := r.register[id.filename]
	if !ok {
		log.Fatal("cannot find file", id.filename, "in register")
	}
	for _, committed := range ids {
		if !committed {
			return false // at least one event has not been committed
		}
	}
	return true // all the events have been registered and committed
}

// fileComplete marks that all the events in a file have been registered and
// we're to expect no more events from that file
func (r *Reconciler) filecComplete(id identifier) {
	r.Lock()
	defer r.Unlock()
	r.files[id.filename] = true
}

// commitEvent marks a transacton as complete
func (r *Reconciler) commitEvent(id identifier) {
	r.Lock()
	r.register[id.filename][id.id] = true
	r.Unlock()
}

// full check ensures all transactions have been committed
func (r *Reconciler) fullCheck() bool {
	r.RLock()
	for filename := range r.register {
		fileComplete, ok := r.files[filename]
		if !ok {
			log.Fatal("cannot find file ", filename, " in files map")
		}
		if !fileComplete {
			return false // not all events have been read from this file
		}
	}
	return true
}
