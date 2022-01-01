package main

import (
	"log"
	"sync"
)

type identifier struct {
	id       string
	filename string
}

type OldReconciler struct {
	sync.RWMutex
	register map[string]map[string]bool
	idChan   chan identifier
	files    map[string]bool
}

func NewOldReconciler() *OldReconciler {
	return &OldReconciler{
		register: make(map[string]map[string]bool), // true if event has been committed
		idChan:   make(chan identifier),
		files:    make(map[string]bool), // true if all txns have been registered
	}
}

// we have files containing events
//
// we want to be able to say we've started processing (read) and finished
// processing (committed) both individual events and files without affecting
// the architecture of how those files and events are processed.
//
// so we need to methods each for files and events:
// * Register, which says we're aware of the file/event
// * Commit, which says we've done what we're gonna do with the file/event
//
// we also need to be able to check the current state of the file or event via
// its identifier. We need to recognise that a file isn't done until all its
// events are done. We need to be able to keep track of which file an event
// belongs to.
//
// as we're dealing with potentially huge amounts of data, we need to be able
// to persist info to disk once we don't really need it in memory any more.

// registerEvent listens for filenane/id pairs on idChan and registers them in
// a map
func (r *OldReconciler) registerEvent(id identifier) {
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
func (r *OldReconciler) checkEvent(id identifier) bool {
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
func (r *OldReconciler) checkFile(id identifier) bool {
	_, ok := r.register[id.filename]
	return ok
}

// checkFileComplete returns true if all the transactions in a file have been
// registered and committed
func (r *OldReconciler) checkFileComplete(id identifier) bool {
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
func (r *OldReconciler) filecComplete(id identifier) {
	r.Lock()
	defer r.Unlock()
	r.files[id.filename] = true
}

// commitEvent marks a transacton as complete
func (r *OldReconciler) commitEvent(id identifier) {
	r.Lock()
	r.register[id.filename][id.id] = true
	r.Unlock()
}

// full check ensures all transactions have been committed
func (r *OldReconciler) fullCheck() bool {
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
