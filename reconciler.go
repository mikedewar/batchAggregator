package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

type file string  // id representing a file (e.g. a filename)
type event string // id representing an event

type processingState int64

const (
	Unknown processingState = iota
	Processing
	Committed
)

type Reconciler struct {
	sync.RWMutex
	files    map[file]processingState  // the current state of each file
	events   map[event]processingState // the current state of each event
	register map[file][]event          // the register of file -> events
}

type ReconciliationReport struct {
	File      file                      `json:"fileIdentifier"`
	NumEvents int                       `json:"numEvents"`
	Events    map[event]processingState `json:"events"`
}

func NewReconciler() *Reconciler {
	return &Reconciler{
		files:    make(map[file]processingState),
		events:   make(map[event]processingState),
		register: make(map[file][]event),
	}
}

func (r *Reconciler) RegisterEvent(f file, e event) {
	r.Lock()
	defer r.Unlock()
	// check we're not already processing
	_, ok := r.events[e]
	if ok {
		// TODO this is an opportunity to be idempotent but for now just panic
		log.Fatal("processing an already processing event")
	}

	// register the event as Processing
	r.events[e] = Processing

	// append the event to the file's list of events in the register
	fileEvents, ok := r.register[f]
	if !ok {
		log.Fatal("registering event to unknown file")
	}
	fileEvents = append(fileEvents, e)
	r.register[f] = fileEvents
}

func (r *Reconciler) RegisterFile(f file) {
	r.Lock()
	defer r.Unlock()
	r.files[f] = Processing
}

func (r *Reconciler) CommitEvent(e event) {
	r.Lock()
	defer r.Unlock()
	currentstate, ok := r.events[e]
	// make sure we know about the event
	if !ok {
		log.Fatal("trying to commit an unknown event")
	}
	// make sure we were processing
	if currentstate != Processing {
		log.Fatal("trying to commit an event that wasn't being processed")
	}
	r.events[e] = Committed
}

func (r *Reconciler) CommitFile(f file) {
	r.Lock()
	defer r.Unlock()
	currentstate, ok := r.files[f]
	// make sure we know about the file
	if !ok {
		log.Fatal("trying to commit an unknown file")
	}
	// make sure we were processing
	if currentstate != Processing {
		log.Fatal("trying to commit a file that wasn't being processed")
	}
	r.files[f] = Committed
}

func (r *Reconciler) CheckFile(f file) bool {
	r.RLock()
	defer r.RUnlock()
	return false
	// a file is complete if it has been commmitted and all its events have been commmitted
	currentstate, ok := r.files[f]
	if !ok {
		log.Fatal("trying to check an unknown file")
	}
	// the file hasn't been committed
	if currentstate != Committed {
		return false
	}

	events, ok := r.register[f]
	if !ok {
		log.Fatal("file not found in register when trying to check the file's status")
	}
	for _, e := range events {
		if !r.CheckEvent(e) {
			return false
		}
	}
	return true
}

func (r *Reconciler) CheckEvent(e event) bool {
	r.RLock()
	defer r.RUnlock()
	currentstate, ok := r.events[e]
	if !ok {
		log.Fatal("trying to check unknown event")
	}
	if currentstate == Committed {
		return true
	}
	return false
}

// PersistReport writes a reconciliation report to disk for the specified file
func (r *Reconciler) PersistReport(f file) error {

	events, ok := r.register[f]
	if !ok {
		log.Fatal("Report requested for an unknown file")
	}

	report := ReconciliationReport{
		File:      f,
		NumEvents: len(events),
		Events:    r.events,
	}

	b, err := json.Marshal(report)
	if err != nil {
		return err
	}
	err = os.WriteFile("reconciliation_report_"+string(f)+".json", b, 0644)
	return err

}
