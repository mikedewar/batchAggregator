package main

import (
	"log"
	"os"
	"strings"
	"sync"

	"github.com/vbauerster/mpb/v7"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GroupByField string

type GroupBy struct {
	events       chan eventWrapper
	progressBars *mpb.Progress
	db           DB
	//reconciler   *Reconciler
	files []file
}

// eventWrapper lets us keep track of which file the event came from
type eventWrapper struct {
	event Event
	f     file
}

func NewGroupBy() GroupBy {

	db := NewDB("/tmp/badger", app)

	gb := GroupBy{
		events:       make(chan eventWrapper),
		progressBars: InitProgressBars(),
		db:           db,
		//reconciler:   NewReconciler(),
	}

	return gb
}

func (gb *GroupBy) Stop() {
	bar := gb.AddProgressBar(len(gb.db.mergeOperators), "finalising write")
	for _, mo := range gb.db.mergeOperators {
		mo.Stop()
		bar.Increment()
	}
}

// ReadFiles looks for all the parquet files in a folder, reads them by parsing
// each line and placing the resulting object on a channel for downstream
// processing. ReadFiles is designed to manage the reading of multiple files at
// the same time.
func (gb *GroupBy) ReadFiles() error {
	dirname := "."

	files, err := os.ReadDir(dirname)
	if err != nil {
		log.Fatal(err)
	}

	parquet_files := make([]file, 0)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".parquet") {
			parquet_files = append(parquet_files, file(f.Name()))
		}
	}

	if len(parquet_files) == 0 {
		log.Fatal("can't find any parquet files in ", dirname)
	}

	gb.files = parquet_files

	// the watigroup means that all the file reading goroutines must be complete
	// before this function returns
	var wg sync.WaitGroup
	totalbar := gb.AddProgressBar(len(parquet_files), "total")

	// the guard channel blocks the for loop below from kicking off too many
	// file-reading goroutines at a time.
	maxGoroutines := 11
	guard := make(chan struct{}, maxGoroutines)

	for _, f := range parquet_files {
		wg.Add(1)

		guard <- struct{}{}

		go func(f file) {
			defer wg.Done()
			fr, err := local.NewLocalFileReader(string(f))
			if err != nil {
				log.Fatal("Can't open file")
			}

			pr, err := reader.NewParquetReader(fr, new(Student), 4)
			if err != nil {
				log.Fatal("Can't create parquet reader", err)
			}

			//gb.reconciler.RegisterFile(f)

			num := int(pr.GetNumRows())
			bar := gb.AddProgressBar(num, string(f)+": Read   ")
			for i := 0; i < num; i++ {
				bar.Increment()
				stus := make([]Student, 1)
				if err = pr.Read(&stus); err != nil {
					log.Fatal("Read error", err)
				}
				if len(stus) == 0 {
					log.Println("wtf")
					continue
				}
				e := eventWrapper{&stus[0], f}
				gb.events <- e
			}
			pr.ReadStop()
			totalbar.Increment()
			//gb.reconciler.CommitFile(f)
			<-guard
		}(f)

	}

	wg.Wait()
	close(gb.events)
	return nil
}

//AsyncBuildGroup reads a channel of events, batching them up into large
//groups and sending them to BuildGroup to be re-ogranised by the GroupByKey
//and then to Commit for it to be commited to the badger db
func (gb *GroupBy) AsyncBuildGroup() {

	N := 5000000 // this really needs to be something to do with spare RAM

	res := make([]eventWrapper, N)

	i := 0
	for e := range gb.events {

		//eventID := e.event.GetID()

		//gb.reconciler.RegisterEvent(e.f, event(eventID))

		res[i] = e
		i++
		if i == N {

			// this is clumsy
			togroup := make([]Event, N)
			for j, e := range res {
				togroup[j] = e.event
			}

			grouped := gb.BuildGroup(togroup)
			gb.Commit(grouped)
			i = 0

			// commit all the events in the group
			/*
				for _, e := range res {
					gb.reconciler.CommitEvent(e.f, event(e.event.GetID()))
				}*/
			// blat res and start again
			res = nil
			res = make([]eventWrapper, N)
		}
	}

	// do the last
	res = res[:i]
	togroup := make([]Event, i)
	for j, e := range res {
		togroup[j] = e.event
	}
	grouped := gb.BuildGroup(togroup)
	gb.Commit(grouped)
}

func (gb *GroupBy) BuildGroup(res []Event) map[GroupByField]Events {

	arrays := make(map[GroupByField]Events)

	// group by the GroupByKey
	for _, studentI := range res {
		//	bar.Increment()
		student := studentI
		key := student.GroupByKey()

		// should be a btree
		oldArray, ok := arrays[key]
		if !ok {
			a := make([]Student, 1)
			oldArray = Students(a)
		}
		oldArray = oldArray.Add(student)
		arrays[key] = oldArray
	}
	return arrays

}

func (gb *GroupBy) Commit(arrays map[GroupByField]Events) {

	bar := gb.AddProgressBar(len(arrays), "Committing latest batch")

	for key, value := range arrays {

		mo := gb.db.GetMO(string(key))

		valueBytes, err := value.Marshal()
		if err != nil {
			log.Fatal(valueBytes)
		}

		mo.Add(valueBytes)
		bar.Increment()

	}
}
