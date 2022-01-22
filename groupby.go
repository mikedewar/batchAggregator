package main

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/vbauerster/mpb/v7"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type file string

type GroupByField string

type GroupBy struct {
	events       chan eventWrapper
	progressBars *mpb.Progress
	db           DB
	files        []file
}

// eventWrapper lets us keep track of which file the event came from
type eventWrapper struct {
	event Student
	f     file
}

func NewGroupBy(dbPath string, parquetFiles []file) GroupBy {

	db := NewDB(dbPath, app)

	gb := GroupBy{
		events:       make(chan eventWrapper),
		progressBars: InitProgressBars(),
		db:           db,
		files:        parquetFiles,
	}

	return gb
}

func (gb *GroupBy) Stop() {
	bar := gb.AddProgressBar(len(gb.db.mergeOperators), "finalising write")
	gb.db.Stop(bar)
	gb.db.db.Close() // <- this still feels clunky, think we need a DB.Close()
}

func GetParquetFiles(dirname string) ([]file, error) {

	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	parquet_files := make([]file, 0)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".parquet") {
			parquet_files = append(parquet_files, file(filepath.Join(dirname, f.Name())))
		}
	}

	if len(parquet_files) == 0 {
		return nil, errors.New("can't find any parquet files in " + dirname)
	}

	return parquet_files, nil

}

func (gb *GroupBy) ProcessFiles() error {

	// the watigroup means that all the file reading goroutines must be complete
	// before this function returns
	var wg sync.WaitGroup
	totalbar := gb.AddProgressBar(len(gb.files), "total")

	// the guard channel blocks the for loop below from kicking off too many
	// file-reading goroutines at a time.
	maxGoroutines := 11
	guard := make(chan struct{}, maxGoroutines)

	for _, f := range gb.files {
		wg.Add(1)

		guard <- struct{}{}

		// this function
		go func(f file) {
			defer wg.Done()
			gb.UnpackFile(f)
			totalbar.Increment()
			//gb.reconciler.CommitFile(f)
			<-guard
		}(f)

	}

	wg.Wait()
	close(gb.events)
	return nil
}

func (gb *GroupBy) UnpackFile(f file) {
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
		e := eventWrapper{stus[0], f}
		gb.events <- e

	}
	pr.ReadStop()
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
			togroup := make([]Student, N)
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
	togroup := make([]Student, i)
	for j, e := range res {
		togroup[j] = e.event
	}
	grouped := gb.BuildGroup(togroup)
	gb.Commit(grouped)
}

func (gb *GroupBy) BuildGroup(res []Student) map[GroupByField][]Student {

	arrays := make(map[GroupByField][]Student)

	// group by the GroupByKey
	for _, student := range res {
		key := student.GroupByKey()

		// should be a btree
		oldArray, ok := arrays[key]
		if !ok {
			// if we didn't find it, we need to make an new one
			oldArray = make([]Student, 0)
		}
		// append the new event
		newArray := append(oldArray, student)
		arrays[key] = newArray
	}
	return arrays

}

func (gb *GroupBy) Commit(arrays map[GroupByField][]Student) {

	bar := gb.AddProgressBar(len(arrays), "Committing latest batch")

	for key, array := range arrays {

		// create an Events object to Marshal
		students := Students{data: array}

		mo := gb.db.GetMO(string(key))

		studentsBytes, err := students.Marshal()
		if err != nil {
			log.Fatal(studentsBytes)
		}

		mo.Add(studentsBytes)
		bar.Increment()

	}
}
