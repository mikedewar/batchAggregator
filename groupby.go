package main

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/vbauerster/mpb/v7"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GroupByField string

type GroupBy struct {
	events       chan Event
	progressBars *mpb.Progress
	db           *badger.DB
}

func NewGroupBy() GroupBy {

	options := badger.DefaultOptions("/tmp/badger")
	options.Logger = nil
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}

	gb := GroupBy{
		events:       make(chan Event),
		progressBars: InitProgressBars(),
		db:           db,
	}

	return gb
}

func (gb *GroupBy) Stop() {
	gb.db.Close()
}

func (gb *GroupBy) ReadFiles() error {
	dirname := "."

	files, err := os.ReadDir(dirname)
	if err != nil {
		log.Fatal(err)
	}

	parquet_files := make([]string, 0)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".parquet") {
			parquet_files = append(parquet_files, file.Name())
		}
	}

	if len(parquet_files) == 0 {
		log.Fatal("can't find any parquet files in ", dirname)
	}

	var wg sync.WaitGroup
	totalbar := gb.AddProgressBar(len(parquet_files), "total")

	maxGoroutines := 10
	guard := make(chan struct{}, maxGoroutines)

	for _, file := range parquet_files {
		wg.Add(1)

		guard <- struct{}{}

		go func(file string) {
			defer wg.Done()
			fr, err := local.NewLocalFileReader(file)
			if err != nil {
				log.Fatal("Can't open file")
			}

			pr, err := reader.NewParquetReader(fr, new(Student), 4)
			if err != nil {
				log.Fatal("Can't create parquet reader", err)
			}

			num := int(pr.GetNumRows())
			bar := gb.AddProgressBar(num, file+": Read   ")
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
				gb.events <- &stus[0]
			}
			pr.ReadStop()
			totalbar.Increment()
			<-guard
		}(file)
	}

	wg.Wait()
	close(gb.events)
	return nil
}

func (gb *GroupBy) AsyncBuildGroup() {

	N := 99999

	res := make([]Event, N)
	i := 0
	for e := range gb.events {
		res[i] = e
		i++
		if i == N {
			//log.Println("building group", len(res))
			gb.BuildGroup(res)
			i = 0
			// blat res and start again
			res = nil
			res = make([]Event, N)
		}
	}

	// do the last
	res = res[:i]
	gb.BuildGroup(res)
}

func (gb *GroupBy) BuildGroup(res []Event) {

	arrays := make(map[GroupByField]Events)

	// group by age
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
	gb.Commit(arrays)

}

func (gb *GroupBy) Commit(arrays map[GroupByField]Events) {

	//	bar := gb.AddProgressBar(len(arrays), "Commit ")

	for key, value := range arrays {

		mo := gb.db.GetMergeOperator([]byte(key), app, 1*time.Second)

		valueBytes, err := value.Marshal()
		if err != nil {
			log.Fatal(valueBytes)
		}

		mo.Add(valueBytes)
		mo.Stop()

		//		bar.Increment()

	}
}
