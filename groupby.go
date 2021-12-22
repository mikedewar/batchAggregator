package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/vbauerster/mpb/v7"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GroupByField string

type GroupBy struct {
	fname        string
	arrays       map[GroupByField]Events
	num          int
	events       chan Event
	progressBars *mpb.Progress
}

func NewGroupBy(fname string) GroupBy {

	gb := GroupBy{
		fname:        fname,
		events:       make(chan Event),
		arrays:       make(map[GroupByField]Events),
		progressBars: InitProgressBars(),
	}

	return gb
}

func (gb *GroupBy) ReadFiles(fname string) error {
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

	fr, err := local.NewLocalFileReader(parquet_files[0])
	if err != nil {
		log.Println("Can't open file")
		return err
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return err
	}

	num := int(pr.GetNumRows())
	gb.num = num
	bar := gb.AddProgressBar(gb.num, fname+": Read   ")
	//log.Println("reading", gb.num, "rows from", fname)
	for i := 0; i < gb.num; i++ {
		bar.Increment()
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Fatal("Read error", err)
		}
		gb.events <- &stus[0]
	}
	close(gb.events)
	pr.ReadStop()
	return nil
}

func (gb *GroupBy) AsyncBuildGroup() {

	N := 9999

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

	// group by age
	bar := gb.AddProgressBar(len(res), gb.fname+": GroupBy")
	for _, studentI := range res {
		bar.Increment()
		student := studentI
		key := student.GroupByKey()

		// should be a btree
		oldArray, ok := gb.arrays[key]
		if !ok {
			a := make([]Student, 1)
			oldArray = Students(a)
		}
		oldArray = oldArray.Add(student)
		gb.arrays[key] = oldArray
	}

}

func (gb *GroupBy) Commit(db *badger.DB) {
	bar := gb.AddProgressBar(len(gb.arrays), gb.fname+": Commit ")

	for key, value := range gb.arrays {

		mo := db.GetMergeOperator([]byte(key), app, 1*time.Second)

		valueBytes, err := value.Marshal()
		if err != nil {
			log.Fatal(valueBytes)
		}

		mo.Add(valueBytes)
		mo.Stop()

		bar.Increment()

	}
}
