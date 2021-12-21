package main

import (
	"log"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GroupByField string

type GroupBy struct {
	fname  string
	arrays map[GroupByField]Events
}

func NewGroupBy(fname string) (GroupBy, error) {

	gb := GroupBy{
		fname: fname,
	}

	fr, err := local.NewLocalFileReader(fname)
	if err != nil {
		log.Println("Can't open file")
		return gb, err
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return gb, err
	}

	num := int(pr.GetNumRows())
	log.Println("reading", num, "rows from", fname)
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return gb, err
	}

	arrays := make(map[GroupByField]Events)

	// group by age
	bar := NewProgressBar(num, fname+": GroupBy")
	for _, studentI := range res {
		bar.Add(1)
		student, ok := studentI.(Student)
		if !ok {
			log.Fatal("couldn't convert to student")
		}
		key := student.GroupByKey()

		// should be a btree
		oldArray, ok := arrays[key]
		if !ok {
			a := make([]Student, 1)
			oldArray = Students(a)
		}
		oldArray = oldArray.Add(&student)
		arrays[key] = oldArray
	}

	gb.arrays = arrays

	return gb, nil

}

func (gb *GroupBy) Commit(db *badger.DB) {
	bar := NewProgressBar(len(gb.arrays), gb.fname+": Commit")

	for key, value := range gb.arrays {

		mo := db.GetMergeOperator([]byte(key), app, 1*time.Second)

		valueBytes, err := value.Marshal()
		if err != nil {
			log.Fatal(valueBytes)
		}

		mo.Add(valueBytes)
		mo.Stop()

		bar.Add(1)

	}
}
