package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type GroupByField string

type GroupBy struct {
	fname  string
	arrays map[GroupByField][]Event
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

	arrays := make(map[GroupByField][]Event)

	// group by age
	bar := NewProgressBar(num, fname+" :GroupBy")
	for _, studentI := range res {
		bar.Add(1)
		student, ok := studentI.(Student)
		if !ok {
			log.Fatal("couldn't convert to student")
		}
		key := student.GroupByKey()

		// should be a btree
		oldArray := arrays[key]
		oldArray = append(oldArray, &student)
		arrays[key] = oldArray
	}

	gb.arrays = arrays

	return gb, nil

}
