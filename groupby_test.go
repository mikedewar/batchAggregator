package main

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func TestNewGroupBy(t *testing.T) {
	files := []file{"sample_1.parquet"}
	gb := NewGroupBy("/tmp/testbadgber", files)
	gb.Stop()
}

func TestGBStop(t *testing.T) {

	files := []file{"sample_1.parquet"}
	gb := NewGroupBy("/tmp/testbadgber", files)

	// add some mergeOperators

	var mo *badger.MergeOperator

	for i := 0; i < 10; i++ {
		mo = gb.db.GetMO(strconv.Itoa(i))
	}

	gb.Stop()

	// mo is the last merge operator we turned on
	// calling .Add should fail as we should have just closed it
	err := mo.Add([]byte("this should fail"))
	assert.Error(t, err)

}

func TestGetParquetFiles(t *testing.T) {

	// make a temp folder and defer its destruction
	//
	dir, err := os.MkdirTemp("", "testGetParquetFiles")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	// touch two parquet files
	foofile := filepath.Join(dir, "foo.parquet")
	err = os.WriteFile(foofile, []byte("Hello, Gophers!"), 0666)
	if err != nil {
		t.Fatal(err)
	}
	barfile := filepath.Join(dir, "bar.parquet")
	err = os.WriteFile(barfile, []byte("Cheerio, Gophers!"), 0666)
	if err != nil {
		t.Fatal(err)
	}
	// get the parquet files
	files, err := GetParquetFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	// validate that the files are in the output
	expected := []file{file(barfile), file(foofile)} // alphabetical order?
	assert.Equal(t, files, expected)

}

func TestUnpackFile(t *testing.T) {

	// make a parquet file with three events in it{
	dir, err := os.MkdirTemp("", "testUnpackFile")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fname := filepath.Join(dir, "testUnpackFile.parquet")
	fw, err := local.NewLocalFileWriter(fname)
	if err != nil {
		t.Fatal("Can't create local file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		t.Fatal("Can't create parquet writer", err)
		return
	}
	// i have no clue what these do!
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	i := 123

	stu1 := Student{
		Name:   "Stu",
		Age:    20,
		Id:     uuid.NewString(),
		Weight: float32(50.0 + float32(i)*0.1),
		Sex:    bool(i%2 == 0),
		Day:    int32(time.Now().Unix() / 3600 / 24),
		Scores: map[string]int32{
			"math":     int32(90 + i%5),
			"physics":  int32(90 + i%3),
			"computer": int32(80 + i%10),
		},
	}

	if err = pw.Write(stu1); err != nil {
		t.Fatal("Write error", err)
	}

	stu2 := Student{
		Name:   "Steve",
		Age:    21,
		Id:     uuid.NewString(),
		Weight: float32(50.0 + float32(i)*0.1),
		Sex:    bool(i%2 == 0),
		Day:    int32(time.Now().Unix() / 3600 / 24),
		Scores: map[string]int32{
			"math":     int32(90 + i%5),
			"physics":  int32(90 + i%3),
			"computer": int32(80 + i%10),
		},
	}

	if err = pw.Write(stu2); err != nil {
		t.Fatal("Write error", err)
	}

	stu3 := Student{
		Name:   "Stella",
		Age:    21,
		Id:     uuid.NewString(),
		Weight: float32(50.0 + float32(i)*0.1),
		Sex:    bool(i%2 == 0),
		Day:    int32(time.Now().Unix() / 3600 / 24),
		Scores: map[string]int32{
			"math":     int32(90 + i%5),
			"physics":  int32(90 + i%3),
			"computer": int32(80 + i%10),
		},
	}

	if err = pw.Write(stu3); err != nil {
		t.Fatal("Write error", err)
	}

	if err = pw.WriteStop(); err != nil {
		t.Fatal("WriteStop error", err)
		return
	}

	fw.Close()

	// make a groupby object
	files, err := GetParquetFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	gb := NewGroupBy("/tmp/testbadgber", files)

	// kick off the unpack in a go rountine
	// it will be blocked until we start listening to gb.events
	go gb.UnpackFile(files[0])

	// read from gb.events and ensure the events are in order
	e := <-gb.events
	assert.Equal(t, stu1, e.event)
	e = <-gb.events
	assert.Equal(t, stu2, e.event)
	e = <-gb.events
	assert.Equal(t, stu3, e.event)

}

func TestProcessFiles(t *testing.T) {

	// make two parquet files in a folder, with three events in each

	students_1 := GetStudents(3)
	students_2 := GetStudents(3)

	dir, err := os.MkdirTemp("", "testProcessFile")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// do the first file with students_1 in it
	fname := filepath.Join(dir, "sudents_1.parquet")
	fw, err := local.NewLocalFileWriter(fname)
	if err != nil {
		t.Fatal("Can't create local file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		t.Fatal("Can't create parquet writer", err)
		return
	}
	// i have no clue what these do!
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, student := range students_1 {
		if err = pw.Write(student); err != nil {
			t.Fatal("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		t.Fatal("WriteStop error", err)
		return
	}
	fw.Close()

	// do the second file with students_2 in it
	fname = filepath.Join(dir, "sudents_2.parquet")
	fw, err = local.NewLocalFileWriter(fname)
	if err != nil {
		t.Fatal("Can't create local file", err)
		return
	}
	pw, err = writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		t.Fatal("Can't create parquet writer", err)
		return
	}
	// i have no clue what these do!
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, student := range students_2 {
		if err = pw.Write(student); err != nil {
			t.Fatal("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		t.Fatal("WriteStop error", err)
		return
	}
	fw.Close()

	// make a groupby object
	files, err := GetParquetFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	gb := NewGroupBy("/tmp/testbadgber", files)

	// kick off the Processing in a go rountine
	// it will be blocked until we start listening to gb.events

	go gb.ProcessFiles()

	// consume from gb.events and make sure we get all 6 events

	received := make([]Student, 6)

	i := 0
	for eventw := range gb.events {
		received[i] = eventw.event
		i++
	}

	students := append(students_1, students_2...)

	assert.ElementsMatch(t, students, received)

}
