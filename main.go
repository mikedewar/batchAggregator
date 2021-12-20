package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

func main() {

	if _, err := os.Stat("to_json.parquet"); errors.Is(err, os.ErrNotExist) {
		// path/to/whatever does not exist
		log.Println("writing sample")
		WriteSample()
	}

	fname := "to_json.parquet"
	gb, err := NewGroupBy(fname)

	// append the groups

	options := badger.DefaultOptions("/tmp/badger")
	options.Logger = nil
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}

	bar := NewProgressBar(len(gb.arrays), fname+": Commit")

	for key, students := range gb.arrays {

		mo := db.GetMergeOperator([]byte(key), app, 1*time.Second)

		studentBytes, err := json.Marshal(students)
		if err != nil {
			log.Fatal(studentBytes)
		}

		mo.Add(studentBytes)
		mo.Stop()

		bar.Add(1)

	}

	db.Close()

}

func meanDuration(ds []time.Duration) time.Duration {

	var a int64

	for _, d := range ds {

		a = a + int64(d)

	}

	return time.Duration(a / int64(len(ds)))

}

func app(currentStudents, newStudent []byte) []byte {
	// unmarshal the original array
	var students []Student
	err := json.Unmarshal(currentStudents, &students)
	if err != nil {
		// the first time we unmarshal it's actually going to
		// be a single student so let's try unmarhsalling that first
		// before giving up

		var firstStudent Student
		err = json.Unmarshal(currentStudents, &firstStudent)
		if err != nil {
			log.Fatal(err)
		}

		// add the first student to the still-empty students array
		students = append(students, firstStudent)
	}

	//unmarhsal the new value

	// note that newStudent might be a bunch of already aggregated  messages!
	// so newStudent might be a single student {....} or an array of students
	// [{..}, {..}, ...]

	switch newStudent[0] {
	case []byte("[")[0]:
		var student []Student
		err = json.Unmarshal(newStudent, &student)
		if err != nil {
			log.Println(string(newStudent), "\n//\n", string(currentStudents))
			log.Fatal(err)
		}
		students = append(students, student...)
	case []byte("{")[0]:
		var student Student
		err = json.Unmarshal(newStudent, &student)
		if err != nil {
			log.Println(string(newStudent), "\n//\n", string(currentStudents))
			log.Fatal(err)
		}
		// add the new student to the list and overwrite
		students = append(students, student)
	default:
		log.Fatal("could not unmarshal the new value")
	}

	// Marhsal and return
	studentsBytes, err := json.Marshal(students)
	if err != nil {
		log.Fatal(err)
	}
	return studentsBytes

}
