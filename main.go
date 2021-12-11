package main

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Student struct {
	Name    string           `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32            `parquet:"name=age, type=INT32"`
	Id      int64            `parquet:"name=id, type=INT64"`
	Weight  float32          `parquet:"name=weight, type=FLOAT"`
	Sex     bool             `parquet:"name=sex, type=BOOLEAN"`
	Day     int32            `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Scores  map[string]int32 `parquet:"name=scores, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	Ignored int32            //without parquet tag and won't write
}

func main() {
	WriteSample()

	fr, err := local.NewLocalFileReader("to_json.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	log.Print(num)
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}

	// clean up
	err = db.DropAll()
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	// initialise merge operators
	merges := make(map[string]*badger.MergeOperator)

	addTimes := make([]time.Duration, len(res))

	for i, studentI := range res {
		if i%1000 == 0 {
			log.Println(i)
		}
		student, ok := studentI.(Student)
		if !ok {
			log.Fatal("couldn't convert to student")
		}
		key := strconv.Itoa(int(student.Age))

		var mo *badger.MergeOperator
		mo, ok = merges[key]
		if !ok {
			// make a new merge operator
			mo = db.GetMergeOperator([]byte(key), app, 1*time.Second)
			log.Println(len(merges), key)
			merges[key] = mo
		}

		studentBytes, err := json.Marshal(student)
		if err != nil {
			log.Fatal(studentBytes)
		}

		st := time.Now()
		mo.Add(studentBytes)
		addTimes[i] = time.Since(st)

	}

	elapsed := time.Since(start)
	log.Printf("Write took %s", elapsed)
	log.Println(meanDuration(addTimes))

	for _, mo := range merges {
		mo.Stop()
	}

	pr.ReadStop()
	fr.Close()

	db.Close()

	log.Println("stopped")

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
