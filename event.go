package main

import (
	"log"
	"strconv"

	"github.com/mikedewar/batchProfiles/protos"
	"google.golang.org/protobuf/proto"
)

type Event interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
	GroupByKey() GroupByField
}

type Events interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
	Add(Event) Events
}

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

func (s *Student) Unmarshal(data []byte) error {
	return nil
}

// Marhsal converts the Student struct to protocol buffer bytes
func (s *Student) Marshal() (data []byte, err error) {

	outStudent := protos.Student{
		Name:    s.Name,
		Age:     s.Age,
		Id:      s.Id,
		Weight:  s.Weight,
		Sex:     s.Sex,
		Day:     s.Day,
		Scores:  s.Scores,
		Ignored: s.Ignored,
	}

	return proto.Marshal(&outStudent)

}

func (s *Student) GroupByKey() GroupByField {
	age := strconv.Itoa(int(s.Age))
	return GroupByField(age)
}

type Students []Student

func (ss Students) Unmarshal(data []byte) error {
	var protoStudents protos.Students
	err := proto.Unmarshal(data, protoStudents)
	if err != nil {
		return err
	}
}

func (ss Students) Marshal() ([]byte, error) {
	return nil, nil
}

func (ss Students) Add(e Event) Events {
	old := []Student(ss)
	newStudent := e.(*Student)
	newArray := append(old, *newStudent)
	return Students(newArray)
}

func app(currentStudents, newStudent []byte) []byte {
	// unmarshal the original array
	var students Students
	log.Println(currentStudents)
	err := students.Unmarshal(currentStudents)
	log.Fatal(err)
	return nil
}

/*
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
*/
