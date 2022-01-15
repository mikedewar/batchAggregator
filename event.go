package main

import (
	"log"
	"strconv"

	"github.com/mikedewar/batchProfiles/protos"
	"google.golang.org/protobuf/proto"
)

type Student struct {
	Name    string           `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32            `parquet:"name=age, type=INT32"`
	Id      string           `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Weight  float32          `parquet:"name=weight, type=FLOAT"`
	Sex     bool             `parquet:"name=sex, type=BOOLEAN"`
	Day     int32            `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Scores  map[string]int32 `parquet:"name=scores, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	Ignored int32            //without parquet tag and won't write
}

func (s *Student) GetID() string {
	return s.Id
}

func (ss *Student) Unmarshal(data []byte) error {
	var s protos.Student
	err := proto.Unmarshal(data, &s)
	if err != nil {
		log.Println(err)
		return err
	}
	ss.Name = s.Name
	ss.Age = s.Age
	ss.Id = s.Id
	ss.Weight = s.Weight
	ss.Sex = s.Sex
	ss.Day = s.Day
	ss.Scores = s.Scores
	ss.Ignored = s.Ignored
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

type Students struct {
	data []Student
}

func (ss *Students) Unmarshal(data []byte) error {
	var protoStudents protos.Students
	err := proto.Unmarshal(data, &protoStudents)
	if err != nil {
		log.Println(string(data))
		return err
	}
	// now protoStudents is a struct called Students with an array in it also
	// called Students. So let's unpack that into our []Students
	myss := make([]Student, len(protoStudents.Students))
	for i, s := range protoStudents.Students {

		myss[i] = Student{
			Name:    s.Name,
			Age:     s.Age,
			Id:      s.Id,
			Weight:  s.Weight,
			Sex:     s.Sex,
			Day:     s.Day,
			Scores:  s.Scores,
			Ignored: s.Ignored,
		}

	}

	ss.data = myss

	return nil

}

func (ss *Students) Marshal() ([]byte, error) {
	var protoStudents protos.Students

	outStudents := make([]*protos.Student, len(ss.data))
	for i, s := range ss.data {

		outStudents[i] = &protos.Student{
			Name:    s.Name,
			Age:     s.Age,
			Id:      s.Id,
			Weight:  s.Weight,
			Sex:     s.Sex,
			Day:     s.Day,
			Scores:  s.Scores,
			Ignored: s.Ignored,
		}

	}

	protoStudents.Students = outStudents

	return proto.Marshal(&protoStudents)
}

func (ss *Students) Add(s Student) {
	old := ss.data
	newArray := append(old, s)
	ss.data = newArray
}

func app(currentStudents, newStudent []byte) []byte {
	// unmarshal the original array
	var c_students Students
	err := c_students.Unmarshal(currentStudents)
	if err != nil {
		log.Fatal(err)
	}
	var n_student Students
	err = n_student.Unmarshal(newStudent)
	if err != nil {
		log.Fatal(err)
	}
	var all_students Students
	all_students_data := append(c_students.data, n_student.data...)
	all_students.data = all_students_data
	out_bytes, err := all_students.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	return out_bytes
}
