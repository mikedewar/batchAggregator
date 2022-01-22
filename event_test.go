package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func GetTestStudent() Student {

	return Student{
		Name:   "Mike",
		Age:    41,
		Id:     uuid.NewString(),
		Weight: 86.4,
		Sex:    true,
		Day:    int32(time.Now().Unix() / 3600 / 24),
		Scores: map[string]int32{
			"math":     int32(90),
			"physics":  int32(80),
			"computer": int32(70),
		},
	}

}

func GetStudents(n int) []Student {

	out := make([]Student, n)

	for i := 0; i < n; i++ {
		out[i] = Student{
			Name:   "Mike",
			Age:    41,
			Id:     uuid.NewString(),
			Weight: 86.4,
			Sex:    true,
			Day:    int32(time.Now().Unix() / 3600 / 24),
			Scores: map[string]int32{
				"math":     int32(90),
				"physics":  int32(80),
				"computer": int32(70),
			},
		}
	}

	return out

}

func TestGetID(t *testing.T) {
	s := GetTestStudent()
	assert.Equal(t, s.GetID(), s.Id, "didn't get the right ID")
}

func TestStudentMarshalUnmarshal(t *testing.T) {

	s := GetTestStudent()

	studentBytes, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var thesameguy Student

	err = thesameguy.Unmarshal(studentBytes)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, s, thesameguy)

}

func TestStudentsUnmarshalMarshal(t *testing.T) {

	s1 := GetTestStudent()
	s2 := GetTestStudent()

	ss := Students{
		data: []Student{s1, s2}}

	ssBytes, err := ss.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var thesamestudents Students

	err = thesamestudents.Unmarshal(ssBytes)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, ss, thesamestudents)

}

func TestApp(t *testing.T) {

	s1 := GetTestStudent()
	s2 := GetTestStudent()
	s3 := GetTestStudent()

	s2.Name = "Bob"
	s3.Name = "Sally"

	ss := Students{
		data: []Student{s1, s2}}

	ssBytes, err := ss.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	sss := Students{
		data: []Student{s2, s3}}

	sssBytes, err := sss.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	fourStudentsBytes := app(ssBytes, sssBytes)

	var fourStudents Students

	err = fourStudents.Unmarshal(fourStudentsBytes)
	if err != nil {
		t.Fatal(err)
	}

	originalFourStudents := Students{
		data: []Student{s1, s2, s2, s3}}

	assert.Equal(t, originalFourStudents, fourStudents)

}
