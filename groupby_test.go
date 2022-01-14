package main

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	log.Println("thesameguy", thesameguy)

	assert.Equal(t, s, thesameguy)

}

func TestApp(t *testing.T) {

	s := GetTestStudent()

	studentBytes, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	twoStudentsBytes := app(studentBytes, studentBytes)

	var bothStudents Students

	err = bothStudents.Unmarshal(twoStudentsBytes)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(bothStudents)

}
