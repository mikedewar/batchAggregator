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

func TestGetID(t *testing.T) {
	s := GetTestStudent()
	assert.Equal(t, s.GetID(), s.Id, "didn't get the right ID")
}
