package main

import "strconv"

type Event interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
	GroupByKey() GroupByField
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

func (s *Student) Marshal() (data []byte, err error) {
	return nil, nil
}

func (s *Student) GroupByKey() GroupByField {
	age := strconv.Itoa(int(s.Age))
	return GroupByField(age)
}
