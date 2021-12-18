package main

import "encoding"

type Event interface {
	encoding.BinaryUnmarshaler
	encoding.BinaryMarshaler
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

func (s *Student) UnmarshalBinary(data []byte) error {
	return nil
}

func (s *Student) MarshalBinary() (data []byte, err error) {
	return nil
}
