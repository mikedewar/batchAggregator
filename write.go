package main

import (
	"log"
	"strconv"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func WriteSample(numFiles int) {

	for i := 0; i < numFiles; i++ {

		fname := "sample_" + strconv.Itoa(i) + ".parquet"

		fw, err := local.NewLocalFileWriter(fname)
		if err != nil {
			log.Println("Can't create local file", err)
			return
		}

		//write
		pw, err := writer.NewParquetWriter(fw, new(Student), 4)
		if err != nil {
			log.Println("Can't create parquet writer", err)
			return
		}

		pw.RowGroupSize = 128 * 1024 * 1024 //128M
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
		num := 100000
		for i := 0; i < num; i++ {
			stu := Student{
				Name:   "StudentName",
				Age:    int32(20 + i%100),
				Id:     int64(i),
				Weight: float32(50.0 + float32(i)*0.1),
				Sex:    bool(i%2 == 0),
				Day:    int32(time.Now().Unix() / 3600 / 24),
				Scores: map[string]int32{
					"math":     int32(90 + i%5),
					"physics":  int32(90 + i%3),
					"computer": int32(80 + i%10),
				},
			}
			if err = pw.Write(stu); err != nil {
				log.Println("Write error", err)
			}
		}
		if err = pw.WriteStop(); err != nil {
			log.Println("WriteStop error", err)
			return
		}
		log.Println("Write Finished")
		fw.Close()
	}

}
