package main

import (
	"errors"
	"log"
	"os"
	"sync"
)

func main() {

	if _, err := os.Stat("sample_1.parquet"); errors.Is(err, os.ErrNotExist) {
		log.Println("writing sample")
		WriteSample(20)
	}

	gb := NewGroupBy()
	// set the groupby running
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gb.AsyncBuildGroup()
	}()

	// read all the parquet files in the current directory
	err := gb.ReadFiles()
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

	gb.Stop()

	for _, f := range gb.files {
		gb.reconciler.PersistReport(f)
	}

}
