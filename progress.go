package main

import (
	"log"

	"github.com/schollz/progressbar/v3"
)

func NewProgressBar(num int, desc string) *progressbar.ProgressBar {

	theme := progressbar.Theme{
		Saucer:        "[green]-[reset]",
		SaucerHead:    "[green]>[reset]",
		SaucerPadding: " ",
		BarStart:      "[",
		BarEnd:        "]",
	}

	cmpl := func() { log.Println("") }

	return progressbar.NewOptions(num,
		progressbar.OptionSetDescription(desc),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(theme),
		progressbar.OptionOnCompletion(cmpl),
	)

}
