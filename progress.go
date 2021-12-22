package main

import (
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func InitProgressBars() *mpb.Progress {
	p := mpb.New(mpb.WithWidth(64))
	return p
}

func (gb *GroupBy) AddProgressBar(num int, desc string) *mpb.Bar {

	bar := gb.progressBars.AddBar(int64(num),
		// BarFillerBuilder with custom style
		//mpb.BarStyle().Lbound("🔥[").Filler("-").Tip(">").Padding(" ").Rbound("]"),
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name(desc, decor.WC{W: len(desc) + 1, C: decor.DidentRight}),
			// replace ETA decorator with "done" message, OnComplete event
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 4}), "💥",
			),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

	return bar

	/*
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
			//progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(10),
		)
	*/

}
