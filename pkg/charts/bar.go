package charts

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
)

func NewBar(w io.Writer, options []charts.GlobalOpts, headTitles []string) {
	bar := charts.NewBar()
	// set some global options like Title/Legend/ToolTip or anything else
	bar.SetGlobalOptions(options...)

	// Put data into instance
	bar.SetXAxis(headTitles)

	//TODO
	//bar.AddSeries("Category A", generateBarItems())

	bar.Render(w)
}
