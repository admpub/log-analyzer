package charts

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

func NewLine(w io.Writer, options []charts.GlobalOpts, headTitles []string) {
	// create a new line instance
	line := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else
	line.SetGlobalOptions(options...)

	// Put data into instance
	line.SetXAxis(headTitles)

	//TODO
	//line.AddSeries("Category A", generateLineItems())

	line.SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(true)}))
	line.Render(w)
}
