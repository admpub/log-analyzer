package chartutil

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

func NewLine(w io.Writer, options []charts.GlobalOpts, headTitles []string, addSeries func(*charts.Line)) *charts.Line {
	// create a new line instance
	line := charts.NewLine()
	// set some global options like Title/Legend/ToolTip or anything else
	line.SetGlobalOptions(options...)

	// Put data into instance
	line.SetXAxis(headTitles)

	//TODO
	//line.AddSeries("Category A", generateLineItems())
	if addSeries != nil {
		addSeries(line)
	}

	line.SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: opts.Bool(true)}))
	if w != nil {
		line.Render(w)
	}
	return line
}

type LineDatasMap map[string][]opts.LineData

func (b *LineDatasMap) SetDatasMap(index int, key string, value interface{}, size int, options ...func(*opts.LineData)) {
	datas, ok := (*b)[key]
	if !ok {
		datas = make([]opts.LineData, size)
		(*b)[key] = datas
	}
	datas[index] = opts.LineData{
		Name:  key,
		Value: value,
	}
	for _, o := range options {
		o(&datas[index])
	}
}

func (b LineDatasMap) AddSeries(bar *charts.Line) {
	for key, val := range b {
		bar.AddSeries(key, val)
	}
}
