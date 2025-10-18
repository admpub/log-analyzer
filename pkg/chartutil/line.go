package chartutil

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

func NewLine(w io.Writer, options []charts.GlobalOpts, headTitles []string, addSeries func(*charts.Line)) *charts.Line {
	// create a new line instance
	line := charts.NewLine()
	options = append(options, Initialization(``, ``))
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

func NewLineDatas() *LineDatasMap {
	return &LineDatasMap{
		m: map[string][]opts.LineData{},
	}
}

type LineDatasMap struct {
	m map[string][]opts.LineData
	r []string
}

func (b *LineDatasMap) SetDatasMap(index int, key string, value interface{}, size int, options ...func(*opts.LineData)) {
	datas, ok := b.m[key]
	if !ok {
		datas = make([]opts.LineData, size)
		b.m[key] = datas
		b.r = append(b.r, key)
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
	for _, key := range b.r {
		val := b.m[key]
		bar.AddSeries(key, val)
	}
}
