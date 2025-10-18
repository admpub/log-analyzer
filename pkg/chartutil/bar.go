package chartutil

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

func NewBar(w io.Writer, options []charts.GlobalOpts, headTitles []string, addSeries func(*charts.Bar)) *charts.Bar {
	bar := charts.NewBar()
	options = append(options, Initialization(``, ``))
	// set some global options like Title/Legend/ToolTip or anything else
	bar.SetGlobalOptions(options...)

	// Put data into instance
	bar.SetXAxis(headTitles)

	//TODO
	//bar.AddSeries("Category A", generateBarItems())
	if addSeries != nil {
		addSeries(bar)
	}

	if w != nil {
		bar.Render(w)
	}
	return bar
}

func NewBarDatas() *BarDatasMap {
	return &BarDatasMap{
		m: map[string][]opts.BarData{},
	}
}

type BarDatasMap struct {
	m map[string][]opts.BarData
	r []string
}

func (b *BarDatasMap) SetDatasMap(index int, key string, value interface{}, size int, options ...func(*opts.BarData)) {
	datas, ok := b.m[key]
	if !ok {
		datas = make([]opts.BarData, size)
		b.m[key] = datas
		b.r = append(b.r, key)
	}
	datas[index] = opts.BarData{
		Name:  key,
		Value: value,
	}
	for _, o := range options {
		o(&datas[index])
	}
}

func (b BarDatasMap) AddSeries(bar *charts.Bar) {
	for _, key := range b.r {
		val := b.m[key]
		if val[0].ItemStyle != nil {
			bar.AddSeries(key, val, charts.WithItemStyleOpts(*val[0].ItemStyle))
		} else {
			bar.AddSeries(key, val)
		}
	}
}
