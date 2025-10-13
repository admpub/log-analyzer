package chartutil

import (
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

// https://github.com/go-echarts/examples

func Title(title, subtitle string, options ...func(*opts.Title)) charts.GlobalOpts {
	option := opts.Title{
		Title:    title,
		Subtitle: subtitle,
	}
	for _, o := range options {
		o(&option)
	}
	return charts.WithTitleOpts(option)
}

func Initialization(title, subtitle string, options ...func(*opts.Initialization)) charts.GlobalOpts {
	option := opts.Initialization{Theme: types.ThemeWesteros}
	for _, o := range options {
		o(&option)
	}
	return charts.WithInitializationOpts(option)
}
