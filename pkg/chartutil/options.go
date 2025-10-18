package chartutil

import (
	"time"

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

func DayStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func DayEnd(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, t.Location())
}
