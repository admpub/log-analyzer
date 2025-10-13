package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/admpub/log-analyzer/pkg/chartutil"
	"github.com/admpub/log-analyzer/pkg/parse"
	"github.com/admpub/log-analyzer/pkg/storage/duckdb"
	"github.com/go-chi/render"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/webx-top/com"
)

func handleChart(w http.ResponseWriter, r *http.Request, cfg *parse.Config, historyItemStyleOpts func(bd *opts.BarData)) {
	em, err := cfg.Storager()
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	kdb, ok := em.(duckdb.Storager)
	if !ok {
		render.Render(w, r, ErrInternalServerError(fmt.Errorf(`unsupported storage`)))
		return
	}
	now := time.Now()

	// == 一周访客量 ==

	days := 7
	headTitles := make([]string, days)
	dayDur := 24 * time.Hour
	datasMapUV := chartutil.BarDatasMap{}
	timeRanges := make([]duckdb.TimeRange, 0, days)
	weekdayN := int(now.Weekday())
	weekStart := duckdb.DayStart(now.Add(dayDur * -time.Duration(weekdayN)))
	counts, err := kdb.DistinctCountByTime(`ip_address`, `%Y-%m-%d`, weekStart.AddDate(0, 0, -days))
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	for index := 0; index < 7; index++ {
		startTime := weekStart.Add(dayDur * time.Duration(index))
		weekDay := getWeekdayName(index)
		headTitles[index] = weekDay
		st := duckdb.DayStart(startTime)
		et := duckdb.DayEnd(startTime)
		//fmt.Printf(`%s: %s - %s`+"\n", weekDay, st.Format(time.DateTime), et.Format(time.DateTime))
		lastWeekStarTime, lastWeekEndTime := st.AddDate(0, 0, -days), et.AddDate(0, 0, -days)
		var lastWeekDayPV int64
		var currWeekDayPV int64
		for _, count := range counts {
			countTime, err := count.ParseTime(`date`, `2006-01-02`)
			if err != nil {
				render.Render(w, r, ErrInternalServerError(err))
				return
			}
			if !countTime.Before(lastWeekStarTime) && !countTime.After(lastWeekEndTime) {
				lastWeekDayPV += count.Count
			} else if !countTime.Before(st) && !countTime.After(et) {
				currWeekDayPV += count.Count
			}
		}
		datasMapUV.SetDatasMap(index, `上周`, lastWeekDayPV, days, historyItemStyleOpts, func(bd *opts.BarData) { bd.Name = `上周` + headTitles[index] })
		datasMapUV.SetDatasMap(index, `本周`, currWeekDayPV, days, func(bd *opts.BarData) { bd.Name = `本周` + headTitles[index] })

		timeRanges = append(timeRanges, duckdb.TimeRange{
			StartTime: st,
			EndTime:   et,
		})
	}
	chartUV := chartutil.NewBar(nil, []charts.GlobalOpts{chartutil.Title(`Vistors`, `一周访客量`)}, headTitles, func(b *charts.Bar) {
		datasMapUV.AddSeries(b)
	})

	// == 一周访问量 ==

	datasMapPV := chartutil.BarDatasMap{}
	counts, err = kdb.TotalByTime(`%Y-%m-%d`, timeRanges[0].StartTime.AddDate(0, 0, -days))
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	for index, timeRange := range timeRanges {
		lastWeekStarTime, lastWeekEndTime := timeRange.StartTime.AddDate(0, 0, -days), timeRange.EndTime.AddDate(0, 0, -days)
		var lastWeekDayPV int64
		var currWeekDayPV int64
		for _, count := range counts {
			countTime, err := count.ParseTime(`date`, `2006-01-02`)
			if err != nil {
				render.Render(w, r, ErrInternalServerError(err))
				return
			}
			if !countTime.Before(lastWeekStarTime) && !countTime.After(lastWeekEndTime) {
				lastWeekDayPV += count.Count
			} else if !countTime.Before(timeRange.StartTime) && !countTime.After(timeRange.EndTime) {
				currWeekDayPV += count.Count
			}
		}
		datasMapPV.SetDatasMap(index, `上周`, lastWeekDayPV, days, historyItemStyleOpts, func(bd *opts.BarData) { bd.Name = `上周` + headTitles[index] })
		datasMapPV.SetDatasMap(index, `本周`, currWeekDayPV, days, func(bd *opts.BarData) { bd.Name = `本周` + headTitles[index] })
	}
	chartVisits := chartutil.NewBar(nil, []charts.GlobalOpts{chartutil.Title(`Visits`, `一周访问量`)}, headTitles, func(b *charts.Bar) {
		datasMapPV.AddSeries(b)
	})

	// == 24小时访问量 ==

	datasMapPV = chartutil.BarDatasMap{}
	hours := 24
	headTitles = make([]string, hours)
	today := duckdb.DayStart(now)
	counts, err = kdb.TotalByTime(`%d %H`, today.Add(-dayDur))
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	for i := 0; i < hours; i++ {
		startTime := time.Date(today.Year(), today.Month(), today.Day(), i, 0, 0, 0, today.Location())
		endTime := time.Date(today.Year(), today.Month(), today.Day(), i, 59, 59, 999999999, today.Location())

		headTitles[i] = startTime.Format(`15:04`)
		var yesterdayHourPV int64
		var todayHourPV int64
		yesterdayStartTime := startTime.AddDate(0, 0, -1)
		yesterdayEndTime := endTime.AddDate(0, 0, -1)
		for _, count := range counts {
			hour := count.Extra[`hour`].(int)
			if count.Extra[`day`].(int) != startTime.Day() {
				if hour >= yesterdayStartTime.Hour() && hour <= yesterdayEndTime.Hour() {
					yesterdayHourPV += count.Count
				}
			} else {
				if hour >= startTime.Hour() && hour <= endTime.Hour() {
					todayHourPV += count.Count
				}
			}
		}
		datasMapPV.SetDatasMap(i, `昨天`, yesterdayHourPV, hours, historyItemStyleOpts, func(bd *opts.BarData) { bd.Name = `昨天` + headTitles[i] })
		datasMapPV.SetDatasMap(i, `今天`, todayHourPV, hours, func(bd *opts.BarData) { bd.Name = `今天` + headTitles[i] })
	}
	chartVisits24Hours := chartutil.NewBar(nil, []charts.GlobalOpts{chartutil.Title(`24 Hours Visits`, `最近24小时访问量`)}, headTitles, func(b *charts.Bar) {
		datasMapPV.AddSeries(b)
	})

	// == 7天每个地区访问量 ==

	geoDatasMap := chartutil.GeoDatasMap{}
	items, err := kdb.TopCount(`location`, 1000, now.AddDate(0, 0, -days))
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	size := len(items)
	for index, item := range items {
		geoDatasMap.SetDatasMap(index, item.Key, com.Float64(item.Extra[`longtitude`]), com.Float64(item.Extra[`latitude`]), float64(item.Value), size)
	}
	chartGeo := chartutil.NewGeo(nil, []charts.GlobalOpts{chartutil.Title(`Map`, `地图`),
		charts.WithGeoComponentOpts(opts.GeoComponent{
			Map: "world",
			//ItemStyle: &opts.ItemStyle{Color: "#006666"},
		}),
		charts.WithVisualMapOpts(opts.VisualMap{
			Calculable: opts.Bool(true),
			InRange: &opts.VisualMapInRange{
				Color: []string{"#50a3ba", "#eac736", "#d94e5d"},
			},
			Text: []string{"High", "Low"},
		})}, func(b *charts.Geo) {
		b.SetSeriesOptions(charts.WithLabelOpts(opts.Label{Show: opts.Bool(true)}))
		geoDatasMap.AddSeries(b)
	})

	// == 组合图 ==

	page := components.NewPage()
	page.AddCharts(
		chartUV,
		chartVisits,
		chartVisits24Hours,
		chartGeo,
	)
	page.Render(w)
}
