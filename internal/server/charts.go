package server

import (
	"strconv"
	"time"

	"github.com/admpub/log-analyzer/pkg/chartutil"
	"github.com/admpub/log-analyzer/pkg/storage/duckdb"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

func heatMapCalendar(kdb duckdb.Storager, isUV bool) (*charts.HeatMap, error) {
	end := time.Now()
	start := end.Add(time.Duration(-366) * time.Hour * 24)
	var counts []duckdb.CountItem
	var err error
	var title string
	var titleEn string
	if isUV {
		title = `访客量`
		titleEn = `Visitors`
		counts, err = kdb.DistinctCountByTime(`ip_address`, `%Y-%m-%d`, start, end)
	} else {
		title = `访问量`
		titleEn = `Visits`
		counts, err = kdb.TotalByTime(`%Y-%m-%d`, start, end)
	}
	if err != nil {
		return nil, err
	}
	return chartutil.NewHeatMapCalendar(nil, []charts.GlobalOpts{chartutil.Title(titleEn, title)}, title, start, end, func(start, end time.Time) int64 {
		var n int64
		for _, count := range counts {
			countTime, err := count.ParseTime(`date`, `2006-01-02`)
			if err != nil {
				return 0
			}
			if countTime.Before(start) {
				continue
			}
			if countTime.After(end) {
				break
			}
			n += count.Count
		}
		return n
	}), nil
}

func daysBar(kdb duckdb.Storager, timeRanges []duckdb.TimeRange, historyItemStyleOpts func(bd *opts.BarData), headTitles []string, isUV bool) (*charts.Bar, error) {
	datasMap := chartutil.NewBarDatas()
	days := len(timeRanges)
	var counts []duckdb.CountItem
	var err error
	var title string
	var titleEn string
	var timeTitle string
	if days == 7 {
		timeTitle = `一周`
	} else {
		timeTitle = strconv.Itoa(days) + `天`
	}
	if isUV {
		title = `访客量`
		titleEn = `Visitors`
		counts, err = kdb.DistinctCountByTime(`ip_address`, `%Y-%m-%d`, timeRanges[0].StartTime.AddDate(0, 0, -days))
	} else {
		title = `访问量`
		titleEn = `Visits`
		counts, err = kdb.TotalByTime(`%Y-%m-%d`, timeRanges[0].StartTime.AddDate(0, 0, -days))
	}
	if err != nil {
		return nil, err
	}
	for index, timeRange := range timeRanges {
		lastWeekStartTime, lastWeekEndTime := timeRange.StartTime.AddDate(0, 0, -days), timeRange.EndTime.AddDate(0, 0, -days)
		var lastWeekDayPV int64
		var currWeekDayPV int64
		for _, count := range counts {
			countTime, err := count.ParseTime(`date`, `2006-01-02`)
			if err != nil {
				return nil, err
			}
			if !countTime.Before(lastWeekStartTime) && !countTime.After(lastWeekEndTime) {
				lastWeekDayPV += count.Count
			} else if !countTime.Before(timeRange.StartTime) && !countTime.After(timeRange.EndTime) {
				currWeekDayPV += count.Count
			}
		}
		datasMap.SetDatasMap(index, `上周`, lastWeekDayPV, days, historyItemStyleOpts, func(bd *opts.BarData) { bd.Name = `上周` + headTitles[index] })
		datasMap.SetDatasMap(index, `本周`, currWeekDayPV, days, func(bd *opts.BarData) { bd.Name = `本周` + headTitles[index] })
	}
	return chartutil.NewBar(nil, []charts.GlobalOpts{chartutil.Title(titleEn, timeTitle+title)}, headTitles, func(b *charts.Bar) {
		datasMap.AddSeries(b)
	}), nil
}

func hoursBar(kdb duckdb.Storager, timeRanges []duckdb.TimeRange, historyItemStyleOpts func(bd *opts.BarData), headTitles []string, isUV bool) (*charts.Bar, error) {
	datasMap := chartutil.NewBarDatas()
	hours := len(timeRanges)
	hoursDur := time.Hour * time.Duration(hours)
	var counts []duckdb.CountItem
	var err error
	var title string
	var titleEn string
	timeTitle := `最近` + strconv.Itoa(hours) + `小时`
	timeTitleEn := strconv.Itoa(hours) + ` Hours`
	if isUV {
		title = timeTitle + `访客量`
		titleEn = timeTitleEn + ` Visitors`
		counts, err = kdb.DistinctCountByTime(`ip_address`, `%d %H`, timeRanges[0].StartTime.Add(-hoursDur))
	} else {
		title = timeTitle + `访问量`
		titleEn = timeTitleEn + ` Visits`
		counts, err = kdb.TotalByTime(`%d %H`, timeRanges[0].StartTime.Add(-hoursDur))
	}
	if err != nil {
		return nil, err
	}
	for index, timeRange := range timeRanges {
		var yesterdayHourPV int64
		var todayHourPV int64
		yesterdayStartTime := timeRange.StartTime.AddDate(0, 0, -1)
		yesterdayEndTime := timeRange.EndTime.AddDate(0, 0, -1)
		for _, count := range counts {
			hour := count.Extra[`hour`].(int)
			if count.Extra[`day`].(int) != timeRange.StartTime.Day() {
				if hour >= yesterdayStartTime.Hour() && hour <= yesterdayEndTime.Hour() {
					yesterdayHourPV += count.Count
				}
			} else {
				if hour >= timeRange.StartTime.Hour() && hour <= timeRange.EndTime.Hour() {
					todayHourPV += count.Count
				}
			}
		}
		datasMap.SetDatasMap(index, `昨天`, yesterdayHourPV, hours, historyItemStyleOpts, func(bd *opts.BarData) { bd.Name = `昨天` + headTitles[index] })
		datasMap.SetDatasMap(index, `今天`, todayHourPV, hours, func(bd *opts.BarData) { bd.Name = `今天` + headTitles[index] })
	}
	return chartutil.NewBar(nil, []charts.GlobalOpts{chartutil.Title(titleEn, title)}, headTitles, func(b *charts.Bar) {
		datasMap.AddSeries(b)
	}), nil
}
