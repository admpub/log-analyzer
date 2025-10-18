package server

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/admpub/log-analyzer/pkg/chartutil"
	"github.com/admpub/log-analyzer/pkg/parse"
	"github.com/admpub/log-analyzer/pkg/storage/duckdb"
	"github.com/coscms/tables"
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
	timeRanges := make([]duckdb.TimeRange, 0, days)
	weekdayN := int(now.Weekday())
	weekStart := duckdb.DayStart(now.Add(dayDur * -time.Duration(weekdayN)))
	for index := 0; index < 7; index++ {
		startTime := weekStart.Add(dayDur * time.Duration(index))
		weekDay := getWeekdayName(index)
		headTitles[index] = weekDay
		st := duckdb.DayStart(startTime)
		et := duckdb.DayEnd(startTime)
		timeRanges = append(timeRanges, duckdb.TimeRange{
			StartTime: st,
			EndTime:   et,
		})
	}
	var chartUV *charts.Bar
	chartUV, err = daysBar(kdb, timeRanges, historyItemStyleOpts, headTitles, true)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	// == 一周访问量 ==

	var chartVisits *charts.Bar
	chartVisits, err = daysBar(kdb, timeRanges, historyItemStyleOpts, headTitles, false)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	// == 24小时访问量 ==

	hours := 24
	headTitles = make([]string, hours)
	today := duckdb.DayStart(now)
	timeRanges = make([]duckdb.TimeRange, 0, hours)
	for i := 0; i < hours; i++ {
		startTime := time.Date(today.Year(), today.Month(), today.Day(), i, 0, 0, 0, today.Location())
		endTime := time.Date(today.Year(), today.Month(), today.Day(), i, 59, 59, 999999999, today.Location())
		headTitles[i] = startTime.Format(`15:04`)
		timeRanges = append(timeRanges, duckdb.TimeRange{
			StartTime: startTime,
			EndTime:   endTime,
		})
	}
	var chartUV24Hours *charts.Bar
	chartUV24Hours, err = hoursBar(kdb, timeRanges, historyItemStyleOpts, headTitles, true)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	var chartVisits24Hours *charts.Bar
	chartVisits24Hours, err = hoursBar(kdb, timeRanges, historyItemStyleOpts, headTitles, false)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}

	// == 7天每个地区访问量 ==

	geoDatasMap := chartutil.GeoDatasMap{}
	items, err := kdb.TopCount(`location`, 1000, now.AddDate(0, 0, -days))
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	size := len(items)
	var maxValue int64
	for index, item := range items {
		maxValue = max(maxValue, item.Value)
		geoDatasMap.SetDatasMap(index, item.Key, com.Float64(item.Extra[`longtitude`]), com.Float64(item.Extra[`latitude`]), float64(item.Value), size)
	}
	chartGeo := chartutil.NewGeo(nil, []charts.GlobalOpts{chartutil.Title(`Last week visits`, `最近一周访问量分布图`),
		charts.WithGeoComponentOpts(opts.GeoComponent{
			Map: "world",
			//ItemStyle: &opts.ItemStyle{Color: "#006666"},
		}),
		charts.WithVisualMapOpts(opts.VisualMap{
			//Calculable: opts.Bool(true),
			InRange: &opts.VisualMapInRange{
				Color: chartutil.InRangeColor,
			},
			Max:  float32(maxValue),
			Text: []string{"High", "Low"},
		})}, func(b *charts.Geo) {
		b.SetSeriesOptions(charts.WithLabelOpts(opts.Label{Show: opts.Bool(true)}))
		geoDatasMap.AddSeries(b)
	})

	var chartHeatMapUV *charts.HeatMap
	chartHeatMapUV, err = heatMapCalendar(kdb, true)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	var chartHeatMapPV *charts.HeatMap
	chartHeatMapPV, err = heatMapCalendar(kdb, false)
	if err != nil {
		render.Render(w, r, ErrInternalServerError(err))
		return
	}
	// == 组合图 ==

	page := components.NewPage()
	page.SetLayout(components.PageFlexLayout)
	page.AddCharts(
		chartHeatMapUV, chartHeatMapPV,
		chartUV, chartVisits,
		chartUV24Hours, chartVisits24Hours,
		chartGeo,
	)
	var rows []duckdb.AnalyzeItem[int64]
	rows, _ = kdb.TopCountWithUV(`path`, 10, true)
	if err != nil {
		return
	}
	table := tables.New()
	table.SetCaptionContent(`热门页面`)
	table.Head.AddRow(new(tables.Row).AddCell(tables.NewCell(`路径`), tables.NewCell(`访问量`), tables.NewCell(`UV`)))
	for _, row := range rows {
		table.Body.AddRow(new(tables.Row).AddCell(tables.NewCell(row.Key), tables.NewCell(row.Value), tables.NewCell(row.UV)))
	}
	buf := bytes.NewBuffer(nil)
	page.Render(buf)
	w.Write(bodyAndLastDiv.ReplaceAll(buf.Bytes(), []byte(tableStyle+`<div class="container"><div class="item" style="width:900px">`+string(table.Render())+`</div></div> </div></body></html>`)))
}

var bodyAndLastDiv = regexp.MustCompile(`</div>\s*</body>\s*</html>\s*$`)
var tableStyle = `<style>
table {border-collapse: collapse;background-color: #f2f2f2;width: 100%;margin: auto;box-shadow: 1px 1px 5px rgba(0,0,0,0.3);}
table caption{color: #516b91; font-weight: bold}
th, td {border: 1px solid #ccc;text-align: left;padding: 8px;}
th {background-color: #516b91;color: white;}
tr:nth-child(odd) {background-color: #f2f2f2;}
tr:nth-child(even) {background-color: #fff;}
@media screen and (max-width: 600px) {
table {display: block;overflow-x: auto;}
th, td {display: block;width: 100%;}
}
</style>`
