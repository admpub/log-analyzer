package chartutil

import (
	"io"
	"time"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

var InRangeColor = []string{"#50a3ba", "#eac736", "#d94e5d"}
var InRangeColor2 = []string{"#68a0f3ff", "#5f8ed5ff", "#537bb8ff", "#466798ff", "#31496dff", "#172740ff"}

func NewHeatMapCalendar(w io.Writer, options []charts.GlobalOpts, label string, start time.Time, end time.Time, genValue func(start time.Time, end time.Time) int64) *charts.HeatMap {
	hm := charts.NewHeatMap()
	if end.IsZero() {
		end = time.Now()
	}
	if start.IsZero() {
		start = end.Add(time.Duration(-366) * time.Hour * 24)
	}
	data, maxValue := genHeatMapCalendarData(start, end, genValue)
	visualMapOpt := opts.VisualMap{
		Top: `65`, Left: `0`,
		Min: 0,
		Max: float32(maxValue),
		//Calculable: opts.Bool(true),
		InRange: &opts.VisualMapInRange{
			Color: InRangeColor2,
		},
	}
	options = append(options, charts.WithVisualMapOpts(visualMapOpt))
	options = append(options, Initialization(``, ``, func(i *opts.Initialization) {
		i.Height = `255px`
	}))
	hm.SetGlobalOptions(options...)

	calendarOpts := &opts.Calendar{
		Top:      "80",
		Left:     "100",
		Right:    "30",
		CellSize: "20",
		ItemStyle: &opts.ItemStyle{
			BorderWidth: 0.5,
		},
		Orient: "horizontal",
	}
	calendarOpts.Range = append(calendarOpts.Range, start.Format(time.DateOnly), end.Format(time.DateOnly))

	hm.AddCalendar(calendarOpts).AddSeries(label, data, charts.WithCoordinateSystem("calendar"))

	if w != nil {
		hm.Render(w)
	}
	return hm
}

func genHeatMapCalendarData(start time.Time, end time.Time, genValue func(time.Time, time.Time) int64) ([]opts.HeatMapData, int64) {
	var items []opts.HeatMapData
	start = DayStart(start)
	end = DayEnd(end)
	var maxValue int64
	for dt := start; dt.Before(end); dt = dt.AddDate(0, 0, 1) {
		value := genValue(dt, DayEnd(dt))
		if value <= 0 {
			items = append(items, opts.HeatMapData{Value: [2]interface{}{dt.Format(time.DateOnly), "-"}})
		} else {
			items = append(items, opts.HeatMapData{Value: [2]interface{}{dt.Format(time.DateOnly), value}})
		}
		maxValue = max(maxValue, value)
	}
	return items, maxValue
}
