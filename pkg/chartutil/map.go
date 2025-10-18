package chartutil

import (
	"io"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
)

func NewMap(w io.Writer, options []charts.GlobalOpts, addSeries func(*charts.Map)) *charts.Map {
	mp := charts.NewMap()
	mp.RegisterMapType("china")
	options = append(options, Initialization(``, ``))
	mp.SetGlobalOptions(options...)
	if addSeries != nil {
		addSeries(mp)
	}
	if w != nil {
		mp.Render(w)
	}
	return mp
}

func NewGeo(w io.Writer, options []charts.GlobalOpts, addSeries func(*charts.Geo)) *charts.Geo {
	geo := charts.NewGeo()
	options = append(options, Initialization(``, ``))
	geo.SetGlobalOptions(options...)
	if addSeries != nil {
		addSeries(geo)
	}
	if w != nil {
		geo.Render(w)
	}
	return geo
}

type MapDatasMap map[string][]opts.MapData

func (b *MapDatasMap) SetDatasMap(index int, key string, value interface{}, size int) {
	datas, ok := (*b)[`map`]
	if !ok {
		datas = make([]opts.MapData, size)
		(*b)[`map`] = datas
	}
	datas[index] = opts.MapData{
		Name:  key,
		Value: value,
	}
}

func (b MapDatasMap) AddSeries(bar *charts.Map) {
	for key, val := range b {
		bar.AddSeries(key, val)
	}
}

type GeoDatasMap map[string][]opts.GeoData

func (b *GeoDatasMap) SetDatasMap(index int, key string, longtitude float64, latitude float64, value float64, size int) {
	datas, ok := (*b)[`geo`]
	if !ok {
		datas = make([]opts.GeoData, size)
		(*b)[`geo`] = datas
	}
	datas[index] = opts.GeoData{
		Name:  key,
		Value: []float64{longtitude, latitude, value},
	}
}

func (b GeoDatasMap) AddSeries(bar *charts.Geo) {
	//options := charts.WithRippleEffectOpts(opts.RippleEffect{Period: 4, Scale: 6, BrushType: "stroke"})
	//options := charts.WithLabelOpts(opts.Label{Show: opts.Bool(true),})
	for key, val := range b {
		//bar.AddSeries(key, types.ChartEffectScatter, val, options)
		bar.AddSeries(key, types.ChartScatter, val)
	}
}
