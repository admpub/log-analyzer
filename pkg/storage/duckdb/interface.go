package duckdb

import (
	"time"

	"github.com/admpub/log-analyzer/pkg/storage"
)

type Storager interface {
	storage.Storager
	ListMaps(limit int) ([]map[string]interface{}, error)
	Total(startAndEndTime ...time.Time) (int64, error)
	TotalByTime(timeFormat string, startAndEndTime ...time.Time) ([]CountItem, error)
	TopInteger(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error)
	TopFloat(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[float64], error)
	TopCount(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error)
	TopCountWithUV(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error)
	DistinctCount(key string, startAndEndTime ...time.Time) (int64, error)
	DistinctCountByTime(key string, timeFormat string, startAndEndTime ...time.Time) ([]CountItem, error)
	Sum(key string, startAndEndTime ...time.Time) (int64, error)
}
