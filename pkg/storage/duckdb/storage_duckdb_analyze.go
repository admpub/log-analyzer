package duckdb

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/webx-top/com"
)

type AnalyzeItem[T comparable] struct {
	Key   string
	Value T
	UV    int64
	Extra map[string]any
}

type TimeRange struct {
	StartTime time.Time
	EndTime   time.Time
}

type CountItem struct {
	Count int64
	Extra map[string]any
}

type QueryClause struct {
	Key       string
	Limit     int
	WithUV    bool
	OrderByUV bool
	StartTime time.Time
	EndTime   time.Time
	Where     string
}

func (c *CountItem) ParseTime(key string, format string) (time.Time, error) {
	countTime, ok := c.Extra[key+`Raw`].(time.Time)
	if !ok {
		date := c.Extra[key].(string)
		var err error
		countTime, err = time.Parse(format, date)
		if err != nil {
			return countTime, err
		}
		c.Extra[key+`Raw`] = countTime
	}
	return countTime, nil
}

func (e *storageDuckDB) TopInteger(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	where = e.makeWhere(where)
	r, err := e.db.Query(`SELECT ` + dbField + ` AS value, COUNT(` + dbField + `) AS num FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY TRY_CAST(` + dbField + ` AS BIGINT) DESC LIMIT ` + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []AnalyzeItem[int64]
	for r.Next() {
		var value sql.NullString
		var num sql.NullInt64
		err = r.Scan(&value, &num)
		if err != nil {
			return nil, err
		}
		results = append(results, AnalyzeItem[int64]{
			Key:   value.String,
			Value: num.Int64,
		})
	}
	return results, err
}

func (e *storageDuckDB) TopFloat(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[float64], error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	where = e.makeWhere(where)
	r, err := e.db.Query(`SELECT ` + dbField + ` AS value, COUNT(` + dbField + `) AS num FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY TRY_CAST(` + dbField + ` AS DOUBLE) DESC LIMIT ` + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []AnalyzeItem[float64]
	for r.Next() {
		var value sql.NullString
		var num sql.NullFloat64
		err = r.Scan(&value, &num)
		if err != nil {
			return nil, err
		}
		results = append(results, AnalyzeItem[float64]{
			Key:   value.String,
			Value: num.Float64,
		})
	}
	return results, err
}

func makeTimeRangeCondition(timeKey string, startAndEndTime ...time.Time) string {
	if len(startAndEndTime) == 0 {
		return ``
	}
	var where string
	timeField := `Params['unix` + timeKey + `']`
	var startTime, endTime int64
	if len(startAndEndTime) > 0 {
		if !startAndEndTime[0].IsZero() {
			startTime = startAndEndTime[0].Unix()
		}
		if len(startAndEndTime) > 1 && !startAndEndTime[1].IsZero() {
			endTime = startAndEndTime[1].Unix()
		}
	} else {
		startTime = time.Now().AddDate(0, 0, -7).Unix()
		endTime = time.Now().Unix()
	}
	if startTime > 0 && endTime > 0 {
		where = `TRY_CAST(` + timeField + ` AS BIGINT) BETWEEN ` + strconv.FormatInt(startTime, 10) + ` AND ` + strconv.FormatInt(endTime, 10)
	} else if startTime > 0 {
		where = `TRY_CAST(` + timeField + ` AS BIGINT) >= ` + strconv.FormatInt(startTime, 10)
	} else if endTime > 0 {
		where = `TRY_CAST(` + timeField + ` AS BIGINT) <= ` + strconv.FormatInt(endTime, 10)
	}
	return where
}

func (e *storageDuckDB) TopCount(key string, limit int, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error) {
	return e.topCount(key, limit, false, false, startAndEndTime...)
}

func (e *storageDuckDB) TopCountWithUV(key string, limit int, orderByUV bool, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error) {
	return e.topCount(key, limit, true, orderByUV, startAndEndTime...)
}

func (e *storageDuckDB) topCount(key string, limit int, withUV bool, orderByUV bool, startAndEndTime ...time.Time) ([]AnalyzeItem[int64], error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	where = e.makeWhere(where)
	selectField := dbField + ` AS value, COUNT(` + dbField + `) AS num`
	selectField += `, ANY_VALUE(Params) AS Params`
	if withUV {
		selectField += `, COUNT(DISTINCT Params['` + e.nameOfIPAddressField + `']) AS uv`
	}
	orderBy := `COUNT(` + dbField + `)`
	if orderByUV {
		orderBy = `COUNT(DISTINCT Params['` + e.nameOfIPAddressField + `'])`
	}
	sqlStr := `SELECT ` + selectField + ` FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY ` + orderBy + ` DESC LIMIT ` + strconv.Itoa(limit)
	r, err := e.db.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []AnalyzeItem[int64]
	for r.Next() {
		var value sql.NullString
		var num sql.NullInt64
		var params duckdb.Map
		var mp AnalyzeItem[int64]
		if withUV {
			var uv sql.NullInt64
			err = r.Scan(&value, &num, &params, &uv)
			mp = AnalyzeItem[int64]{
				UV:    uv.Int64,
				Key:   value.String,
				Value: num.Int64,
			}
		} else {
			err = r.Scan(&value, &num, &params)
			mp = AnalyzeItem[int64]{
				Key:   value.String,
				Value: num.Int64,
			}
		}
		if err != nil {
			return nil, err
		}
		mp.Extra = map[string]any{}
		for k, v := range params {
			mp.Extra[k.(string)] = v
		}
		results = append(results, mp)
	}
	return results, err
}

func (e *storageDuckDB) DistinctCount(key string, startAndEndTime ...time.Time) (int64, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	where = e.makeWhere(where)
	r, err := e.db.Query(`SELECT COUNT(DISTINCT ` + dbField + `) AS num FROM ` + tableName + where)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	for r.Next() {
		var num sql.NullInt64
		err = r.Scan(&num)
		if err != nil {
			return 0, err
		}
		return num.Int64, err
	}
	return 0, err
}

func (e *storageDuckDB) DistinctCountByTime(key string, timeFormat string, startAndEndTime ...time.Time) ([]CountItem, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	where = e.makeWhere(where)
	timeField := `STRPTIME(Params['` + e.nameOfTimestampField + `'],'%Y-%m-%d %H:%M:%S %z %Z')`
	timeField = `CAST(` + timeField + ` AS TIMESTAMP)`
	timeFormatField := `STRFTIME(` + timeField + `, '` + timeFormat + `')`
	r, err := e.db.Query(`SELECT COUNT(DISTINCT ` + dbField + `) AS num,` + timeFormatField + ` AS tim FROM ` + tableName + where + ` GROUP BY ` + timeFormatField + ` ORDER BY tim ASC`)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	goTimeLayout := datetimeReplacer.Replace(timeFormat)
	var results []CountItem
	for r.Next() {
		var num sql.NullInt64
		var tim sql.NullString
		err = r.Scan(&num, &tim)
		if err != nil {
			return nil, err
		}
		//panic(hour.String)
		t, err := time.Parse(goTimeLayout, tim.String)
		if err != nil {
			return nil, err
		}
		extra := map[string]any{
			`date`: t.Format(time.DateOnly),
			`hour`: t.Hour(),
			`day`:  t.Day(),
		}
		results = append(results, CountItem{
			Count: num.Int64,
			Extra: extra,
		})
	}
	return results, err
}

func (e *storageDuckDB) Sum(key string, startAndEndTime ...time.Time) (int64, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	timeWhere := makeTimeRangeCondition(e.nameOfTimestampField, startAndEndTime...)
	if len(timeWhere) > 0 {
		timeWhere = ` AND ` + timeWhere
	}
	where := `TRY_CAST(` + dbField + ` AS BIGINT)>0 ` + timeWhere
	where = e.makeWhere(where)
	r, err := e.db.Query(`SELECT SUM(TRY_CAST(` + dbField + ` AS BIGINT)) AS num FROM ` + tableName + ` WHERE ` + where)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	for r.Next() {
		var num sql.NullInt64
		err = r.Scan(&num)
		return num.Int64, err
	}
	return 0, err
}
