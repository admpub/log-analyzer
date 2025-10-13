package storage

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/webx-top/com"
)

func (e *storageDuckDB) TopInteger(key string, limit int, startAndEndTime ...time.Time) ([]map[string]any, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(`timestamp`, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	r, err := e.db.Query(`SELECT ` + dbField + ` AS value, COUNT(` + dbField + `) AS num FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY TRY_CAST(` + dbField + ` AS BIGINT) DESC LIMIT ` + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []map[string]any
	for r.Next() {
		var value sql.NullString
		var num sql.NullInt64
		err = r.Scan(&value, &num)
		if err != nil {
			return nil, err
		}
		results = append(results, map[string]any{
			value.String: num.Int64,
		})
	}
	return results, err
}

func (e *storageDuckDB) TopFloat(key string, limit int, startAndEndTime ...time.Time) ([]map[string]any, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(`timestamp`, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	r, err := e.db.Query(`SELECT ` + dbField + ` AS value, COUNT(` + dbField + `) AS num FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY TRY_CAST(` + dbField + ` AS DOUBLE) DESC LIMIT ` + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []map[string]any
	for r.Next() {
		var value sql.NullString
		var num sql.NullFloat64
		err = r.Scan(&value, &num)
		if err != nil {
			return nil, err
		}
		results = append(results, map[string]any{
			value.String: num.Float64,
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

func (e *storageDuckDB) TopCount(key string, limit int, startAndEndTime ...time.Time) ([]map[string]any, error) {
	return e.topCount(key, limit, false, startAndEndTime...)
}

func (e *storageDuckDB) TopCountWithUV(key string, limit int, startAndEndTime ...time.Time) ([]map[string]any, error) {
	return e.topCount(key, limit, true, startAndEndTime...)
}

func (e *storageDuckDB) topCount(key string, limit int, withUV bool, startAndEndTime ...time.Time) ([]map[string]any, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(`timestamp`, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
	selectField := dbField + ` AS value, COUNT(` + dbField + `) AS num`
	if withUV {
		selectField += `, COUNT(DISTINCT Params['ip_address']) AS uv`
	}
	r, err := e.db.Query(`SELECT ` + selectField + ` FROM ` + tableName + where + ` GROUP BY ` + dbField + ` ORDER BY COUNT(` + dbField + `) DESC LIMIT ` + strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []map[string]any
	for r.Next() {
		var value sql.NullString
		var num sql.NullInt64
		var mp map[string]any
		if withUV {
			var uv sql.NullInt64
			err = r.Scan(&value, &num, &uv)
			mp = map[string]any{
				value.String: num.Int64,
				`uv`:         uv.Int64,
			}
		} else {
			err = r.Scan(&value, &num)
			mp = map[string]any{
				value.String: num.Int64,
			}
		}
		if err != nil {
			return nil, err
		}
		results = append(results, mp)
	}
	return results, err
}

func (e *storageDuckDB) DistinctCount(key string, startAndEndTime ...time.Time) (int64, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	where := makeTimeRangeCondition(`timestamp`, startAndEndTime...)
	if len(where) > 0 {
		where = ` WHERE ` + where
	}
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

func (e *storageDuckDB) Sum(key string, startAndEndTime ...time.Time) (int64, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `']`
	timeWhere := makeTimeRangeCondition(`timestamp`, startAndEndTime...)
	if len(timeWhere) > 0 {
		timeWhere = ` AND ` + timeWhere
	}
	where := `TRY_CAST(` + dbField + ` AS BIGINT)>0 ` + timeWhere
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
