package storage

import (
	"database/sql"
	"strconv"

	"github.com/webx-top/com"
)

func (e *storageDuckDB) Top(key string, limit int) ([]map[string]any, error) {
	safeKey := com.AddSlashes(key)
	dbField := `Params['` + safeKey + `'][1]`
	r, err := e.db.Query(`SELECT ` + dbField + ` AS value, COUNT(` + dbField + `) AS num FROM ` + tableName + ` GROUP BY ` + dbField + ` ORDER BY TRY_CAST(` + dbField + ` AS BIGINT) LIMIT ` + strconv.Itoa(limit))
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
