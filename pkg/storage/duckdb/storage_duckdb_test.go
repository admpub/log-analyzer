package storage

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/admpub/log-analyzer/pkg/extraction"
	"github.com/admpub/pp"
	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	a, err := newDuckDB(nil)
	assert.NoError(t, err)
	a.Close()
	u, err := url.Parse(`duckdb://./eee`)
	assert.NoError(t, err)
	assert.Equal(t, `.`, u.Host)
	assert.Equal(t, `/eee`, u.Path)
}

func TestAppend(t *testing.T) {
	a, err := newDuckDB(nil)
	assert.NoError(t, err)
	data := extraction.Extraction{
		Params: map[string]extraction.Param{
			"int_bytes": {
				Value: 203023,
				Type:  "int",
			},
			"ip_address": {
				Value: "192.168.9.216",
				Type:  "ip",
			},
			"method": {
				Value: "GET",
				Type:  "str",
			},
			"path": {
				Value: "/presentations/logstash-monitorama-2013/images/kibana-search.png",
				Type:  "str",
			},
			"status": {
				Value: 200,
				Type:  "int",
			},
			"timestamp": {
				Value: "2015-05-17T10:05:03Z",
				Type:  "time",
			},
			"url": {
				Value: "http://semicomplete.com/presentations/logstash-monitorama-2013/",
				Type:  "str",
			},
			"user_agent": {
				Value: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36",
				Type:  "str",
			},
		},
		Pattern:    "ip_address - - [timestamp] \"method path *\" status int_bytes \"url\" \"user_agent\"",
		LineNumber: 0,
		Line:       "192.168.9.216 - - [17/May/2015:10:05:03 +0000] \"GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1\" 200 203023 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"",
	}
	err = a.Append(data)
	assert.NoError(t, err)
	lines := a.GetLastLines(1)
	assert.Equal(t, 1, len(lines))
	pp.Println(lines)
	err = a.Update(data)
	assert.NoError(t, err)
	list, err := a.List(100)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(list))
	pp.Println(list)

	r, err := a.(*storageDuckDB).db.Query(`SELECT SUM(TRY_CAST(Params['int_bytes'][1] AS BIGINT)) FROM ` + tableName)
	//r, err := a.(*storageDuckDB).db.Query(`SELECT Params['int_bytes'][1] FROM ` + tableName)
	assert.NoError(t, err)
	defer r.Close()
	var sum sql.NullInt64
	for r.Next() {
		err = r.Scan(&sum)
		if err != nil {
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, sql.NullInt64{
		Int64: 203023,
		Valid: true,
	}, sum)
	pp.Println(sum)

	r, err = a.(*storageDuckDB).db.Query(`SELECT SUM(TRY_CAST(Params['int_bytes'][1] AS BIGINT)) FROM ` + tableName + ` WHERE Params['int_bytes'][1]='203023'`)
	assert.NoError(t, err)
	defer r.Close()
	var sum2 sql.NullInt64
	for r.Next() {
		err = r.Scan(&sum2)
		if err != nil {
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, sql.NullInt64{
		Int64: 203023,
		Valid: true,
	}, sum2)

	top, err := a.(*storageDuckDB).Top(`int_bytes`, 100)
	assert.NoError(t, err)
	assert.Equal(t, []map[string]any{
		{`203023`: int64(1)},
	}, top)
	a.Close()
}
