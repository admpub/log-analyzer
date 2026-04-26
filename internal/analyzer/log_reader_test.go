package analyzer

import (
	"context"
	"encoding/json"
	"log-analyzer/internal/model"
	"log-analyzer/internal/output"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateDateDirs(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		MinStartTime: time.Now().AddDate(0, 0, -1),
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	dirs := d.generateDateDirs()
	b, _ := json.MarshalIndent(dirs, ``, `  `)
	t.Log(string(b))
}

// CGO_ENABLED=1 go test -v --count=1 -run "^TestQueryHttpdLog$"
func TestQueryHttpdLog(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		LogFile:   `/home/swh/下载/access_local.log`,
		LogFormat: `combinedCHD`,
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	err = d.InstallHttpdLogModule()
	require.NoError(t, err)
	startTime := time.Now().Add(-24 * 3 * time.Hour)
	results, err := d.QueryLogFile(context.Background(), QueryOptions{
		Columns: d.RewriteHttpLogColumnsWithGeoIP(),
		Limit:   3,
		Where:   whereStartTime(startTime, true),
	})
	require.NoError(t, err)
	t.Log(startTime.Format(`2006-01-02 15:04:05.000000000-07:00`))
	b, _ := json.MarshalIndent(results, ``, `  `)
	t.Log(string(b))
}

func TestConvertToParquet(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		LogFile:     `/home/swh/下载/access_local.log`,
		LogFormat:   `combinedCHD`,
		GeoIPDBPath: `/home/swh/下载/GeoLite2-City.mmdb`,
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	err = d.InstallHttpdLogModule()
	require.NoError(t, err)
	err = d.CovertLogFileToParquet(context.Background(), time.Time{}, `../../data/logs/output.parquet`)
	require.NoError(t, err)
}

func TestNewestTimestamp(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		LogDirectory: `../../data/logs`,
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	ts, err := d.GetNewestTimestamp(context.Background())
	require.NoError(t, err)
	t.Log(ts)
}

func TestConvertToPartitionParquet(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		LogFile:          `/home/swh/下载/access_local.log`,
		LogFormat:        `combinedCHD`,
		GeoIPDBPath:      `/home/swh/下载/GeoLite2-City.mmdb`,
		OverwriteParquet: true,
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	err = d.InstallHttpdLogModule()
	require.NoError(t, err)
	err = d.CovertLogFileToParquetAndPartition(context.Background(), time.Time{}, `../../data/logs/partition`)
	require.NoError(t, err)
}

func TestQueryParquet(t *testing.T) {
	d, err := NewAnalyzer(&Config{
		LogFile:     `../../data/logs/partition`,
		LogFormat:   `combinedCHD`,
		LogParquet:  true,
		GeoIPDBPath: `/home/swh/下载/GeoLite2-City.mmdb`,
	}, zerolog.New(os.Stdout))
	assert.NoError(t, err)
	defer d.Close()
	err = d.InstallHttpdLogModule()
	require.NoError(t, err)
	results, err := d.QueryLogFile(context.Background(), QueryOptions{
		//Columns: `NOW(),timestamp,NOW() - INTERVAL 5 HOUR`,
		Limit:   2,
		OrderBy: `timestamp DESC`,
	})
	require.NoError(t, err)
	b, _ := json.MarshalIndent(results, ``, `  `)
	t.Log(string(b))
	output.Table(`List`, results)
	/*//
	SELECT
			remote_addr,
			status,
			method,
			path,
			count(*) as cnt
		FROM read_httpd_log('./access.log')
		WHERE status >= 400
		GROUP BY remote_addr, status, method, path
		ORDER BY cnt DESC
		LIMIT 10
	//*/
	results, err = d.QueryLogFile(context.Background(), QueryOptions{
		Columns: `remote_addr,
			status,
			method,
			path,
			count(*) as cnt`,
		Where:   `status >= 400`,
		GroupBy: `remote_addr, status, method, path`,
		OrderBy: `cnt DESC`,
		Limit:   2,
	})
	require.NoError(t, err)
	output.Table(`status >= 400`, results)

	result, err := d.GetHourlyStats(context.Background(), 5)
	require.NoError(t, err)
	t.Logf(`%+v`, result)

	resultSD, err := d.GetStatusDistribution(context.Background())
	require.NoError(t, err)
	t.Logf(`%+v`, resultSD)

	resultPath, err := d.GetTopPaths(context.Background(), 5, 5)
	require.NoError(t, err)
	t.Logf(`%+v`, resultPath)

	resultTS, err := d.GetRealTimeStats(context.Background(), 24)
	require.NoError(t, err)
	t.Logf(`%+v`, resultTS)

	resultD, err := d.GetSlowPaths(context.Background(), 5, 24, 1)
	require.NoError(t, err)
	t.Logf(`%+v`, resultD)

	resultUVT, err := d.GetUVDistribution(context.Background(), 1)
	require.NoError(t, err)
	t.Logf(`%+v`, resultUVT)

	t.SkipNow()

	resultSI, err := d.GetSuspiciousIPs(context.Background(), 5)
	require.NoError(t, err)
	t.Logf(`%+v`, resultSI)

	pad := &model.PathAnalysisDetail{}
	err = d.getCorrelationAnalysis(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getSlowRequests(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getUserAgents(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getTopClients(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getHourlyStats(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getStatusDistribution(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
	err = d.getBasicStats(context.Background(), pad, `/`, 24)
	require.NoError(t, err)
	t.Logf(`%+v`, pad)
}
