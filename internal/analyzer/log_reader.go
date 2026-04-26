package analyzer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (a *LogAnalyzer) InstallHttpdLogModule() error {
	var err error
	for _, query := range []string{`INSTALL httpd_log FROM community;`, `INSTALL httpd_log FROM 'httpd_log.duckdb';`} {
		_, err = a.db.Exec(query)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	_, err = a.db.Exec(`LOAD httpd_log;`)
	return err
}

func (a *LogAnalyzer) SetLogFile(logFile string, format ...string) {
	a.logFile = logFile
	if len(format) > 0 && len(format[0]) > 0 {
		a.logFormat = format[0]
	}
	if strings.HasSuffix(a.logFile, `.parquet`) && !a.logParquet {
		a.logParquet = true
	}
}

func (a *LogAnalyzer) SetLogParquet(logParquet bool) {
	a.logParquet = logParquet
}

type QueryOptions struct {
	Columns     string
	Where       string
	GroupBy     string
	OrderBy     string
	NamedArgs   map[string]any
	Limit       int
	WithColumns string
}

func rewriteHttpLogColumns(logFormat string) string {
	var pathAndQueryString string
	if logFormat == `combinedCHD` {
		pathAndQueryString = `split_part(path, '?', 2) AS query_string, split_part(path, '?', 1) AS path, `
	} else {
		pathAndQueryString = `query_string, path, `
	}
	columns := `client_host AS remote_addr, 
timestamp, 
date_part('year', timestamp) AS year, 
date_part('month', timestamp) AS month, 
date_part('day', timestamp) AS day, 
date_part('hour', timestamp) AS hour,
method, 
` + pathAndQueryString + `
protocol, 
status, 
bytes AS body_bytes_sent, 
referer AS http_referer, 
user_agent AS http_user_agent, 
date_part('us', duration)/1000 AS request_time` // duration 实际是毫秒（除以1000转为秒）
	return columns
}

// rewriteHttpLogColumnsWithGeoIP 包含国家/城市字段的列定义
func rewriteHttpLogColumnsWithGeoIP(logFormat string) string {
	return rewriteHttpLogColumns(logFormat) + `,
'' AS country,
'' AS city`
}

func (a *LogAnalyzer) RewriteHttpLogColumns() string {
	return rewriteHttpLogColumns(a.logFormat)
}

// RewriteHttpLogColumnsWithGeoIP 返回带地理位置字段的列定义
func (a *LogAnalyzer) RewriteHttpLogColumnsWithGeoIP() string {
	return rewriteHttpLogColumnsWithGeoIP(a.logFormat)
}

func (a *LogAnalyzer) readHttpLogParams() (query string, args []any) {
	if len(a.logFormat) == 0 {
		return
	}

	logFormat := a.logFormat
	switch logFormat {
	case `combined`, `common`:
		query = `, format_type='` + logFormat + `'`
	case `combinedCHD`: // with schema / host / duration
		// GET http webx.top /plugins/slider1/loading.gif HTTP/1.1
		// client_host ident auth_user timestamp %r=method(%m)+path(%U)+query_string(%q)+protocol(%H) status bytes referer user_agent duration
		query = `, format_str='%h %l %u %t "%m %{scheme}i %{c}h %U%q %H" %>s %b "%{Referer}i" "%{User-Agent}i" %>D'`
	case `combinedD`: // with duration
		// client_host ident auth_user timestamp %r=method(%m)+path(%U)+query_string(%q)+protocol(%H) status bytes referer user_agent duration
		query = `, format_str='%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i" %>D'`
	default:
		if after, found := strings.CutPrefix(logFormat, `{combined}`); found {
			logFormat = `%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i" ` + strings.TrimSpace(after)
		} else if after, found := strings.CutPrefix(logFormat, `{common}`); found {
			logFormat = `%h %l %u %t "%r" %>s %b ` + strings.TrimSpace(after)
		}
		args = []any{
			logFormat,
		}
		query = `, format_str=?` //SELECT * FROM read_httpd_log(?, format_str='%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"')
	}
	return
}

// documention: https://github.com/saygox/duckdb-httpd-log/blob/main/docs/read_httpd_log.md
func (a *LogAnalyzer) QueryLogFile(ctx context.Context, opt QueryOptions) ([]map[string]any, error) {
	where := make([]string, 0, len(opt.NamedArgs))
	if opt.NamedArgs == nil {
		opt.NamedArgs = map[string]any{}
	}
	query := `SELECT `
	if len(opt.Columns) > 0 {
		query += opt.Columns
	} else {
		query += `*`
	}
	query += ` FROM `
	if a.logParquet {
		query += `read_parquet`
	} else {
		query += `read_httpd_log`
	}
	query += `(?`
	args := []any{a.logFile}
	if !a.logParquet {
		if len(opt.Columns) == 0 {
			query += `,raw=true`
		}
		_query, _args := a.readHttpLogParams()
		query += _query
		args = append(args, _args...)
	}
	query += `)`
	for key, val := range opt.NamedArgs {
		field := strings.ReplaceAll(key, "`", "``")
		var prefix string
		if len(where) > 0 {
			prefix = ` AND `
		}
		where = append(where, prefix+"`"+field+"`=?")
		args = append(args, val)
	}
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, ` `)
		if len(opt.Where) > 0 {
			query += ` AND ` + opt.Where
		}
	} else if len(opt.Where) > 0 {
		query += ` WHERE ` + opt.Where
	}
	if len(opt.GroupBy) > 0 {
		query += ` GROUP BY ` + opt.GroupBy
	}
	if len(opt.OrderBy) > 0 {
		query += ` ORDER BY ` + opt.OrderBy
	}
	if opt.Limit > 0 {
		query += ` LIMIT ` + strconv.Itoa(opt.Limit)
	}
	if len(opt.WithColumns) > 0 {
		query = `WITH temp AS(` + query + `) SELECT ` + opt.WithColumns + ` FROM temp`
	}

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	r, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var cols []string
	cols, err = r.Columns()
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	for r.Next() {
		row := map[string]any{}
		recv := make([]any, len(cols))
		for idx, col := range cols {
			var v interface{}
			recv[idx] = &v
			row[col] = &v
		}
		err = r.Scan(recv...)
		if err != nil {
			return nil, err
		}
		for name, value := range row {
			row[name] = *(value.(*any))
		}
		rows = append(rows, row)
	}
	return rows, err
}

func minStartTime() time.Time {
	return time.Now().AddDate(0, 0, -30)
}

func whereStartTime(startTime time.Time, noWherePrefix ...bool) string {
	if startTime.IsZero() {
		startTime = minStartTime()
	}
	condition := `timestamp >= '` + startTime.Format(`2006-01-02 15:04:05.000000000-07:00`) + `'::TIMESTAMPTZ`
	if len(noWherePrefix) > 0 && noWherePrefix[0] {
		return condition
	}
	return ` WHERE ` + condition
}

func (a *LogAnalyzer) whereStartTime(startTime time.Time, noWherePrefix ...bool) string {
	if startTime.IsZero() && !a.config.MinStartTime.IsZero() {
		startTime = a.config.MinStartTime
	}
	return whereStartTime(startTime, noWherePrefix...)
}

func (a *LogAnalyzer) overwriteOrIgnore() string {
	if a.config.OverwriteParquet {
		return `, OVERWRITE`
	}
	return `, OVERWRITE_OR_IGNORE`
}

func (a *LogAnalyzer) CovertLogFileToParquet(ctx context.Context, startTime time.Time, outputFile ...string) error {
	var output string
	if len(outputFile) > 0 && len(outputFile[0]) > 0 {
		output = outputFile[0]
	} else {
		output = filepath.Join(a.logDir, `output.parquet`)
	}

	// 判断是否使用带GeoIP的列定义
	columns := a.RewriteHttpLogColumnsWithGeoIP()

	args := []any{output}
	_query, _args := a.readHttpLogParams()
	args = append(args, _args...)
	args = append(args, a.logFile)

	if a.geoIP != nil {
		// 使用GeoIP：先写入临时表，再填充地理位置，最后导出parquet
		return a.convertWithGeoIP(ctx, columns, _query, args, startTime, output, false)
	}

	// 无GeoIP：直接导出
	_, err := a.db.ExecContext(ctx, `COPY (
	SELECT `+columns+` FROM read_httpd_log(?`+_query+`)`+a.whereStartTime(startTime)+`
	) TO ? (FORMAT PARQUET`+a.overwriteOrIgnore()+`)`, args...)
	return err
}

func (a *LogAnalyzer) CovertLogFileToParquetAndPartition(ctx context.Context, startTime time.Time, outputDir ...string) error {
	var output string
	if len(outputDir) > 0 && len(outputDir[0]) > 0 {
		output = outputDir[0]
	} else {
		output = filepath.Join(a.logDir, `logs_partitioned`)
	}
	if err := os.MkdirAll(output, os.ModePerm); err != nil {
		return err
	}

	// 判断是否使用带GeoIP的列定义
	columns := a.RewriteHttpLogColumnsWithGeoIP()

	args := []any{output}
	_query, _args := a.readHttpLogParams()
	args = append(args, _args...)
	args = append(args, a.logFile)

	if a.geoIP != nil {
		// 使用GeoIP：先写入临时表，再填充地理位置，最后分区导出parquet
		return a.convertWithGeoIP(ctx, columns, _query, args, startTime, output, true)
	}

	// 无GeoIP：直接导出
	_, err := a.db.ExecContext(ctx, `COPY (
	SELECT `+columns+`
	FROM read_httpd_log(?`+_query+`)`+a.whereStartTime(startTime)+`
	) TO ? (
		FORMAT PARQUET, 
	 PARTITION_BY (year, month, day, hour)`+a.overwriteOrIgnore()+`
	)`, args...)
	return err
}

// convertWithGeoIP 使用GeoIP转换日志到Parquet
func (a *LogAnalyzer) convertWithGeoIP(ctx context.Context, columns, httpdQuery string, args []any, startTime time.Time, outputPath string, partition bool) error {
	// 1. 创建临时表
	tempTable := `_temp_logs_geoip`
	createSQL := fmt.Sprintf(`CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM read_httpd_log(?%s)`+a.whereStartTime(startTime),
		tempTable, columns, httpdQuery)
	execArg := args[len(args)-1] // 去掉output参数

	if _, err := a.db.ExecContext(ctx, createSQL, execArg); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// 2. 查询所有唯一IP并批量解析GeoIP
	ipRows, err := a.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT DISTINCT remote_addr FROM %s WHERE remote_addr IS NOT NULL AND remote_addr != ''`, tempTable))
	if err != nil {
		return fmt.Errorf("failed to query unique IPs: %w", err)
	}
	defer ipRows.Close()

	// 构建IP到地理位置的映射
	type geoInfo struct {
		country string
		city    string
	}
	ipGeoMap := make(map[string]geoInfo)
	var ips []string

	for ipRows.Next() {
		var ip string
		if err := ipRows.Scan(&ip); err != nil {
			continue
		}
		ips = append(ips, ip)
		loc := a.geoIP.Lookup(ip)
		ipGeoMap[ip] = geoInfo{country: loc.Country, city: loc.City}
	}

	a.logger.Info().Int("unique_ips", len(ips)).Msg("geoip lookup completed")

	// 3. 批量更新临时表中的country和city字段
	// 使用CASE WHEN语句批量更新（每批处理500个IP）
	batchSize := 500
	for i := 0; i < len(ips); i += batchSize {
		end := i + batchSize
		if end > len(ips) {
			end = len(ips)
		}

		// 构建CASE WHEN语句
		caseCountry := "CASE remote_addr "
		caseCity := "CASE remote_addr "
		countries := make([]any, 0, end-i)
		cities := make([]any, 0, end-i)
		for j := i; j < end; j++ {
			info := ipGeoMap[ips[j]]
			caseCountry += fmt.Sprintf("WHEN '%s' THEN ? ", ips[j])
			caseCity += fmt.Sprintf("WHEN '%s' THEN ? ", ips[j])
			countries = append(countries, info.country)
			cities = append(cities, info.city)
		}
		caseCountry += "END"
		caseCity += "END"

		updateSQL := fmt.Sprintf(`UPDATE %s SET country = %s, city = %s 
			WHERE remote_addr IN (%s)`,
			tempTable, caseCountry, caseCity,
			func() string {
				placeholders := make([]string, end-i)
				for k := range placeholders {
					placeholders[k] = "?"
				}
				return strings.Join(placeholders, ",")
			}())

		updateArgs := make([]any, 0, (end-i)*3)
		updateArgs = append(updateArgs, countries...)
		updateArgs = append(updateArgs, cities...)
		for k := 0; k < end-i; k++ {
			updateArgs = append(updateArgs, ips[i+k])
		}

		if _, err := a.db.ExecContext(ctx, updateSQL, updateArgs...); err != nil {
			a.logger.Warn().Err(err).Msg("batch update failed, continuing...")
		}
	}

	// 4. 导出到Parquet
	if partition {
		exportSQL := fmt.Sprintf(`COPY (SELECT * FROM %s) TO ? (FORMAT PARQUET, PARTITION_BY (year, month, day, hour)`+a.overwriteOrIgnore()+`)`, tempTable)
		_, err = a.db.ExecContext(ctx, exportSQL, args[0])
	} else {
		exportSQL := fmt.Sprintf(`COPY (SELECT * FROM %s) TO ? (FORMAT PARQUET`+a.overwriteOrIgnore()+`)`, tempTable)
		_, err = a.db.ExecContext(ctx, exportSQL, args[0])
	}

	if err != nil {
		return fmt.Errorf("failed to export parquet: %w", err)
	}

	// 5. 清理临时表
	a.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tempTable))

	a.logger.Info().Str("file", outputPath).Bool("partition", partition).Int("ips_resolved", len(ips)).Msg("parquet file created with geolocation data")
	return nil
}
