package analyzer

import (
	"context"
	"errors"
	"fmt"
	"log-analyzer/internal/model"
	"strconv"
	"time"
)

func (a *LogAnalyzer) GetRealTimeStats(ctx context.Context, hours int) (*model.RealTimeStats, error) {
	// 检查缓存
	if stats, found := a.cache.GetRealTimeStats(hours); found {
		return stats, nil
	}

	table := a.config.LogTable

	query := `
	SELECT
		COUNT(*) as total_requests,
		COUNT(DISTINCT ip) as unique_visitors,
		COALESCE(SUM(bytes_sent), 0) as total_bytes,
		COALESCE(AVG(bytes_sent), 0) as avg_response_size,
		COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0) as error_count,
		COALESCE(AVG(CASE WHEN status >= 400 THEN 1.0 ELSE 0 END), 0) as error_rate,
		COUNT(*) * 1.0 / ? as request_rate,
		COALESCE(SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END), 0) as server_errors,
		COALESCE(SUM(CASE WHEN status >= 400 AND status < 500 THEN 1 ELSE 0 END), 0) as client_errors,
		COALESCE(SUM(CASE WHEN status < 400 THEN 1 ELSE 0 END), 0) as success_count,
		COALESCE(AVG(CASE WHEN status < 400 THEN 1.0 ELSE 0 END), 0) as success_rate
	FROM ` + table + `
	WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	var stats model.RealTimeStats
	row := a.db.QueryRowContext(ctx, query, hours*3600)

	err := row.Scan(
		&stats.TotalRequests,
		&stats.UniqueVisitors,
		&stats.TotalBytes,
		&stats.AvgResponseSize,
		&stats.ErrorCount,
		&stats.ErrorRate,
		&stats.RequestRate,
		&stats.ServerErrors,
		&stats.ClientErrors,
		&stats.SuccessCount,
		&stats.SuccessRate,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get realtime stats: %w", err)
	}

	// 缓存结果
	a.cache.SetRealTimeStats(hours, &stats)
	return &stats, nil
}

func (a *LogAnalyzer) GetTopPaths(ctx context.Context, limit int, hours int) ([]model.TopPath, error) {
	cacheKey := fmt.Sprintf("top_paths_%d_%d", limit, hours)
	if paths, found := a.cache.GetTopPaths(cacheKey); found {
		return paths, nil
	}

	query := `
	SELECT
		path,
		COUNT(*) as request_count,
		COUNT(DISTINCT ip) as unique_ips,
		AVG(bytes_sent) as avg_bytes,
		SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) as errors,
		AVG(CASE WHEN status >= 400 THEN 1.0 ELSE 0 END) as error_rate,
		SUM(bytes_sent) * 1.0 / ? as bytes_rate
	FROM ` + a.config.LogTable + `
	WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path != ''
	GROUP BY path
	ORDER BY request_count DESC
	LIMIT ?`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query, hours*3600, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top paths: %w", err)
	}
	defer rows.Close()

	var paths []model.TopPath
	for rows.Next() {
		var p model.TopPath
		if err := rows.Scan(
			&p.Path,
			&p.RequestCount,
			&p.UniqueIPs,
			&p.AvgBytes,
			&p.Errors,
			&p.ErrorRate,
			&p.BytesRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		paths = append(paths, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetTopPaths(cacheKey, paths)
	return paths, nil
}

// GetSlowPaths 查询耗时最长的路径
func (a *LogAnalyzer) GetSlowPaths(ctx context.Context, limit int, hours int, minRequests int, noOther ...bool) ([]model.SlowPathAnalysis, error) {
	cacheKey := fmt.Sprintf("slow_paths_%d_%d_%d", hours, limit, minRequests)
	if paths, found := a.cache.GetSlowPaths(cacheKey); found {
		return paths, nil
	}
	/*
		queryQuick := `SELECT * FROM nginx_logs TABLESAMPLE BERNOULLI(1)  -- 1%抽样
		WHERE request_time > 1 ORDER BY request_time DESC LIMIT 100`
	*/
	query := `
	WITH path_stats AS (
		SELECT
			path,
			COUNT(*) as request_count,
			ROUND(AVG(request_time) * 1000, 2) as avg_ms,
			ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY request_time) * 1000, 2) as p95_ms,
			ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY request_time) * 1000, 2) as p99_ms,
			ROUND(MAX(request_time) * 1000, 2) as max_ms,
			SUM(CASE WHEN request_time > 1 THEN 1 ELSE 0 END) as slow_count,
			ROUND(AVG(bytes_sent) / 1024, 2) as avg_kb,
			ROUND(SUM(CASE WHEN status < 400 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) as success_rate
		FROM ` + a.config.LogTable + `
		WHERE 
			timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
			AND path != ''
			AND request_time IS NOT NULL
			AND request_time > 0
		GROUP BY path
		HAVING COUNT(*) >= ?
	)
	SELECT
		path,
		request_count,
		avg_ms,
		p95_ms,
		p99_ms,
		max_ms,
		slow_count,
		ROUND(slow_count * 100.0 / request_count, 2) as slow_rate,
		avg_kb,
		success_rate
	FROM path_stats
	ORDER BY p95_ms DESC
	LIMIT ?`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query, minRequests, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query slow paths: %w", err)
	}
	defer rows.Close()

	var paths []model.SlowPathAnalysis
	var skipOther bool
	if len(noOther) > 0 {
		skipOther = noOther[0]
	}
	for rows.Next() {
		var p model.SlowPathAnalysis
		if err := rows.Scan(
			&p.Path,
			&p.RequestCount,
			&p.AvgResponseTime,
			&p.P95ResponseTime,
			&p.P99ResponseTime,
			&p.MaxResponseTime,
			&p.SlowRequestCount,
			&p.SlowRequestRate,
			&p.AvgResponseSize,
			&p.SuccessRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// 获取额外信息
		if !skipOther {
			p.PeakHour = a.getPathPeakHour(ctx, p.Path, hours)
			p.TopStatusCodes = a.getPathTopStatusCodes(ctx, p.Path, hours)
		}

		paths = append(paths, p)
	}

	a.cache.SetSlowPaths(cacheKey, paths)
	return paths, nil
}

// 辅助方法：获取路径的请求高峰期
func (a *LogAnalyzer) getPathPeakHour(ctx context.Context, path string, hours int) string {
	query := `
	SELECT DATE_PART('hour', timestamp) as hour
	FROM ` + a.config.LogTable + `
	WHERE 
		path = ?
		AND timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
	GROUP BY DATE_PART('hour', timestamp)
	ORDER BY COUNT(*) DESC
	LIMIT 1`

	var peakHour string
	err := a.db.QueryRowContext(ctx, query, path).Scan(&peakHour)
	if err != nil {
		a.logger.Error().Msgf(`failed to getPathPeakHour: %v`, err.Error())
	}
	return peakHour
}

// 辅助方法：获取路径的主要状态码
func (a *LogAnalyzer) getPathTopStatusCodes(ctx context.Context, path string, hours int) string {
	query := `
	SELECT STRING_AGG(status::text, ', ')
	FROM (
		SELECT status, COUNT(*) as cnt
		FROM ` + a.config.LogTable + `
		WHERE 
			path = ?
			AND timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		GROUP BY status
		ORDER BY cnt DESC
		LIMIT 3
	) t`

	var statusCodes string
	err := a.db.QueryRowContext(ctx, query, path).Scan(&statusCodes)
	if err != nil {
		a.logger.Error().Msgf(`failed to getPathTopStatusCodes: %v`, err.Error())
	}
	return statusCodes
}

func (a *LogAnalyzer) GetPathDetail(ctx context.Context, path string, hours int) (map[string]any, error) {
	hoursString := strconv.Itoa(hours)
	query := `
	SELECT
		path,
		COUNT(*) as request_count,
		ROUND(AVG(request_time) * 1000, 2) as avg_response_time,
		ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY request_time) * 1000, 2) as p95_response_time,
		ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY request_time) * 1000, 2) as p99_response_time,
		ROUND(MAX(request_time) * 1000, 2) as max_response_time,
		SUM(CASE WHEN request_time > 1 THEN 1 ELSE 0 END) as slow_request_count,
		ROUND(SUM(CASE WHEN request_time > 1 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) as slow_request_rate,
		ROUND(AVG(bytes_sent) / 1024, 2) as avg_response_size,
		ROUND(SUM(CASE WHEN status < 400 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) as success_rate
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + hoursString + ` HOUR
		AND path = ?
	GROUP BY path
	LIMIT 1`

	results, err := a.GetCustomQuery(ctx, query, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get path detail: %w", err)
	}

	if len(results) == 0 {
		return nil, errors.New("path not found")
	}

	// 获取更多信息
	detail := results[0]

	// 获取主要状态码
	statusQuery := `
	SELECT STRING_AGG(status::text, ', ') as top_status_codes
	FROM (
		SELECT status, COUNT(*) as cnt
		FROM ` + a.config.LogTable + `
		WHERE 
			timestamp >= NOW() - INTERVAL ` + hoursString + ` HOUR
			AND path = ?
		GROUP BY status
		ORDER BY cnt DESC
		LIMIT 3
	) t`

	statusResults, _ := a.GetCustomQuery(ctx, statusQuery, path)
	if len(statusResults) > 0 {
		detail["top_status_codes"] = statusResults[0]["top_status_codes"]
	}

	// 获取请求高峰期
	peakQuery := `
	SELECT DATE_FORMAT(timestamp, '%H:00') as peak_hour
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + hoursString + ` HOUR
		AND path = ?
	GROUP BY DATE_FORMAT(timestamp, '%H:00')
	ORDER BY COUNT(*) DESC
	LIMIT 1`

	peakResults, _ := a.GetCustomQuery(ctx, peakQuery, path)
	if len(peakResults) > 0 {
		detail["peak_hour"] = peakResults[0]["peak_hour"]
	}
	return detail, err
}

func (a *LogAnalyzer) GetSlowRequests(ctx context.Context, path string, threshold float64, limit int, hours int) ([]map[string]any, error) {
	query := `
	SELECT
		timestamp,
		path,
		ROUND(request_time * 1000, 2) as response_ms,
		status,
		ROUND(bytes_sent / 1024, 2) as size_kb,
		ip,
		user_agent
	FROM nginx_logs
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
		AND request_time > ?
	ORDER BY request_time DESC
	LIMIT ?`

	results, err := a.GetCustomQuery(ctx, query, path, threshold, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get slow requests: %w", err)
	}

	return results, err
}

func (a *LogAnalyzer) GetTopIPs(ctx context.Context, limit int, hours int) ([]model.TopIP, error) {
	cacheKey := fmt.Sprintf("top_ips_%d_%d", limit, hours)
	if ips, found := a.cache.GetTopIPs(cacheKey); found {
		return ips, nil
	}

	query := `
	SELECT
		ip,
		COUNT(*) as request_count,
		COUNT(DISTINCT path) as unique_paths,
		AVG(bytes_sent) as avg_bytes,
		SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) as errors,
		AVG(CASE WHEN status >= 400 THEN 1.0 ELSE 0 END) as error_rate,
		SUM(bytes_sent) * 1.0 / ? as bytes_rate
	FROM ` + a.config.LogTable + `
	WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND ip != ''
	GROUP BY ip
	ORDER BY request_count DESC
	LIMIT ?`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query, hours*3600, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top paths: %w", err)
	}
	defer rows.Close()

	var ips []model.TopIP
	for rows.Next() {
		var p model.TopIP
		if err := rows.Scan(
			&p.IP,
			&p.RequestCount,
			&p.UniquePaths,
			&p.AvgBytes,
			&p.Errors,
			&p.ErrorRate,
			&p.BytesRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		ips = append(ips, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetTopIPs(cacheKey, ips)
	return ips, nil
}

func (a *LogAnalyzer) GetHourlyStats(ctx context.Context, days int) ([]model.HourlyStats, error) {
	cacheKey := fmt.Sprintf("hourly_stats_%d", days)
	if stats, found := a.cache.GetHourlyStats(cacheKey); found {
		return stats, nil
	}

	query := `
	SELECT
		date_trunc('hour', timestamp) as hour,
		COUNT(*) as request_count,
		COUNT(DISTINCT ip) as unique_ips,
		SUM(bytes_sent) as total_bytes,
		SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) as error_count,
		COUNT(*) * 1.0 / 3600 as peak_rps
	FROM ` + a.config.LogTable + `
	WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(days) + ` DAY
	GROUP BY date_trunc('hour', timestamp)
	ORDER BY hour`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query hourly stats: %w", err)
	}
	defer rows.Close()

	var stats []model.HourlyStats
	for rows.Next() {
		var s model.HourlyStats
		if err := rows.Scan(
			&s.Hour,
			&s.RequestCount,
			&s.UniqueIPs,
			&s.TotalBytes,
			&s.ErrorCount,
			&s.PeakRPS,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		stats = append(stats, s)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetHourlyStats(cacheKey, stats)
	return stats, nil
}

func (a *LogAnalyzer) GetStatusDistribution(ctx context.Context) ([]model.StatusDistribution, error) {
	if dist, found := a.cache.GetStatusDistribution(); found {
		return dist, nil
	}

	query := `
	WITH status_counts AS (
		SELECT
			status as status_code,
			COUNT(*) as count
		FROM ` + a.config.LogTable + `
		WHERE timestamp >= NOW() - INTERVAL 24 HOUR
		GROUP BY status
	),
	total_counts AS (
		SELECT SUM(count) as total FROM status_counts
	)
	SELECT
		sc.status_code,
		sc.count,
		sc.count * 100.0 / tc.total as percentage,
		CASE
			WHEN sc.status_code < 300 THEN 'success'
			WHEN sc.status_code < 400 THEN 'redirect'
			WHEN sc.status_code < 500 THEN 'client_error'
			ELSE 'server_error'
		END as category
	FROM status_counts sc, total_counts tc
	ORDER BY sc.count DESC`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query status distribution: %w", err)
	}
	defer rows.Close()

	var dist []model.StatusDistribution
	for rows.Next() {
		var d model.StatusDistribution
		if err := rows.Scan(
			&d.StatusCode,
			&d.Count,
			&d.Percentage,
			&d.Category,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		dist = append(dist, d)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetStatusDistribution(dist)
	return dist, nil
}

func (a *LogAnalyzer) GetSuspiciousIPs(ctx context.Context, threshold int) ([]model.IPAnalysis, error) {
	query := `
	WITH ip_stats AS (
		SELECT
			ip,
			COUNT(*) as request_count,
			COUNT(DISTINCT path) as unique_paths,
			AVG(bytes_sent) as avg_bytes,
			MAX(timestamp) as last_seen,
			MIN(timestamp) as first_seen,
			COUNT(*) * 1.0 / (EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) + 1) as request_rate,
			AVG(CASE WHEN status >= 400 THEN 1.0 ELSE 0 END) as error_rate,
			MAX(user_agent) as user_agent
		FROM ` + a.config.LogTable + `
		WHERE timestamp >= NOW() - INTERVAL 1 HOUR
		GROUP BY ip
		HAVING COUNT(*) > ?
	)
	SELECT
		ip,
		request_count,
		unique_paths,
		avg_bytes,
		last_seen,
		request_rate,
		error_rate,
		user_agent,
		CASE
			WHEN request_rate > 100 OR error_rate > 0.5 THEN true
			ELSE false
		END as is_suspicious
	FROM ip_stats
	ORDER BY request_count DESC
	LIMIT 100`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query, threshold)
	if err != nil {
		return nil, fmt.Errorf("failed to query suspicious IPs: %w", err)
	}
	defer rows.Close()

	var ips []model.IPAnalysis
	for rows.Next() {
		var ip model.IPAnalysis
		var lastSeen time.Time
		if err := rows.Scan(
			&ip.IP,
			&ip.RequestCount,
			&ip.TotalBytes,
			&ip.LastSeen,
			&ip.RequestRate,
			&ip.ErrorRate,
			&ip.UserAgent,
			&ip.IsSuspicious,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		ip.LastSeen = lastSeen.Format(time.RFC3339)
		ips = append(ips, ip)
	}

	return ips, nil
}

func (a *LogAnalyzer) GetTopCountries(ctx context.Context, limit int, hours int) ([]model.CountryStats, error) {
	cacheKey := fmt.Sprintf("top_countries_%d_%d", limit, hours)
	if countries, found := a.cache.GetTopCountries(cacheKey); found {
		return countries, nil
	}

	query := `
	SELECT
		country,
		COUNT(*) as request_count,
		COUNT(DISTINCT ip) as unique_ips,
		COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage,
		AVG(CASE WHEN status >= 400 THEN 1.0 ELSE 0 END) as error_rate
	FROM ` + a.config.LogTable + `
	WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
	GROUP BY country
	ORDER BY request_count DESC
	LIMIT ?`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top countries: %w", err)
	}
	defer rows.Close()

	var countries []model.CountryStats
	for rows.Next() {
		var c model.CountryStats
		if err := rows.Scan(
			&c.Country,
			&c.RequestCount,
			&c.UniqueIPs,
			&c.Percentage,
			&c.ErrorRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		countries = append(countries, c)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetTopCountries(cacheKey, countries)
	return countries, nil
}

// GetUVTrend 获取UV趋势数据（按天分组）
func (a *LogAnalyzer) GetUVTrend(ctx context.Context, days int) ([]model.UVTrend, error) {
	cacheKey := fmt.Sprintf("uv_trend_%d", days)
	if trend, found := a.cache.GetUVTrend(cacheKey); found {
		return trend, nil
	}

	// 按天统计UV：新访客(首次出现日期=当天)、回访客(首次出现日期<当天)
	query := `
	WITH daily_stats AS (
		SELECT
			DATE_TRUNC('day', timestamp) as date,
			COUNT(*) as pv,
			COUNT(DISTINCT ip) as total_uv,
			ip,
			MIN(DATE_TRUNC('day', timestamp)) OVER (PARTITION BY ip) as ip_first_seen
		FROM ` + a.config.LogTable + `
		WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(days) + ` DAY
		GROUP BY DATE_TRUNC('day', timestamp), ip
	),
	daily_summary AS (
		SELECT
			date,
			SUM(pv) as pv,
			COUNT(DISTINCT CASE WHEN date = ip_first_seen THEN ip END) as new_uv,
			COUNT(DISTINCT CASE WHEN date > ip_first_seen THEN ip END) as returning_uv
		FROM daily_stats
		GROUP BY date
	)
	SELECT
		date::VARCHAR as date,
		COALESCE(new_uv, 0) as new_uv,
		COALESCE(returning_uv, 0) as returning_uv,
		COALESCE(new_uv, 0) + COALESCE(returning_uv, 0) as total_uv,
		COALESCE(pv, 0) as pv,
		CASE WHEN (COALESCE(new_uv, 0) + COALESCE(returning_uv, 0)) > 0
			THEN ROUND(COALESCE(pv, 0) * 1.0 / (COALESCE(new_uv, 0) + COALESCE(returning_uv, 0)), 2)
			ELSE 0 END as avg_pv_per_uv
	FROM daily_summary
	ORDER BY date`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query uv trend: %w", err)
	}
	defer rows.Close()

	var trends []model.UVTrend
	for rows.Next() {
		var t model.UVTrend
		if err := rows.Scan(
			&t.Date,
			&t.NewUV,
			&t.ReturningUV,
			&t.TotalUV,
			&t.PV,
			&t.AvgPVPerUV,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		trends = append(trends, t)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	a.cache.SetUVTrend(cacheKey, trends)
	return trends, nil
}

// GetUVDistribution 获取UV分布概览
func (a *LogAnalyzer) GetUVDistribution(ctx context.Context, days int) (*model.UVDistribution, error) {
	cacheKey := fmt.Sprintf("uv_dist_%d", days)
	if dist, found := a.cache.GetUVDistribution(cacheKey); found {
		return dist, nil
	}

	// 总体统计
	overallQuery := `
	WITH all_data AS (
		SELECT
			ip,
			COUNT(*) as pv,
			MIN(timestamp) as first_visit,
			DATEDIFF('day', MIN(timestamp), NOW()) as days_since_first
		FROM ` + a.config.LogTable + `
		WHERE timestamp >= NOW() - INTERVAL ` + strconv.Itoa(days) + ` DAY
		AND ip != ''
		GROUP BY ip
	),
	uv_summary AS (
		SELECT
			COUNT(*) as total_uv,
			COALESCE(SUM(pv), 0) as total_pv,
			COALESCE(SUM(CASE WHEN days_since_first <= 1 THEN 1 ELSE 0 END), 0) as new_uv,
			COALESCE(SUM(CASE WHEN days_since_first > 1 THEN 1 ELSE 0 END), 0) as returning_uv,
			COALESCE(ROUND(AVG(pv), 2), 0) as avg_pv_per_user,
			COALESCE(SUM(CASE WHEN pv = 1 THEN 1 ELSE 0 END), 0)::FLOAT / COUNT(*) * 100.0 as bounce_rate
		FROM all_data
	)
	SELECT * FROM uv_summary`

	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	var dist model.UVDistribution
	row := a.db.QueryRowContext(ctx, overallQuery)
	err := row.Scan(
		&dist.TotalUV,
		&dist.TotalPV,
		&dist.NewUV,
		&dist.ReturningUV,
		&dist.AvgPVPerUser,
		&dist.BounceRate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query uv distribution: %w", err)
	}

	if isNaN(dist.AvgPVPerUser) {
		dist.AvgPVPerUser = 0
	}
	if isNaN(dist.BounceRate) {
		dist.BounceRate = 0
	}

	if dist.TotalUV > 0 {
		dist.NewUVRatio = float64(dist.NewUV) / float64(dist.TotalUV) * 100
	}

	// 各时段UV分布
	hourQuery := `
	SELECT
		date_trunc('hour', timestamp) as hour,
		COUNT(DISTINCT ip) as uv,
		COUNT(*) as pv
	FROM ` + a.config.LogTable + `
	WHERE timestamp >= NOW() - INTERVAL 1 DAY
	AND ip != ''
	GROUP BY date_trunc('hour', timestamp)
	ORDER BY hour`

	rows, err := a.db.QueryContext(ctx, hourQuery)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var h model.HourUV
			if rows.Scan(&h.Hour, &h.UV, &h.PV) == nil {
				dist.HotHourUVs = append(dist.HotHourUVs, h)
			}
		}
	}

	a.cache.SetUVDistribution(cacheKey, &dist)
	return &dist, err
}

func (a *LogAnalyzer) GetCustomQuery(ctx context.Context, sql string, params ...interface{}) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, a.config.QueryTimeout)
	defer cancel()

	rows, err := a.db.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowMap := make(map[string]interface{})
		for i, col := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		results = append(results, rowMap)
	}

	return results, nil
}
