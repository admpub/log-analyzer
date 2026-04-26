package analyzer

import (
	"context"
	"errors"
	"fmt"
	"log-analyzer/internal/model"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AnalyzePath 深度分析特定路径
func (a *LogAnalyzer) AnalyzePath(ctx context.Context, path string, hours int) (*model.PathAnalysisDetail, error) {
	cacheKey := fmt.Sprintf("path_analysis_%s_%d", path, hours)
	if analysis, found := a.cache.GetPathAnalysis(cacheKey); found {
		return analysis, nil
	}

	analysis := &model.PathAnalysisDetail{
		Path:      path,
		TimeRange: fmt.Sprintf("最近%d小时", hours),
	}

	var err error
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string

	// 并行执行所有分析
	wg.Add(7)

	// 1. 基础统计
	go func() {
		defer wg.Done()
		if err := a.getBasicStats(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("基础统计: %v", err))
			mu.Unlock()
		}
	}()

	// 2. 状态码分布
	go func() {
		defer wg.Done()
		if err := a.getStatusDistribution(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("状态码分布: %v", err))
			mu.Unlock()
		}
	}()

	// 3. 小时统计
	go func() {
		defer wg.Done()
		if err := a.getHourlyStats(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("小时统计: %v", err))
			mu.Unlock()
		}
	}()

	// 4. 客户端分析
	go func() {
		defer wg.Done()
		if err := a.getTopClients(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("客户端分析: %v", err))
			mu.Unlock()
		}
	}()

	// 5. UserAgent分析
	go func() {
		defer wg.Done()
		if err := a.getUserAgents(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("UserAgent分析: %v", err))
			mu.Unlock()
		}
	}()

	// 6. 慢请求详情
	go func() {
		defer wg.Done()
		if err := a.getSlowRequests(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("慢请求详情: %v", err))
			mu.Unlock()
		}
	}()

	// 7. 相关性分析
	go func() {
		defer wg.Done()
		if err := a.getCorrelationAnalysis(ctx, analysis, path, hours); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Sprintf("相关性分析: %v", err))
			mu.Unlock()
		}
	}()

	wg.Wait()

	// 生成建议
	analysis.GenerateRecommendations()

	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "\n"))
		a.logger.Error().Fields(errs).Msg(`failed to AnalyzePath`)
		return analysis, err
	}
	// 缓存结果
	a.cache.SetPathAnalysis(cacheKey, analysis)

	return analysis, err
}

// 1. 获取基础统计
func (a *LogAnalyzer) getBasicStats(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	SELECT
		COUNT(*) as request_count,
		COALESCE(ROUND(AVG(request_time) * 1000, 2), 0) as avg_time,
		COALESCE(ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY request_time) * 1000, 2), 0) as p50_time,
		COALESCE(ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY request_time) * 1000, 2), 0) as p90_time,
		COALESCE(ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY request_time) * 1000, 2), 0) as p95_time,
		COALESCE(ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY request_time) * 1000, 2), 0) as p99_time,
		COALESCE(ROUND(MAX(request_time) * 1000, 2), 0) as max_time,
		COALESCE(ROUND(MIN(request_time) * 1000, 2), 0) as min_time,
		COALESCE(ROUND(STDDEV(request_time) * 1000, 2), 0) as std_dev_time,
		COALESCE(SUM(CASE WHEN request_time > 1 THEN 1 ELSE 0 END), 0) as slow_count,
		COALESCE(SUM(CASE WHEN request_time > 3 THEN 1 ELSE 0 END), 0) as very_slow_count,
		COALESCE(SUM(CASE WHEN request_time > 10 THEN 1 ELSE 0 END), 0) as timeout_count
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
		AND request_time IS NOT NULL`

	row := a.db.QueryRowContext(ctx, query, path)

	err := row.Scan(
		&analysis.RequestCount,
		&analysis.TimeStats.AvgTime,
		&analysis.TimeStats.P50Time,
		&analysis.TimeStats.P90Time,
		&analysis.TimeStats.P95Time,
		&analysis.TimeStats.P99Time,
		&analysis.TimeStats.MaxTime,
		&analysis.TimeStats.MinTime,
		&analysis.TimeStats.StdDevTime,
		&analysis.TimeStats.SlowCount,
		&analysis.TimeStats.VerySlowCount,
		&analysis.TimeStats.TimeoutCount,
	)

	return err
}

// 2. 获取状态码分布
func (a *LogAnalyzer) getStatusDistribution(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	WITH status_stats AS (
		SELECT
			status as status_code,
			COUNT(*) as count,
			COALESCE(ROUND(AVG(request_time) * 1000, 2), 0) as avg_time
		FROM ` + a.config.LogTable + `
		WHERE 
			timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
			AND path = ?
		GROUP BY status
	),
	total_stats AS (
		SELECT SUM(count) as total FROM status_stats
	)
	SELECT
		ss.status_code,
		ss.count,
		COALESCE(ROUND(ss.count * 100.0 / ts.total, 2), 0) as percentage,
		ss.avg_time
	FROM status_stats ss, total_stats ts
	ORDER BY ss.count DESC
	LIMIT 20`

	rows, err := a.db.QueryContext(ctx, query, path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var distributions []model.StatusDistributionAvgTime
	for rows.Next() {
		var d model.StatusDistributionAvgTime
		if err := rows.Scan(&d.StatusCode, &d.Count, &d.Percentage, &d.AvgTime); err != nil {
			return err
		}
		distributions = append(distributions, d)
	}

	analysis.StatusDistribution = distributions
	return nil
}

// 3. 获取小时统计
func (a *LogAnalyzer) getHourlyStats(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	SELECT
		DATE_TRUNC('hour',timestamp) as hour,
		COUNT(*) as request_count,
		COALESCE(ROUND(AVG(request_time) * 1000, 2), 0) as avg_time,
		COALESCE(ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY request_time) * 1000, 2), 0) as p95_time,
		COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0) as error_count,
		COUNT(*) * 1.0 / 3600 as peak_rps
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
	GROUP BY DATE_TRUNC('hour',timestamp)
	ORDER BY hour`

	rows, err := a.db.QueryContext(ctx, query, path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var hourlyStats []model.HourlyPathStats
	for rows.Next() {
		var s model.HourlyPathStats
		if err := rows.Scan(&s.Hour, &s.RequestCount, &s.AvgTime, &s.P95Time, &s.ErrorCount, &s.PeakRPS); err != nil {
			return err
		}
		hourlyStats = append(hourlyStats, s)
	}

	analysis.HourlyStats = hourlyStats
	return nil
}

// 4. 获取客户端分析
func (a *LogAnalyzer) getTopClients(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	SELECT
		ip,
		COUNT(*) as request_count,
		COALESCE(ROUND(AVG(request_time) * 1000, 2), 0) as avg_time,
		COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0) as error_count,
		MAX(timestamp) as last_seen
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
	GROUP BY ip
	ORDER BY request_count DESC
	LIMIT 20`

	rows, err := a.db.QueryContext(ctx, query, path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var clients []model.TopClient
	for rows.Next() {
		var c model.TopClient
		var lastSeen time.Time
		if err := rows.Scan(&c.IP, &c.RequestCount, &c.AvgTime, &c.ErrorCount, &lastSeen); err != nil {
			return err
		}
		c.LastSeen = lastSeen.Format(time.RFC3339)
		clients = append(clients, c)
	}

	analysis.TopClients = clients
	return nil
}

// 5. 获取UserAgent分析
func (a *LogAnalyzer) getUserAgents(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	WITH ua_stats AS (
		SELECT
			CASE
				WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
				WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
				WHEN user_agent LIKE '%Safari%' AND user_agent NOT LIKE '%Chrome%' THEN 'Safari'
				WHEN user_agent LIKE '%Edge%' THEN 'Edge'
				WHEN user_agent LIKE '%Mobile%' OR user_agent LIKE '%Android%' OR user_agent LIKE '%iPhone%' THEN 'Mobile'
				WHEN user_agent LIKE '%bot%' OR user_agent LIKE '%crawler%' OR user_agent LIKE '%spider%' THEN 'Crawler'
				ELSE 'Other'
			END as category,
			user_agent,
			COUNT(*) as count,
			COALESCE(ROUND(AVG(request_time) * 1000, 2), 0) as avg_time,
			COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0) as error_count
		FROM ` + a.config.LogTable + `
		WHERE 
			timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
			AND path = ?
		GROUP BY category, user_agent
	),
	total_stats AS (
		SELECT SUM(count) as total FROM ua_stats
	)
	SELECT
		user_agent,
		category,
		count,
		COALESCE(ROUND(count * 100.0 / ts.total, 2), 0) as percentage,
		avg_time,
		COALESCE(ROUND(error_count * 100.0 / count, 2), 0) as error_rate
	FROM ua_stats, total_stats ts
	ORDER BY count DESC
	LIMIT 20`

	rows, err := a.db.QueryContext(ctx, query, path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var uaStats []model.UserAgentDistribution
	for rows.Next() {
		var u model.UserAgentDistribution
		if err := rows.Scan(&u.UserAgent, &u.Category, &u.Count, &u.Percentage, &u.AvgTime, &u.ErrorRate); err != nil {
			return err
		}
		uaStats = append(uaStats, u)
	}

	analysis.UserAgents = uaStats
	return nil
}

// 6. 获取慢请求详情
func (a *LogAnalyzer) getSlowRequests(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	query := `
	SELECT
		timestamp,
		COALESCE(ROUND(request_time * 1000, 2), 0) as response_time,
		status,
		COALESCE(ROUND(bytes_sent / 1024, 2), 0) as size_kb,
		ip,
		user_agent,
		COALESCE(referer, '') as referer
	FROM ` + a.config.LogTable + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
		AND request_time > 1
	ORDER BY request_time DESC
	LIMIT 50`

	rows, err := a.db.QueryContext(ctx, query, path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var slowRequests []model.SlowRequestDetail
	for rows.Next() {
		var r model.SlowRequestDetail
		var timestamp time.Time
		if err := rows.Scan(&timestamp, &r.ResponseTime, &r.Status, &r.SizeKB, &r.IP, &r.UserAgent, &r.Referer); err != nil {
			return err
		}
		r.Timestamp = timestamp.Format(time.RFC3339)
		slowRequests = append(slowRequests, r)
	}

	analysis.SlowRequests = slowRequests
	return nil
}

// 7. 获取相关性分析
func (a *LogAnalyzer) getCorrelationAnalysis(ctx context.Context, analysis *model.PathAnalysisDetail, path string, hours int) error {
	tableName := a.config.LogTable
	query := `
	SELECT
		COALESCE(CORR(request_time, bytes_sent), 0) as size_time_corr,
		COALESCE(CORR(request_time, status), 0) as status_time_corr,
		COALESCE(CORR(request_time, EXTRACT(HOUR FROM timestamp)), 0) as hour_time_corr
	FROM ` + tableName + `
	WHERE 
		timestamp >= NOW() - INTERVAL ` + strconv.Itoa(hours) + ` HOUR
		AND path = ?
		AND request_time IS NOT NULL
		AND bytes_sent IS NOT NULL`

	row := a.db.QueryRowContext(ctx, query, path)

	var corr model.CorrelationAnalysis
	if err := row.Scan(&corr.SizeTimeCorr, &corr.StatusTimeCorr, &corr.HourTimeCorr); err != nil {
		return err
	}
	if isNaN(corr.SizeTimeCorr) {
		corr.SizeTimeCorr = 0
	}
	if isNaN(corr.HourTimeCorr) {
		corr.HourTimeCorr = 0
	}
	if isNaN(corr.StatusTimeCorr) {
		corr.StatusTimeCorr = 0
	}
	// 分析模式
	patterns := []string{}
	if corr.SizeTimeCorr > 0.7 {
		patterns = append(patterns, "强正相关：响应时间与响应大小高度相关")
	} else if corr.SizeTimeCorr > 0.3 {
		patterns = append(patterns, "中等正相关：响应时间与响应大小相关")
	} else if corr.SizeTimeCorr < -0.3 {
		patterns = append(patterns, "负相关：响应时间与响应大小呈负相关")
	}

	if corr.HourTimeCorr > 0.3 {
		patterns = append(patterns, "时间模式：晚上时段响应更慢")
	} else if corr.HourTimeCorr < -0.3 {
		patterns = append(patterns, "时间模式：白天时段响应更慢")
	}

	if corr.StatusTimeCorr > 0.5 {
		patterns = append(patterns, "状态码相关：高错误率时响应时间更长")
	} else if corr.StatusTimeCorr < -0.5 {
		patterns = append(patterns, "状态码反常：成功请求反而更慢，可能存在重试")
	}

	corr.Patterns = patterns
	analysis.Correlation = corr

	return nil
}
