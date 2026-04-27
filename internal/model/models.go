package model

import (
	"fmt"
	"strconv"
	"time"
)

// LogRecord 日志记录结构
type LogRecord struct {
	ID            int64     `json:"id" db:"id"`
	RemoteAddr    string    `json:"remote_addr" db:"remote_addr"`
	TimeLocal     time.Time `json:"time_local" db:"time_local"`
	Method        string    `json:"method" db:"method"`
	Path          string    `json:"path" db:"path"`
	QueryString   string    `json:"query_string,omitempty" db:"query_string"`
	Protocol      string    `json:"protocol" db:"protocol"`
	Status        int       `json:"status" db:"status"`
	BodyBytesSent int64     `json:"body_bytes_sent" db:"body_bytes_sent"`
	UserAgent     string    `json:"user_agent" db:"user_agent"`
	Referer       string    `json:"referer,omitempty" db:"referer"`
	RequestTime   float64   `json:"request_time,omitempty" db:"request_time"`
	Year          int       `json:"year" db:"year"`
	Month         int       `json:"month" db:"month"`
	Day           int       `json:"day" db:"day"`
	Hour          int       `json:"hour" db:"hour"`
	IsBot         bool      `json:"is_bot" db:"is_bot"`
	Country       string    `json:"country,omitempty" db:"country"`
	City          string    `json:"city,omitempty" db:"city"`
}

// StatsRequest 统计请求参数
type StatsRequest struct {
	TimeRange TimeRange `json:"time_range"`
	GroupBy   string    `json:"group_by"`
	Filters   []Filter  `json:"filters"`
	Limit     int       `json:"limit"`
	Offset    int       `json:"offset"`
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Filter struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // eq, ne, gt, lt, like, in
	Value    interface{} `json:"value"`
}

// API响应结构
type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Meta    *Pagination `json:"meta,omitempty"`
}

type Pagination struct {
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Pages    int   `json:"pages"`
}

// 统计数据结构
type RealTimeStats struct {
	TotalRequests   int64   `json:"total_requests"`
	UniqueVisitors  int64   `json:"unique_visitors"`
	TotalBytes      int64   `json:"total_bytes"`
	AvgResponseSize float64 `json:"avg_response_size"`
	ErrorCount      int64   `json:"error_count"`
	ErrorRate       float64 `json:"error_rate"`
	RequestRate     float64 `json:"request_rate"` // 每秒请求数
	ServerErrors    int64   `json:"server_errors"`
	ClientErrors    int64   `json:"client_errors"`
	SuccessCount    int64   `json:"success_count"`
	SuccessRate     float64 `json:"success_rate"`
}

type TopPath struct {
	Path         string  `json:"path"`
	RequestCount int64   `json:"request_count"`
	UniqueIPs    int64   `json:"unique_ips"`
	AvgBytes     float64 `json:"avg_bytes"`
	Errors       int64   `json:"errors"`
	ErrorRate    float64 `json:"error_rate"`
	BytesRate    float64 `json:"bytes_rate"` // 每秒字节数
}

// SlowPathAnalysis 慢路径分析结果
type SlowPathAnalysis struct {
	Path             string  `json:"path"`
	RequestCount     int64   `json:"request_count"`
	AvgResponseTime  float64 `json:"avg_response_time"`
	P95ResponseTime  float64 `json:"p95_response_time"`
	P99ResponseTime  float64 `json:"p99_response_time"`
	MaxResponseTime  float64 `json:"max_response_time"`
	SlowRequestCount int64   `json:"slow_request_count"` // >1s的请求数
	SlowRequestRate  float64 `json:"slow_request_rate"`  // 慢请求比例
	AvgResponseSize  float64 `json:"avg_response_size"`  // 平均响应大小(KB)
	SuccessRate      float64 `json:"success_rate"`       // 成功率
	PeakHour         string  `json:"peak_hour"`          // 请求高峰期
	TopStatusCodes   string  `json:"top_status_codes"`   // 主要状态码
	TopUserAgents    string  `json:"top_user_agents"`    // 主要UserAgent
}

// PathAnalysisDetail 路径深度分析结果
type PathAnalysisDetail struct {
	Path               string                      `json:"path"`
	TimeRange          string                      `json:"time_range"`
	RequestCount       int64                       `json:"request_count"`
	TimeStats          PathTimeStats               `json:"time_stats"`
	StatusDistribution []StatusDistributionAvgTime `json:"status_distribution"`
	HourlyStats        []HourlyPathStats           `json:"hourly_stats"`
	TopClients         []TopClient                 `json:"top_clients"`
	UserAgents         []UserAgentDistribution     `json:"user_agents"`
	SlowRequests       []SlowRequestDetail         `json:"slow_requests"`
	Correlation        CorrelationAnalysis         `json:"correlation"`
	Recommendations    []string                    `json:"recommendations"`
}

// 生成优化建议
func (a *PathAnalysisDetail) GenerateRecommendations() {
	var recs []string

	// [FIX] 空数据保护：请求量为0时无需生成建议
	if a.RequestCount <= 0 {
		a.Recommendations = recs
		return
	}

	// 基于响应时间
	if a.TimeStats.AvgTime > 1000 {
		recs = append(recs, "🚨 严重：平均响应时间超过1秒，需要立即优化")
	} else if a.TimeStats.AvgTime > 500 {
		recs = append(recs, "⚠️ 警告：平均响应时间超过500ms，建议优化")
	} else if a.TimeStats.AvgTime > 200 {
		recs = append(recs, "ℹ️ 提示：平均响应时间超过200ms，可考虑优化")
	}

	// 基于P95响应时间
	if a.TimeStats.P95Time > 3000 {
		recs = append(recs, "🚨 严重：P95响应时间超过3秒，用户体验很差")
	} else if a.TimeStats.P95Time > 1000 {
		recs = append(recs, "⚠️ 警告：P95响应时间超过1秒，需要关注")
	}

	// [FIX #1] 基于慢请求比例 — 加防除零保护
	slowRate := safeDiv(float64(a.TimeStats.SlowCount), float64(a.RequestCount)) * 100
	if slowRate > 10 {
		recs = append(recs, fmt.Sprintf("🚨 严重：慢请求比例%.1f%%，性能问题严重", slowRate))
	} else if slowRate > 5 {
		recs = append(recs, fmt.Sprintf("⚠️ 警告：慢请求比例%.1f%%，需要优化", slowRate))
	}

	// 基于错误率
	var errorRate float64
	for _, s := range a.StatusDistribution {
		if s.StatusCode >= 400 {
			errorRate += s.Percentage
		}
	}
	if errorRate > 10 {
		recs = append(recs, fmt.Sprintf("🚨 严重：错误率%.1f%%，需要立即排查", errorRate))
	} else if errorRate > 5 {
		recs = append(recs, fmt.Sprintf("⚠️ 警告：错误率%.1f%%，需要关注", errorRate))
	}

	// 基于响应时间标准差
	if a.TimeStats.StdDevTime > 1000 {
		recs = append(recs, "📊 波动大：响应时间波动很大，可能存在不稳定因素")
	}

	// [FIX] 基于相关性分析 — 加nil保护
	if a.Correlation.SizeTimeCorr > 0.7 {
		recs = append(recs, "📦 大小相关：响应时间与响应大小高度相关，考虑压缩或分页")
	}

	// [FIX #2+#4] 基于时间段 — 修复切片越界 + 修正误导性变量名
	var nightCount, dayCount int64
	for _, h := range a.HourlyStats {
		hourStr := safeExtractHour(h.Hour)
		if hourStr == "" {
			continue // 跳过格式异常数据
		}
		hInt, err := strconv.Atoi(hourStr)
		if err != nil {
			continue
		}
		if hInt >= 20 || hInt <= 6 { // 晚上8点到早上6点
			nightCount += int64(h.RequestCount)
		} else {
			dayCount += int64(h.RequestCount)
		}
	}

	if len(a.HourlyStats) > 0 {
		nightRate := safeDiv(float64(nightCount), float64(a.RequestCount)) * 100
		if nightRate > 70 {
			recs = append(recs, "🌙 夜间访问：主要访问集中在夜间，考虑优化夜间性能")
		}
	}

	// [FIX #1] 基于客户端 — 加防除零保护
	if len(a.TopClients) > 0 {
		topClientRate := safeDiv(float64(a.TopClients[0].RequestCount), float64(a.RequestCount)) * 100
		if topClientRate > 50 {
			recs = append(recs, "🎯 客户端集中：单一客户端访问比例过高，可能为爬虫或API调用")
		}
	}

	// 基于UserAgent
	var crawlerRate float64
	for _, ua := range a.UserAgents {
		if ua.Category == "Crawler" {
			crawlerRate += ua.Percentage
		}
	}
	if crawlerRate > 30 {
		recs = append(recs, fmt.Sprintf("🤖 爬虫流量：%.1f%%的请求来自爬虫，考虑优化爬虫策略", crawlerRate))
	}

	a.Recommendations = recs
}

// safeDiv 安全除法，分母为0时返回0避免 panic
func safeDiv(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

// safeExtractHour 从时间字符串中安全提取小时部分
// 支持格式: "2006-01-02T15:04:05" 或 "2006-01-02 15:04:05"
func safeExtractHour(timeStr string) string {
	if len(timeStr) < 13 {
		return ""
	}
	return timeStr[11:13]
}

type PathTimeStats struct {
	AvgTime       float64 `json:"avg_time"`
	P50Time       float64 `json:"p50_time"`
	P90Time       float64 `json:"p90_time"`
	P95Time       float64 `json:"p95_time"`
	P99Time       float64 `json:"p99_time"`
	MaxTime       float64 `json:"max_time"`
	MinTime       float64 `json:"min_time"`
	StdDevTime    float64 `json:"std_dev_time"`
	SlowCount     int64   `json:"slow_count"`      // >1秒的请求
	VerySlowCount int64   `json:"very_slow_count"` // >3秒的请求
	TimeoutCount  int64   `json:"timeout_count"`   // >10秒的请求
}

type StatusDistributionAvgTime struct {
	StatusCode int     `json:"status_code"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
	AvgTime    float64 `json:"avg_time"`
}

type HourlyPathStats struct {
	Hour         string  `json:"hour"`
	RequestCount int64   `json:"request_count"`
	AvgTime      float64 `json:"avg_time"`
	P95Time      float64 `json:"p95_time"`
	ErrorCount   int64   `json:"error_count"`
	PeakRPS      float64 `json:"peak_rps"`
}

type TopClient struct {
	IP           string  `json:"ip"`
	RequestCount int64   `json:"request_count"`
	AvgTime      float64 `json:"avg_time"`
	ErrorCount   int64   `json:"error_count"`
	LastSeen     string  `json:"last_seen"`
	Country      string  `json:"country,omitempty"`
	City         string  `json:"city,omitempty"`
}

type UserAgentDistribution struct {
	UserAgent  string  `json:"user_agent"`
	Category   string  `json:"category"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
	AvgTime    float64 `json:"avg_time"`
	ErrorRate  float64 `json:"error_rate"`
}

type SlowRequestDetail struct {
	Timestamp    string  `json:"timestamp"`
	ResponseTime float64 `json:"response_time"`
	Status       int     `json:"status"`
	SizeKB       float64 `json:"size_kb"`
	IP           string  `json:"ip"`
	UserAgent    string  `json:"user_agent"`
	Referer      string  `json:"referer,omitempty"`
}

type CorrelationAnalysis struct {
	SizeTimeCorr   float64  `json:"size_time_correlation"`
	StatusTimeCorr float64  `json:"status_time_correlation"`
	HourTimeCorr   float64  `json:"hour_time_correlation"`
	Patterns       []string `json:"patterns"`
}

type TopIP struct {
	IP           string  `json:"ip"`
	RequestCount int64   `json:"request_count"`
	UniquePaths  int64   `json:"unique_paths"`
	AvgBytes     float64 `json:"avg_bytes"`
	Errors       int64   `json:"errors"`
	ErrorRate    float64 `json:"error_rate"`
	BytesRate    float64 `json:"bytes_rate"` // 每秒字节数
}

type HourlyStats struct {
	Hour         time.Time `json:"hour"`
	RequestCount int64     `json:"request_count"`
	UniqueIPs    int64     `json:"unique_ips"`
	TotalBytes   int64     `json:"total_bytes"`
	ErrorCount   int64     `json:"error_count"`
	PeakRPS      float64   `json:"peak_rps"`
}

type StatusDistribution struct {
	StatusCode int     `json:"status_code"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
	Category   string  `json:"category"`
}

type BrowserStats struct {
	BrowserType string  `json:"browser_type"`
	Count       int64   `json:"count"`
	UniqueIPs   int64   `json:"unique_ips"`
	Percentage  float64 `json:"percentage"`
}

type IPAnalysis struct {
	IP           string  `json:"ip"`
	RequestCount int64   `json:"request_count"`
	TotalBytes   int64   `json:"total_bytes"`
	LastSeen     string  `json:"last_seen"`
	IsSuspicious bool    `json:"is_suspicious"`
	RequestRate  float64 `json:"request_rate"`
	ErrorRate    float64 `json:"error_rate"`
	UserAgent    string  `json:"user_agent,omitempty"`
	Country      string  `json:"country,omitempty"`
}

// CountryStats 国家/地区访问统计
type CountryStats struct {
	Country      string  `json:"country"`
	RequestCount int64   `json:"request_count"`
	UniqueIPs    int64   `json:"unique_ips"`
	Percentage   float64 `json:"percentage"`
	ErrorRate    float64 `json:"error_rate"`
}

// UVTrend UV趋势数据
type UVTrend struct {
	Date        string  `json:"date"`
	NewUV       int64   `json:"new_uv"`        // 新访客数
	ReturningUV int64   `json:"returning_uv"`  // 回访客数
	TotalUV     int64   `json:"total_uv"`      // 总UV = NewUV + ReturningUV
	PV          int64   `json:"pv"`            // 页面浏览量(PV)
	AvgPVPerUV  float64 `json:"avg_pv_per_uv"` // 人均PV
}

// UVDistribution UV分布概览
type UVDistribution struct {
	TotalUV      int64    `json:"total_uv"`           // 总独立访客
	TotalPV      int64    `json:"total_pv"`           // 总页面浏览量
	NewUV        int64    `json:"new_uv"`             // 新访客数
	ReturningUV  int64    `json:"returning_uv"`       // 回访客数
	NewUVRatio   float64  `json:"new_uv_ratio"`       // 新访客占比
	AvgPVPerUser float64  `json:"avg_pv_per_user"`    // 人均PV
	BounceRate   float64  `json:"bounce_rate"`        // 跳出率(只访问1次的IP占比)
	HotHourUVs   []HourUV `json:"hour_uvs,omitempty"` // 各时段UV分布
}

// HourUV 小时级UV
type HourUV struct {
	Hour string `json:"hour"`
	UV   int64  `json:"uv"`
	PV   int64  `json:"pv"`
}
