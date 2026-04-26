package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"log-analyzer/internal/analyzer"
	"log-analyzer/internal/model"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

type Handler struct {
	analyzer *analyzer.LogAnalyzer
	logger   zerolog.Logger
}

func NewHandler(analyzer *analyzer.LogAnalyzer, logger zerolog.Logger) *Handler {
	return &Handler{
		analyzer: analyzer,
		logger:   logger,
	}
}

// Success 成功响应
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, model.APIResponse{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// Error 错误响应
func Error(c *gin.Context, code int, message string) {
	c.JSON(code, model.APIResponse{
		Code:    code,
		Message: message,
	})
}

// GetRealTimeStats 获取实时统计
// @Summary 获取实时统计
// @Description 获取最近N小时的实时统计信息
// @Tags stats
// @Accept json
// @Produce json
// @Param hours query int false "小时数，默认1" minimum(1) maximum(24)
// @Success 200 {object} model.APIResponse{data=model.RealTimeStats}
// @Router /api/stats/realtime [get]
func (h *Handler) GetRealTimeStats(c *gin.Context) {
	hoursStr := c.DefaultQuery("hours", "1")
	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours < 1 || hours > 24 {
		Error(c, 400, "invalid hours parameter")
		return
	}

	stats, err := h.analyzer.GetRealTimeStats(c.Request.Context(), hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get realtime stats")
		Error(c, 500, "failed to get realtime stats")
		return
	}

	Success(c, stats)
}

// GetTopPaths 获取热门路径
// @Summary 获取热门访问路径
// @Description 获取最近N小时的热门访问路径
// @Tags stats
// @Accept json
// @Produce json
// @Param limit query int false "返回数量，默认10" minimum(1) maximum(100)
// @Param hours query int false "小时数，默认24" minimum(1) maximum(168)
// @Success 200 {object} model.APIResponse{data=[]model.TopPath}
// @Router /api/stats/top-paths [get]
func (h *Handler) GetTopPaths(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	hoursStr := c.DefaultQuery("hours", "24")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 || limit > 100 {
		Error(c, 400, "invalid limit parameter")
		return
	}

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours < 1 || hours > 168 {
		Error(c, 400, "invalid hours parameter")
		return
	}

	paths, err := h.analyzer.GetTopPaths(c.Request.Context(), limit, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get top paths")
		Error(c, 500, "failed to get top paths")
		return
	}

	Success(c, paths)
}

// internal/api/handlers.go 新增方法

// GetSlowPaths 获取慢路径
// @Summary 获取慢路径列表
// @Description 查询响应时间最长的路径
// @Tags analysis
// @Accept json
// @Produce json
// @Param hours query int false "小时数，默认24" minimum(1) maximum(168)
// @Param limit query int false "返回数量，默认20" minimum(1) maximum(100)
// @Param minRequests query int false "最小请求数，默认10" minimum(1) maximum(1000)
// @Success 200 {object} model.APIResponse{data=[]model.SlowPathAnalysis}
// @Router /api/stats/slow-paths [get]
func (h *Handler) GetSlowPaths(c *gin.Context) {
	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	minRequests, _ := strconv.Atoi(c.DefaultQuery("minRequests", "10"))
	skipOther, _ := strconv.ParseBool(c.DefaultQuery("skipOther", "false"))

	if hours < 1 || hours > 168 {
		Error(c, 400, "hours must be between 1 and 168")
		return
	}

	if limit < 1 || limit > 100 {
		Error(c, 400, "limit must be between 1 and 100")
		return
	}

	paths, err := h.analyzer.GetSlowPaths(c.Request.Context(), limit, hours, minRequests, skipOther)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get slow paths")
		Error(c, 500, "failed to get slow paths")
		return
	}

	Success(c, paths)
}

// AnalyzePath 深度分析路径
// @Summary 深度分析特定路径
// @Description 对特定路径进行深度性能分析
// @Tags analysis
// @Accept json
// @Produce json
// @Param path query string true "路径"
// @Param hours query int false "小时数，默认24"
// @Success 200 {object} model.APIResponse{data=model.PathAnalysisDetail}
// @Router /api/stats/analyze-path [get]
func (h *Handler) AnalyzePath(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		Error(c, 400, "path parameter is required")
		return
	}

	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	if hours < 1 || hours > 720 {
		Error(c, 400, "hours must be between 1 and 168")
		return
	}

	analysis, err := h.analyzer.AnalyzePath(c.Request.Context(), path, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to analyze path")
		Error(c, 500, "failed to analyze path")
		return
	}

	Success(c, analysis)
}

// GetPathDetail 获取路径详情
// @Summary 获取路径详情
// @Description 获取特定路径的详细信息
// @Tags analysis
// @Accept json
// @Produce json
// @Param path query string true "路径"
// @Param hours query int false "小时数，默认24"
// @Success 200 {object} model.APIResponse{data=map[string]interface{}}
// @Router /api/stats/path-detail [get]
func (h *Handler) GetPathDetail(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		Error(c, 400, "path parameter is required")
		return
	}

	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	detail, err := h.analyzer.GetPathDetail(c.Request.Context(), path, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get path detail")
		Error(c, 500, "failed to get path detail")
		return
	}
	Success(c, detail)
}

// GetSlowRequests 获取慢请求列表
// @Summary 获取慢请求列表
// @Description 获取特定路径的慢请求详情
// @Tags analysis
// @Accept json
// @Produce json
// @Param path query string true "路径"
// @Param threshold query float64 false "阈值(秒)，默认1.0"
// @Param limit query int false "返回数量，默认50"
// @Success 200 {object} model.APIResponse{data=[]map[string]interface{}}
// @Router /api/stats/slow-requests [get]
func (h *Handler) GetSlowRequests(c *gin.Context) {
	path := c.Query("path")
	if path == "" {
		Error(c, 400, "path parameter is required")
		return
	}

	threshold, _ := strconv.ParseFloat(c.DefaultQuery("threshold", "0.5"), 64)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))

	results, err := h.analyzer.GetSlowRequests(c.Request.Context(), path, threshold, limit, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get slow requests")
		Error(c, 500, "failed to get slow requests")
		return
	}

	Success(c, results)
}

// GetTopIPs 获取热门IP
// @Summary 获取热门访问IP
// @Description 获取最近N小时的热门访问IP
// @Tags stats
// @Accept json
// @Produce json
// @Param limit query int false "返回数量，默认10" minimum(1) maximum(100)
// @Param hours query int false "小时数，默认24" minimum(1) maximum(168)
// @Success 200 {object} model.APIResponse{data=[]model.TopIP}
// @Router /api/stats/top-ips [get]
func (h *Handler) GetTopIPs(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "10")
	hoursStr := c.DefaultQuery("hours", "24")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 || limit > 100 {
		Error(c, 400, "invalid limit parameter")
		return
	}

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours < 1 || hours > 168 {
		Error(c, 400, "invalid hours parameter")
		return
	}

	ips, err := h.analyzer.GetTopIPs(c.Request.Context(), limit, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get top IPs")
		Error(c, 500, "failed to get top IPs")
		return
	}

	Success(c, ips)
}

// GetHourlyStats 获取小时统计
// @Summary 获取小时级统计
// @Description 获取最近N天的小时级统计
// @Tags stats
// @Accept json
// @Produce json
// @Param days query int false "天数，默认7" minimum(1) maximum(30)
// @Success 200 {object} model.APIResponse{data=[]model.HourlyStats}
// @Router /api/stats/hourly [get]
func (h *Handler) GetHourlyStats(c *gin.Context) {
	daysStr := c.DefaultQuery("days", "7")
	days, err := strconv.Atoi(daysStr)
	if err != nil || days < 1 || days > 30 {
		Error(c, 400, "invalid days parameter")
		return
	}

	stats, err := h.analyzer.GetHourlyStats(c.Request.Context(), days)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get hourly stats")
		Error(c, 500, "failed to get hourly stats")
		return
	}

	Success(c, stats)
}

// GetStatusDistribution 获取状态码分布
// @Summary 获取状态码分布
// @Description 获取最近24小时的状态码分布
// @Tags stats
// @Accept json
// @Produce json
// @Success 200 {object} model.APIResponse{data=[]model.StatusDistribution}
// @Router /api/stats/status-distribution [get]
func (h *Handler) GetStatusDistribution(c *gin.Context) {
	dist, err := h.analyzer.GetStatusDistribution(c.Request.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get status distribution")
		Error(c, 500, "failed to get status distribution")
		return
	}

	Success(c, dist)
}

// GetTopCountries 获取国家访问统计
// @Summary 获取国家访问统计
// @Description 获取最近N小时按国家分组的访问统计
// @Tags stats
// @Accept json
// @Produce json
// @Param limit query int false "返回数量，默认20" minimum(1) maximum(200)
// @Param hours query int false "小时数，默认24" minimum(1) maximum(168)
// @Success 200 {object} model.APIResponse{data=[]model.CountryStats}
// @Router /api/stats/countries [get]
func (h *Handler) GetTopCountries(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "20")
	hoursStr := c.DefaultQuery("hours", "24")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 || limit > 200 {
		Error(c, 400, "invalid limit parameter")
		return
	}

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours < 1 || hours > 168 {
		Error(c, 400, "invalid hours parameter")
		return
	}

	countries, err := h.analyzer.GetTopCountries(c.Request.Context(), limit, hours)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get top countries")
		Error(c, 500, "failed to get top countries")
		return
	}

	Success(c, countries)
}

// GetUVTrend 获取UV趋势
// @Summary 获取UV趋势数据
// @Description 获取最近N天的UV(独立访客)趋势，包含新访客/回访客/人均PV
// @Tags stats
// @Accept json
// @Produce json
// @Param days query int false "天数，默认7" minimum(1) maximum(30)
// @Success 200 {object} model.APIResponse{data=[]model.UVTrend}
// @Router /api/stats/uv-trend [get]
func (h *Handler) GetUVTrend(c *gin.Context) {
	daysStr := c.DefaultQuery("days", "7")
	days, err := strconv.Atoi(daysStr)
	if err != nil || days < 1 || days > 30 {
		Error(c, 400, "invalid days parameter")
		return
	}

	trend, err := h.analyzer.GetUVTrend(c.Request.Context(), days)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get uv trend")
		Error(c, 500, "failed to get uv trend")
		return
	}

	Success(c, trend)
}

// GetUVDistribution 获取UV分布概览
// @Summary 获取UV分布概览
// @Description 获取最近N天的UV分布统计，含新访客率、跳出率、各时段UV等
// @Tags stats
// @Accept json
// @Produce json
// @Param days query int false "天数，默认7" minimum(1) maximum(30)
// @Success 200 {object} model.APIResponse{data=model.UVDistribution}
// @Router /api/stats/uv-distribution [get]
func (h *Handler) GetUVDistribution(c *gin.Context) {
	daysStr := c.DefaultQuery("days", "7")
	days, err := strconv.Atoi(daysStr)
	if err != nil || days < 1 || days > 30 {
		Error(c, 400, "invalid days parameter")
		return
	}

	dist, err := h.analyzer.GetUVDistribution(c.Request.Context(), days)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get uv distribution")
		Error(c, 500, "failed to get uv distribution")
		return
	}

	Success(c, dist)
}

// GetSuspiciousIPs 获取可疑IP
// @Summary 获取可疑IP列表
// @Description 获取最近1小时内请求异常频繁的IP
// @Tags security
// @Accept json
// @Produce json
// @Param threshold query int false "阈值，默认100" minimum(10) maximum(1000)
// @Success 200 {object} model.APIResponse{data=[]model.IPAnalysis}
// @Router /api/stats/suspicious-ips [get]
func (h *Handler) GetSuspiciousIPs(c *gin.Context) {
	thresholdStr := c.DefaultQuery("threshold", "100")
	threshold, err := strconv.Atoi(thresholdStr)
	if err != nil || threshold < 10 || threshold > 1000 {
		Error(c, 400, "invalid threshold parameter")
		return
	}

	ips, err := h.analyzer.GetSuspiciousIPs(c.Request.Context(), threshold)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get suspicious IPs")
		Error(c, 500, "failed to get suspicious IPs")
		return
	}

	Success(c, ips)
}

// ExecuteCustomQuery 执行自定义查询
// @Summary 执行自定义SQL查询
// @Description 执行自定义SQL查询（仅限SELECT）
// @Tags query
// @Accept json
// @Produce json
// @Param query body map[string]interface{} true "查询参数"
// @Success 200 {object} model.APIResponse{data=[]map[string]interface{}}
// @Router /api/query [post]
func (h *Handler) ExecuteCustomQuery(c *gin.Context) {
	var req struct {
		SQL    string        `json:"sql" binding:"required"`
		Params []interface{} `json:"params,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn().Err(err).Msg("custom query: invalid request body")
		Error(c, 400, "invalid request format")
		return
	}

	// [FIX #3] 长度限制：防止超长SQL导致资源耗尽
	const maxSQLLength = 10000
	if len(req.SQL) > maxSQLLength {
		Error(c, 400, "query too long, max length is "+strconv.Itoa(maxSQLLength))
		return
	}

	// [FIX #1+#4] 增强安全检查：
	// 1) 必须以 SELECT 开头（防止 INSERT/DELETE/DROP 等非查询操作）
	// 2) 禁止分号分隔的多语句注入
	// 3) 禁止嵌套的危险关键字
	sqlUpper := strings.ToUpper(strings.TrimSpace(req.SQL))
	if !strings.HasPrefix(sqlUpper, "SELECT") {
		Error(c, 400, "only SELECT queries are allowed")
		return
	}

	dangerousKeywords := []string{
		";", "--", "/*", "*/", // 注释与语句分隔符
		"DROP ", "DELETE ", "INSERT ", "UPDATE ", // DML 操作
		"ALTER ", "CREATE ", "TRUNCATE ", // DDL 操作
		"EXEC ", "EXECUTE ", "CALL ", "GRANT ", // 存储过程与权限
		"INTO OUTFILE ", "INTO DUMPFILE ", // 文件写入
	}
	for _, kw := range dangerousKeywords {
		if strings.Contains(sqlUpper, kw) {
			h.logger.Warn().Str("sql", req.SQL).Str("keyword", kw).
				Msg("custom query: blocked dangerous keyword")
			Error(c, 400, "query contains disallowed operations")
			return
		}
	}

	results, err := h.analyzer.GetCustomQuery(c.Request.Context(), req.SQL, req.Params...)
	if err != nil {
		// [FIX #2] 日志记录完整错误细节，但只向客户端返回通用提示，避免信息泄露
		h.logger.Error().Err(err).Str("sql", req.SQL).Msg("failed to execute custom query")
		Error(c, 500, "failed to execute query, please check server logs")
		return
	}

	Success(c, results)
}

// RefreshData 刷新数据
// @Summary 刷新数据
// @Description 强制刷新数据，重新加载Parquet文件
// @Tags system
// @Accept json
// @Produce json
// @Success 200 {object} model.APIResponse
// @Router /api/refresh [post]
func (h *Handler) RefreshData(c *gin.Context) {
	if err := h.analyzer.Refresh(); err != nil {
		h.logger.Error().Err(err).Msg("failed to refresh data")
		Error(c, 500, "failed to refresh data")
		return
	}

	Success(c, gin.H{"status": "refreshed", "time": time.Now()})
}

// HealthCheck 健康检查
// @Summary 健康检查
// @Description 服务健康检查接口
// @Tags system
// @Accept json
// @Produce json
// @Success 200 {object} model.APIResponse
// @Router /health [get]
func (h *Handler) HealthCheck(c *gin.Context) {
	// 检查数据库连接
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := h.analyzer.PingDB(ctx); err != nil {
		Error(c, 503, "database connection failed")
		return
	}

	Success(c, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// DebugInfo 获取调试信息
// @Summary 获取调试信息
// @Description 获取数据库和系统状态信息
// @Tags system
// @Accept json
// @Produce json
// @Success 200 {object} model.APIResponse{data=map[string]interface{}}
// @Router /api/debug [get]
func (h *Handler) DebugInfo(c *gin.Context) {
	info := h.analyzer.DebugInfo()
	Success(c, info)
}
