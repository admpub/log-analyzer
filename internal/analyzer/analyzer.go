package analyzer

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"log-analyzer/internal/geo"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

type LogAnalyzer struct {
	db          *sql.DB
	logDir      string
	logFile     string
	logFormat   string // combined / common / combinedCHD / combinedD
	logParquet  bool
	logger      zerolog.Logger
	cache       *StatsCache
	refreshLock sync.RWMutex
	lastRefresh time.Time
	config      *Config
	cancel      context.CancelFunc
	geoIP       *geo.GeoIP
}

func NewAnalyzer(cfg *Config, logger zerolog.Logger) (*LogAnalyzer, error) {
	cfg.SetDefaults()
	if !isValidIdentifier(cfg.LogTable) {
		return nil, fmt.Errorf("invalid log table name: contains unsafe characters")
	}
	// 初始化DuckDB连接
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	analyzer := &LogAnalyzer{
		db:         db,
		logDir:     cfg.LogDirectory,
		logFile:    cfg.LogFile,
		logFormat:  cfg.LogFormat,
		logParquet: cfg.LogParquet,
		logger:     logger,
		cache:      NewStatsCache(cfg.CacheTTL),
		config:     cfg,
	}

	// 初始化GeoIP（如果配置了数据库路径）
	if len(cfg.GeoIPDBPath) > 0 {
		geoIP, err := geo.NewGeoIP(cfg.GeoIPDBPath, logger)
		if err != nil {
			logger.Warn().Err(err).Str("path", cfg.GeoIPDBPath).Msg("failed to init GeoIP, country/city will be empty")
		} else {
			analyzer.geoIP = geoIP
			logger.Info().Str("path", cfg.GeoIPDBPath).Msg("GeoIP initialized successfully")
		}
	}

	// 初始化数据库视图
	if err := analyzer.initDatabase(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to init database: %w", err)
	}

	// 启动自动刷新
	if analyzer.config.RefreshInterval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		analyzer.cancel = cancel
		go analyzer.startAutoRefresh(ctx)
	}

	return analyzer, nil
}

func (a *LogAnalyzer) generateDateDirs() (dateDirs []string) {
	now := time.Now()

	startTime := a.config.MinStartTime
	if startTime.IsZero() {
		startTime = minStartTime()
	}

	year := startTime.Year()
	month := startTime.Month()
	day := startTime.Day()
	dateDirs = []string{fmt.Sprintf("year=%d/month=%d/day=%d", year, month, day)}
	if year == now.Year() && month == now.Month() {
		for d, end := day+1, now.Day(); d <= end; d++ {
			dateDirs = append(dateDirs, fmt.Sprintf("year=%d/month=%d/day=%d", year, month, d))
		}
		return dateDirs
	}
	nextMonth := startTime.AddDate(0, 1, 0)
	nextMonthFisrtDay := time.Date(nextMonth.Year(), nextMonth.Month(), 1, 0, 0, 0, 0, nextMonth.Location())
	for d, end := day+1, nextMonthFisrtDay.AddDate(0, 0, -1).Day(); d <= end; d++ {
		dateDirs = append(dateDirs, fmt.Sprintf("year=%d/month=%d/day=%d", year, month, d))
	}

	years := now.Year() - startTime.Year()
	if years > 0 {
		for mo := month + 1; mo <= 12; mo++ {
			dateDirs = append(dateDirs, fmt.Sprintf("year=%d/month=%d/**", year, mo))
		}
		for i := 1; i <= years; i++ {
			dateDirs = append(dateDirs, fmt.Sprintf("year=%d/**/**", year+i))
		}
	} else if now.Month() > month {
		for mo := month + 1; mo <= now.Month(); mo++ {
			dateDirs = append(dateDirs, fmt.Sprintf("year=%d/month=%d/**", year, mo))
		}
	}

	return dateDirs
}

func (a *LogAnalyzer) getParquetFiles() (parquetFiles []string, err error) {
	if len(a.logDir) > 0 {
		partterns := []string{
			filepath.Join(a.logDir, "**", "*.parquet"),
			filepath.Join(a.logDir, "*.parquet"),
			//filepath.Join(a.logDir, "**", "**", "**", "**", "*.parquet"), // 年/月/日/时
		}
		// 查找所有Parquet文件
		var _parquetFiles []string
		var _err error
		for _, pattern := range partterns {
			a.logger.Info().Str("pattern", pattern).Msg("pattern")
			_parquetFiles, _err = filepath.Glob(pattern)
			if _err != nil {
				a.logger.Warn().Err(_err).Msg("error")
				continue
			}
			if len(_parquetFiles) == 0 {
				continue
			}
			parquetFiles = append(parquetFiles, _parquetFiles...)
			break
		}
		if len(parquetFiles) > 0 {
			return
		}
		dateDirs := a.generateDateDirs()
		for _, ymd := range dateDirs {
			pattern := filepath.Join(a.logDir, ymd, "**", "*.parquet") // 年/月/日/时
			a.logger.Info().Str("pattern", pattern).Msg("pattern")
			_parquetFiles, _err = filepath.Glob(pattern)
			if _err != nil {
				a.logger.Warn().Err(_err).Msg("error")
				continue
			}
			if len(_parquetFiles) == 0 {
				continue
			}
			parquetFiles = append(parquetFiles, _parquetFiles...)
		}

		if _err != nil {
			err = fmt.Errorf("failed to find parquet files: %w", _err)
			return
		}

	} else if a.logParquet && a.logFile != `` {
		parquetFiles = append(parquetFiles, a.logFile)
	}
	return
}

const tableColumns = `remote_addr as ip,
COALESCE(method, '') as method,
COALESCE(path, '') as path,
COALESCE(protocol, '') as protocol,
COALESCE(status, 0) as status,
COALESCE(body_bytes_sent, 0) as bytes_sent,
COALESCE(http_user_agent, '') as user_agent,
COALESCE(http_referer, '') as referer,
COALESCE(request_time, 0.0) as request_time,
COALESCE(country, 'Unknown') as country,
COALESCE(city, '') as city,
timestamp,
year,
month,
day,
hour`

func (a *LogAnalyzer) initDatabase(blocked ...bool) error {
	parquetFiles, err := a.getParquetFiles()
	if err != nil {
		return err
	}
	if len(parquetFiles) == 0 {
		a.logger.Warn().Msg("no parquet files found")
		return nil
	}

	viewMode := true
	var tableType string
	if viewMode {
		tableType = `VIEW`
	} else {
		tableType = `TEMP TABLE`
	}

	// 创建临时表
	createSQL := `
	CREATE OR REPLACE ` + tableType + ` ` + a.config.LogTable + ` AS
	SELECT ` + tableColumns + ` FROM read_parquet([`

	// 添加所有Parquet文件路径
	fileList := make([]string, len(parquetFiles))
	for i, file := range parquetFiles {
		fileList[i] = fmt.Sprintf("'%s'", file)
	}

	createSQL += strings.Join(fileList, ",") + `], union_by_name=true, filename=true, hive_partitioning=true)`

	if len(blocked) == 0 || !blocked[0] {
		a.refreshLock.Lock()
		defer a.refreshLock.Unlock()
	}

	_, err = a.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create %s table: %w", a.config.LogTable, err)
	}

	if !viewMode {
		// 创建索引
		indexSQLs := []string{
			"CREATE INDEX IF NOT EXISTS idx_time ON " + a.config.LogTable + "(timestamp)",
			"CREATE INDEX IF NOT EXISTS idx_ip ON " + a.config.LogTable + "(ip)",
			"CREATE INDEX IF NOT EXISTS idx_status ON " + a.config.LogTable + "(status)",
			"CREATE INDEX IF NOT EXISTS idx_path ON " + a.config.LogTable + "(path)",
			"CREATE INDEX IF NOT EXISTS idx_path_request_time ON " + a.config.LogTable + "(path, request_time)",
			"CREATE INDEX IF NOT EXISTS idx_timestamp_request_time ON " + a.config.LogTable + "(timestamp, request_time)",
		}

		for _, sql := range indexSQLs {
			if _, err := a.db.Exec(sql); err != nil {
				a.logger.Warn().Err(err).Msg("failed to create index")
			}
		}

	}

	a.lastRefresh = time.Now()
	a.logger.Info().Int("files", len(parquetFiles)).Msg("database initialized")
	return nil
}

func (a *LogAnalyzer) initDatabaseByFile() error {
	// 递归查找所有Parquet文件
	var allFiles []string
	err := filepath.WalkDir(a.logDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".parquet") {
			allFiles = append(allFiles, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(allFiles) == 0 {
		return fmt.Errorf("no parquet files found in %s", a.logDir)
	}

	// 逐个读取并合并
	for i, file := range allFiles {
		if i == 0 {
			// 第一个文件创建表
			createSQL := fmt.Sprintf(`
            CREATE OR REPLACE TABLE `+a.config.LogTable+` AS
            SELECT `+tableColumns+` FROM read_parquet('%s')`, file)

			_, err := a.db.Exec(createSQL)
			if err != nil {
				return fmt.Errorf("failed to create table from %s: %w", file, err)
			}
		} else {
			// 后续文件插入
			insertSQL := fmt.Sprintf(`
            INSERT INTO `+a.config.LogTable+` 
            SELECT `+tableColumns+` FROM read_parquet('%s')`, file)

			_, err := a.db.Exec(insertSQL)
			if err != nil {
				a.logger.Warn().Err(err).Str("file", file).Msg("插入文件失败")
			}
		}
	}

	a.logger.Info().Int("files", len(allFiles)).Msg("数据库初始化完成")
	return nil
}

func (a *LogAnalyzer) PingDB(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

func (a *LogAnalyzer) startAutoRefresh(ctx context.Context) {
	ticker := time.NewTicker(a.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := a.Refresh(); err != nil {
				a.logger.Error().Err(err).Msg("failed to refresh data")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (a *LogAnalyzer) Refresh() error {
	a.refreshLock.Lock()
	defer a.refreshLock.Unlock()

	start := time.Now()
	defer func() {
		a.logger.Info().Dur("duration", time.Since(start)).Msg("data refresh completed")
	}()

	return a.initDatabase(true)
}

func (a *LogAnalyzer) GetNewestTimestamp(ctx context.Context) (sql.NullTime, error) {
	ts := sql.NullTime{}
	if a.lastRefresh.IsZero() {
		return ts, nil
	}
	query := `
	SELECT timestamp
	FROM ` + a.config.LogTable + `
	ORDER BY timestamp DESC
	LIMIT 1`
	err := a.db.QueryRowContext(ctx, query).Scan(&ts)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf(`failed to getNewestTimestamp: %v`, err.Error())
	}
	return ts, err
}

func (a *LogAnalyzer) DebugInfo() map[string]interface{} {
	info := make(map[string]interface{})

	// 检查表是否存在
	var tableExists bool
	err := a.db.QueryRow(`
        SELECT COUNT(*) > 0 
        FROM information_schema.tables 
        WHERE table_name = ?
    `, a.config.LogTable).Scan(&tableExists)

	if err != nil {
		info["table_check_error"] = err.Error()
	} else {
		info["table_exists"] = tableExists
	}

	// 获取表行数
	var rowCount int64
	err = a.db.QueryRow("SELECT COUNT(*) FROM " + a.config.LogTable).Scan(&rowCount)
	if err != nil {
		info["row_count_error"] = err.Error()
	} else {
		info["row_count"] = rowCount
	}

	// 获取表结构
	columns := []map[string]string{}
	rows, err := a.db.Query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = ?
        ORDER BY ordinal_position
    `, a.config.LogTable)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var colName, dataType string
			if err := rows.Scan(&colName, &dataType); err == nil {
				columns = append(columns, map[string]string{
					"name": colName,
					"type": dataType,
				})
			}
		}
		info["columns"] = columns
	} else {
		info["columns_error"] = err.Error()
	}

	// 检查Parquet文件
	parquetFiles, err := a.getParquetFiles()
	if err == nil {
		info["parquet_files_count"] = len(parquetFiles)
		if len(parquetFiles) > 0 {
			info["parquet_files_sample"] = parquetFiles[:min(3, len(parquetFiles))]
		}
	} else {
		info["parquet_files_error"] = err.Error()
	}

	info["last_refresh"] = a.lastRefresh.Format(time.RFC3339)
	info["log_directory"] = a.logDir

	return info
}

func (a *LogAnalyzer) Close() error {
	if a.cancel != nil {
		a.cancel()
	}
	a.cache.Clear()
	if a.geoIP != nil {
		if err := a.geoIP.Close(); err != nil {
			a.logger.Warn().Err(err).Msg("failed to close GeoIP")
		}
	}
	return a.db.Close()
}
