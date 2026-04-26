package main

import (
	"context"
	"flag"
	"fmt"
	"log-analyzer/internal/analyzer"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// 支持的日志格式列表
var supportedFormats = "combined, common, combinedCHD, combinedD"

// ========== Subcommand: convert ==========

func runConvert() {
	var (
		input        string
		output       string
		format       string
		geoipDB      string
		partition    bool
		queryTimeout time.Duration
		startTime    string
		overwrite    bool
	)
	flag.StringVar(&input, "input", "", "输入日志文件路径（必填）")
	flag.StringVar(&input, "i", "", "输入日志文件路径的简写")
	flag.StringVar(&output, "output", "output.parquet", "输出parquet文件或目录路径")
	flag.StringVar(&output, "o", "output.parquet", "输出parquet文件路径的简写")
	flag.StringVar(&format, "format", "combined", "日志格式: "+supportedFormats)
	flag.StringVar(&geoipDB, "geoip-db", "", "GeoIP数据库路径(如 GeoLite2-City.mmdb)，用于自动填充国家/城市")
	flag.BoolVar(&partition, "partition", false, "是否按年月日时分区输出到目录")
	flag.BoolVar(&overwrite, "overwrite", false, "是否覆盖已有输出文件")
	flag.DurationVar(&queryTimeout, "timeout", 10*time.Minute, "查询超时时间")
	flag.StringVar(&startTime, "start-time", "", "开始时间过滤 (RFC3339 或 '2006-01-02 15:04:05' 格式)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, `log-analyzer convert — 将 Nginx/Apache 日志转换为 Parquet 格式

用法:
  log-analyzer convert [选项] --input <日志文件> --output <输出.parquet>

示例:
  # 基本转换（默认 combined 格式）
  log-analyzer convert -i access.log -o logs.parquet

  # 指定开始时间过滤
  log-analyzer convert -i access.log -o logs.parquet --start-time "2026-01-01T00:00:00"

  # 指定日志格式
  log-analyzer convert -i access.log -o logs.parquet -format combined

  # 带 GeoIP 地理位置解析
  log-analyzer convert -i access.log -o logs.parquet --geoip-db GeoLite2-City.mmdb

  # 按年/月/日/时分区输出到目录
  log-analyzer convert -i access.log -o output_dir/ --partition

选项:`)
		flag.PrintDefaults()
	}

	flag.Parse()

	if input == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定输入日志文件 (--input / -i)")
		flag.Usage()
		os.Exit(1)
	}

	// 校验输入文件是否存在
	if _, err := os.Stat(input); err != nil {
		fmt.Fprintf(os.Stderr, "错误: 输入文件不存在或无法访问: %s\n", input)
		os.Exit(1)
	}

	logger := zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	}).With().Timestamp().Logger()

	ctx := context.Background()

	cfg := &analyzer.Config{
		LogFile:          input,
		LogFormat:        format,
		QueryTimeout:     queryTimeout,
		OverwriteParquet: overwrite,
		MaxConnections:   1,
	}
	if geoipDB != "" {
		cfg.GeoIPDBPath = geoipDB
	}
	cfg.LogDirectory, output = resolveLogDirAndOutput(output, partition)

	// 当非分区模式且 output 是目录时，自动拼接默认文件名
	if !partition && filepath.Base(output) == output { // output 不含路径分隔符，视为纯文件名
		if fi, err := os.Stat(output); err == nil && fi.IsDir() {
			output = filepath.Join(output, "output.parquet")
		}
	}

	a, err := analyzer.NewAnalyzer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("初始化分析器失败")
	}
	defer a.Close()

	if err := a.InstallHttpdLogModule(); err != nil {
		logger.Fatal().Err(err).Msg("安装 httpd_log 扩展失败")
	}

	a.SetLogFile(input, format)

	start := time.Now()
	whereStart := parseStartTime(startTime, logger, ctx, a)

	logger.Info().
		Str("input", input).
		Str("output", output).
		Str("format", format).
		Bool("partition", partition).
		Str("geoip_db", geoipDB).
		Str("start_time", whereStart.Format(time.RFC3339Nano)).
		Bool("overwrite", overwrite).
		Msg("开始转换日志...")

	var convertErr error
	if partition {
		convertErr = a.CovertLogFileToParquetAndPartition(ctx, whereStart, output)
	} else {
		convertErr = a.CovertLogFileToParquet(ctx, whereStart, output)
	}

	if convertErr != nil {
		logger.Fatal().Err(convertErr).Msg("转换失败")
	}

	logger.Info().
		Str("output", output).
		Dur("elapsed", time.Since(start)).
		Msg("转换完成!")
}

// resolveLogDirAndOutput 根据输出路径和分区模式推断日志数据目录
func resolveLogDirAndOutput(output string, partition bool) (logDir string, outputFileOrDir string) {
	outputFileOrDir = output
	if output == "" {
		logDir = "."
		return
	}
	// 如果 output 已是目录（已存在或以 / 结尾），直接使用
	if fi, err := os.Stat(output); err == nil {
		if fi.IsDir() {
			logDir = output
			if !partition {
				outputFileOrDir = filepath.Join(output, `output.parquet`)
			}
		} else {
			logDir = filepath.Dir(output)
			if partition {
				outputFileOrDir = logDir
			}
		}
	} else {
		if partition || strings.HasSuffix(output, string(filepath.Separator)) {
			logDir = output
			if !partition {
				outputFileOrDir = filepath.Join(output, `output.parquet`)
			}
		} else {
			logDir = filepath.Dir(output)
		}
	}
	return
}

// parseStartTime 解析开始时间参数；为空时自动从日志中获取最新时间戳
func parseStartTime(raw string, logger zerolog.Logger, ctx context.Context, a *analyzer.LogAnalyzer) time.Time {
	if raw == "" {
		t, err := a.GetNewestTimestamp(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("获取最新时间戳失败")
		}
		if t.Time.IsZero() {
			return time.Now().AddDate(0, 0, -30)
		}
		return t.Time.Local().Add(time.Nanosecond)
	}

	// 尝试 RFC3339 格式
	parsed, err := time.Parse(time.RFC3339, raw)
	if err == nil {
		return parsed
	}

	// 尝试空格分隔的 datetime 格式
	raw = strings.ReplaceAll(raw, "T", " ")
	parsed, err = time.ParseInLocation(time.DateTime, raw, time.Local)
	if err != nil {
		logger.Fatal().Err(err).Str("value", raw).Msg("开始时间格式无效，请使用 RFC3339 或 'YYYY-MM-DD HH:MM:SS' 格式")
	}
	return parsed
}
