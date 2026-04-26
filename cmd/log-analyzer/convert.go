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
	flag.StringVar(&output, "output", "output.parquet", "输出parquet文件或目录路径")
	flag.StringVar(&format, "format", "combined", "日志格式: combined, common, combined2")
	flag.StringVar(&geoipDB, "geoip-db", "", "GeoIP数据库路径(如 GeoLite2-City.mmdb)，用于自动填充国家/城市")
	flag.BoolVar(&partition, "partition", false, "是否按年月日时分区输出到目录")
	flag.BoolVar(&overwrite, "overwrite", false, "是否覆盖输出文件")
	flag.DurationVar(&queryTimeout, "timeout", 10*time.Minute, "查询超时时间")
	flag.StringVar(&startTime, "start-time", "", "开始时间")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `log-analyzer convert - 将Nginx/Apache日志转换为Parquet格式

用法:
  log-analyzer convert -input <日志文件> -output <输出.parquet> [选项]

示例:
  # 基本转换
  log-analyzer convert -input access.log -output logs.parquet

  # 指定开始时间
  log-analyzer convert -input access.log -output logs.parquet --start-time "2006-01-02T15:04:05"

  # 指定日志格式
  log-analyzer convert -input access.log -output logs.parquet -format combined

  # 带地理位置解析
  log-analyzer convert -input access.log -output logs.parquet --geoip-db GeoLite2-City.mmdb

  # 分区输出（按年/月/日/时）
  log-analyzer convert -input access.log -output output_dir/ --partition


选项:
`)
		flag.PrintDefaults()
	}

	flag.Parse()

	if input == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须指定输入日志文件 (-input)")
		flag.Usage()
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
	if len(geoipDB) > 0 {
		cfg.GeoIPDBPath = geoipDB
	}
	if fi, err := os.Stat(output); err == nil {
		if fi.IsDir() {
			cfg.LogDirectory = output
			if !partition {
				output = filepath.Join(output, `output.parquet`)
			}
		} else {
			cfg.LogDirectory = filepath.Dir(output)
			if partition {
				output = cfg.LogDirectory
			}
		}
	} else {
		if partition {
			cfg.LogDirectory = output
		} else {
			cfg.LogDirectory = filepath.Dir(output)
		}
	}

	a, err := analyzer.NewAnalyzer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("初始化分析器失败")
	}
	defer a.Close()

	if err := a.InstallHttpdLogModule(); err != nil {
		logger.Fatal().Err(err).Msg("安装httpd_log扩展失败")
	}

	a.SetLogFile(input, format)

	start := time.Now()

	var whereStart time.Time
	if startTime != "" {
		var err error
		whereStart, err = time.Parse(time.RFC3339, startTime)
		if err != nil {
			startTime = strings.ReplaceAll(startTime, `T`, ` `)
			whereStart, err = time.ParseInLocation(time.DateTime, startTime, time.Local)
			if err != nil {
				logger.Fatal().Err(err).Msg("解析开始时间失败")
			}
		}
	} else {
		t, err := a.GetNewestTimestamp(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("获取最新时间失败")
		}
		if t.Time.IsZero() {
			whereStart = time.Now().AddDate(0, 0, -30)
		} else {
			whereStart = t.Time.Local().Add(time.Nanosecond)
		}
	}

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
