package main

import (
	"context"
	"fmt"
	"io"
	"log-analyzer/internal/analyzer"
	"log-analyzer/internal/api"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ========== Server Config & Helpers ==========

type Config struct {
	Server struct {
		Host string `mapstructure:"host"`
		Port int    `mapstructure:"port"`
	} `mapstructure:"server"`

	Analyzer analyzer.Config `mapstructure:"analyzer"`

	Log struct {
		Level    string `mapstructure:"level"`
		FilePath string `mapstructure:"file_path"`
		MaxSize  int    `mapstructure:"max_size"`
		MaxAge   int    `mapstructure:"max_age"`
	} `mapstructure:"log"`
}

func loadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("/etc/log-analyzer")

	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("analyzer.log_directory", "/data/logs")
	viper.SetDefault("analyzer.refresh_interval", "5m")
	viper.SetDefault("analyzer.cache_ttl", "1m")
	viper.SetDefault("analyzer.max_connections", 10)
	viper.SetDefault("analyzer.query_timeout", "30s")
	viper.SetDefault("log.level", "info")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warn().Msg("config file not found, using defaults")
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func setupLogger(cfg *Config) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Log.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	var writers []io.Writer
	writers = append(writers, zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	})

	if cfg.Log.FilePath != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.Log.FilePath,
			MaxSize:    cfg.Log.MaxSize,
			MaxAge:     cfg.Log.MaxAge,
			MaxBackups: 3,
			Compress:   true,
		}
		writers = append(writers, fileWriter)
	}

	multiWriter := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()

	return logger
}

func setupRouter(cfg *Config, a *analyzer.LogAnalyzer, logger zerolog.Logger) *gin.Engine {
	if cfg.Log.Level == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	router.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()
		errorMessage := c.Errors.ByType(gin.ErrorTypePrivate).String()

		event := logger.Info()
		if statusCode >= 400 {
			event = logger.Error()
		}

		event.Str("client_ip", clientIP).
			Str("method", method).
			Int("status", statusCode).
			Str("path", path).
			Str("query", query).
			Str("latency", latency.String()).
			Msg(errorMessage)
	})

	router.Static("/static", "./web/static")
	router.StaticFile("/", "./web/static/index.html")
	router.StaticFile("/slow-paths.html", "./web/static/slow-paths.html")
	router.StaticFile("/analyze-path.html", "./web/static/analyze-path.html")

	apiHandler := api.NewHandler(a, logger)
	apiGroup := router.Group("/api")
	{
		apiGroup.GET("/stats/realtime", apiHandler.GetRealTimeStats)
		apiGroup.GET("/stats/top-paths", apiHandler.GetTopPaths)
		apiGroup.GET("/stats/slow-paths", apiHandler.GetSlowPaths)
		apiGroup.GET("/stats/analyze-path", apiHandler.AnalyzePath)
		apiGroup.GET("/stats/path-detail", apiHandler.GetPathDetail)
		apiGroup.GET("/stats/slow-requests", apiHandler.GetSlowRequests)
		apiGroup.GET("/stats/top-ips", apiHandler.GetTopIPs)
		apiGroup.GET("/stats/hourly", apiHandler.GetHourlyStats)
		apiGroup.GET("/stats/status-distribution", apiHandler.GetStatusDistribution)
		apiGroup.GET("/stats/countries", apiHandler.GetTopCountries)
		apiGroup.GET("/stats/uv-trend", apiHandler.GetUVTrend)
		apiGroup.GET("/stats/uv-distribution", apiHandler.GetUVDistribution)
		apiGroup.GET("/stats/suspicious-ips", apiHandler.GetSuspiciousIPs)

		apiGroup.POST("/refresh", apiHandler.RefreshData)
		apiGroup.POST("/query", apiHandler.ExecuteCustomQuery)
		apiGroup.GET("/health", apiHandler.HealthCheck)
		apiGroup.GET("/debug", apiHandler.DebugInfo)

		apiGroup.GET("/metadata", func(c *gin.Context) {
			api.Success(c, gin.H{
				"name":        "Nginx Log Analyzer",
				"version":     "1.0.0",
				"description": "Real-time Nginx log analysis system",
				"author":      "Your Team",
				"endpoints": []string{
					"GET    /api/stats/realtime",
					"GET    /api/stats/top-paths",
					"GET    /api/stats/hourly",
					"GET    /api/stats/status-distribution",
					"GET    /api/stats/suspicious-ips",
					"POST   /api/refresh",
					"POST   /api/query",
					"GET    /api/health",
				},
			})
		})
	}

	return router
}

// ========== Subcommand: server ==========

func runServer() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	logger := setupLogger(cfg)
	logger.Info().Msgf(`config: %+v`, *cfg)

	logger.Info().Msg("initializing log analyzer...")
	a, err := analyzer.NewAnalyzer(&cfg.Analyzer, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create analyzer")
	}
	defer a.Close()

	router := setupRouter(cfg, a, logger)
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info().Str("addr", addr).Msg("starting HTTP server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("failed to start server")
		}
	}()

	<-quit
	logger.Info().Msg("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("server forced to shutdown")
	}

	logger.Info().Msg("server exited")
}
