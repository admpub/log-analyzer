package analyzer

import (
	"math"
	"time"
)

type Config struct {
	LogDirectory     string        `yaml:"log_directory" mapstructure:"log_directory"`
	LogTable         string        `yaml:"log_table" mapstructure:"log_table"`
	LogFile          string        `yaml:"log_file" mapstructure:"log_file"`
	LogFormat        string        `yaml:"log_format" mapstructure:"log_format"`
	LogParquet       bool          `yaml:"log_parquet" mapstructure:"log_parquet"`
	OverwriteParquet bool          `yaml:"overwrite_parquet" mapstructure:"overwrite_parquet"`
	GeoIPDBPath      string        `yaml:"geoip_db_path" mapstructure:"geoip_db_path"` // GeoIP数据库路径
	RefreshInterval  time.Duration `yaml:"refresh_interval" mapstructure:"refresh_interval"`
	CacheTTL         time.Duration `yaml:"cache_ttl" mapstructure:"cache_ttl"`
	MaxConnections   int           `yaml:"max_connections" mapstructure:"max_connections"`
	QueryTimeout     time.Duration `yaml:"query_timeout" mapstructure:"query_timeout"`
	MinStartTime     time.Time     `yaml:"min_start_time" mapstructure:"min_start_time"`
}

func (c *Config) SetDefaults() {
	if c.LogTable == `` {
		c.LogTable = `nginx_logs`
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = time.Minute * 5
	}
	if c.CacheTTL == 0 {
		c.CacheTTL = time.Minute
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = time.Second * 30
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 20
	}
}

// isValidIdentifier 校验表名是否为安全的 SQL 标识符
// 仅允许字母、数字、下划线，且必须以字母或下划线开头
func isValidIdentifier(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for i, ch := range name {
		if i == 0 {
			if !isLetter(ch) && ch != '_' {
				return false
			}
		} else {
			if !isLetter(ch) && !isDigit(ch) && ch != '_' {
				return false
			}
		}
	}
	return true
}

func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }
func isDigit(ch rune) bool  { return ch >= '0' && ch <= '9' }

func isNaN(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}
