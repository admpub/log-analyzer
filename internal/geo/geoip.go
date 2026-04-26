package geo

import (
	"net"
	"sync"

	"github.com/oschwald/geoip2-golang"
	"github.com/rs/zerolog"
)

// GeoIP GeoIP地理位置解析器
type GeoIP struct {
	db     *geoip2.Reader
	logger zerolog.Logger
	mu     sync.RWMutex
}

// Location 地理位置
type Location struct {
	Country string
	City    string
}

// NewGeoIP 创建GeoIP解析器
func NewGeoIP(dbPath string, logger zerolog.Logger) (*GeoIP, error) {
	db, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, err
	}
	return &GeoIP{
		db:     db,
		logger: logger,
	}, nil
}

// Lookup 查询IP地址的地理位置
func (g *GeoIP) Lookup(ipStr string) *Location {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.db == nil {
		return &Location{Country: "Unknown", City: ""}
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return &Location{Country: "Unknown", City: ""}
	}

	record, err := g.db.City(ip)
	if err != nil {
		g.logger.Debug().Err(err).Str("ip", ipStr).Msg("failed to lookup geoip")
		return &Location{Country: "Unknown", City: ""}
	}

	loc := &Location{}

	// 获取国家
	if record.Country.Names["en"] != "" {
		loc.Country = record.Country.Names["en"]
	} else if record.RegisteredCountry.Names["en"] != "" {
		loc.Country = record.RegisteredCountry.Names["en"]
	} else {
		loc.Country = "Unknown"
	}

	// 获取城市
	if record.City.Names["en"] != "" {
		loc.City = record.City.Names["en"]
	}

	return loc
}

// Close 关闭GeoIP数据库
func (g *GeoIP) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.db != nil {
		return g.db.Close()
	}
	return nil
}
