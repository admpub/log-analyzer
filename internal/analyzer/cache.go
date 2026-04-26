package analyzer

import (
	"fmt"
	"sync"
	"time"

	"log-analyzer/internal/model"
)

type StatsCache struct {
	cache             map[string]cacheEntry
	mu                sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	stopCleanup       chan bool
}

type cacheEntry struct {
	value      interface{}
	expiration time.Time
}

func NewStatsCache(defaultExpiration time.Duration) *StatsCache {
	cache := &StatsCache{
		cache:             make(map[string]cacheEntry),
		defaultExpiration: defaultExpiration,
		cleanupInterval:   defaultExpiration / 2,
		stopCleanup:       make(chan bool),
	}

	go cache.startCleanup()
	return cache
}

func (c *StatsCache) startCleanup() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *StatsCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.cache {
		if now.After(entry.expiration) {
			delete(c.cache, key)
		}
	}
}

func (c *StatsCache) GetRealTimeStats(hours int) (*model.RealTimeStats, bool) {
	key := fmt.Sprintf("realtime_stats_%d", hours)
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if stats, ok := entry.value.(*model.RealTimeStats); ok {
			return stats, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetRealTimeStats(hours int, stats *model.RealTimeStats) {
	key := fmt.Sprintf("realtime_stats_%d", hours)
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      stats,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetTopPaths(key string) ([]model.TopPath, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if paths, ok := entry.value.([]model.TopPath); ok {
			return paths, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetTopPaths(key string, paths []model.TopPath) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      paths,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetSlowPaths(key string) ([]model.SlowPathAnalysis, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if paths, ok := entry.value.([]model.SlowPathAnalysis); ok {
			return paths, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetSlowPaths(key string, paths []model.SlowPathAnalysis) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      paths,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetTopIPs(key string) ([]model.TopIP, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if paths, ok := entry.value.([]model.TopIP); ok {
			return paths, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetTopIPs(key string, paths []model.TopIP) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      paths,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetHourlyStats(key string) ([]model.HourlyStats, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if stats, ok := entry.value.([]model.HourlyStats); ok {
			return stats, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetHourlyStats(key string, stats []model.HourlyStats) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      stats,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetStatusDistribution() ([]model.StatusDistribution, bool) {
	key := "status_distribution"
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if dist, ok := entry.value.([]model.StatusDistribution); ok {
			return dist, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetStatusDistribution(dist []model.StatusDistribution) {
	key := "status_distribution"
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      dist,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetTopCountries(key string) ([]model.CountryStats, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if countries, ok := entry.value.([]model.CountryStats); ok {
			return countries, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetTopCountries(key string, countries []model.CountryStats) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      countries,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetUVTrend(key string) ([]model.UVTrend, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if trend, ok := entry.value.([]model.UVTrend); ok {
			return trend, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetUVTrend(key string, trend []model.UVTrend) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      trend,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

func (c *StatsCache) GetUVDistribution(key string) (*model.UVDistribution, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if dist, ok := entry.value.(*model.UVDistribution); ok {
			return dist, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetUVDistribution(key string, dist *model.UVDistribution) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      dist,
		expiration: time.Now().Add(c.defaultExpiration),
	}
	c.mu.Unlock()
}

// 添加路径分析缓存方法

func (c *StatsCache) GetPathAnalysis(key string) (*model.PathAnalysisDetail, bool) {
	c.mu.RLock()
	entry, found := c.cache[key]
	c.mu.RUnlock()

	if found && time.Now().Before(entry.expiration) {
		if analysis, ok := entry.value.(*model.PathAnalysisDetail); ok {
			return analysis, true
		}
	}
	return nil, false
}

func (c *StatsCache) SetPathAnalysis(key string, analysis *model.PathAnalysisDetail) {
	c.mu.Lock()
	c.cache[key] = cacheEntry{
		value:      analysis,
		expiration: time.Now().Add(5 * time.Minute), // 路径分析缓存5分钟
	}
	c.mu.Unlock()
}

func (c *StatsCache) Clear() {
	c.mu.Lock()
	c.cache = make(map[string]cacheEntry)
	c.mu.Unlock()
}

func (c *StatsCache) Stop() {
	close(c.stopCleanup)
}
