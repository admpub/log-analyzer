package parse

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/admpub/log"
	"github.com/admpub/log-analyzer/pkg/storage"
	"github.com/araddon/dateparse"
)

type Config struct {
	Tokens           []string              `json:"tokens"`
	Patterns         []string              `json:"patterns"`
	Dependencies     map[string][]string   `json:"dependencies,omitempty"`
	Conversions      map[string]Conversion `json:"conversions,omitempty"`
	LastLines        int                   `json:"lastLines,omitempty"`
	ShowProgress     bool                  `json:"showProgress,omitempty"`
	StorageEngine    string                `json:"storageEngine,omitempty"`
	TimeRange        *TimeStrRange         `json:"timeRange,omitempty"`
	timeRange        *TimeRange
	storager         storage.Storager
	patternCount     []PatternCount
	hasMultiple      bool
	mutilinePatterns map[int]PartialPattern //{Patterns.index:[]string}
	useLastLine      bool
}

type TimeStrRange struct {
	Token string `json:"token,omitempty"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

type TimeRange struct {
	Token string
	Start time.Time
	End   time.Time
}

type PartialPattern struct {
	Patterns   []string
	TokenCount []int
}

func (c *Config) Storager() (storage.Storager, error) {
	if c.storager == nil {
		var err error
		c.storager, err = storage.New(c.StorageEngine)
		if err != nil {
			return nil, err
		}
	}
	return c.storager, nil
}

func (c *Config) Close() {
	if c.storager != nil {
		c.storager.Close()
	}
}

func (c *Config) SetDefaults() error {
	if len(c.StorageEngine) == 0 {
		c.StorageEngine = `memory`
	}
	if c.StorageEngine == `memory` && c.LastLines <= 0 {
		c.LastLines = 10000
	}
	c.mutilinePatterns = map[int]PartialPattern{}
	c.patternCount = make([]PatternCount, len(c.Patterns))
	for i, v := range c.Patterns {
		md := PatternCount{
			LineCount:  patternLineCount(v),
			TokenCount: tokenCounts(v, c.Tokens),
		}
		if md.LineCount > 1 {
			if !c.hasMultiple {
				c.hasMultiple = true
			}
			partial := PartialPattern{
				Patterns: splitLines(v),
			}
			partial.TokenCount = make([]int, len(partial.Patterns))
			for pi, pv := range partial.Patterns {
				partial.TokenCount[pi] = tokenCounts(pv, c.Tokens)
			}
			c.mutilinePatterns[i] = partial
		}
		c.patternCount[i] = md
	}
	if c.hasMultiple {
		oldNew := make([]string, 0, len(c.Tokens)*2)
		for _, token := range c.Tokens {
			oldNew = append(oldNew, token, `{*}`)
		}
		repl := strings.NewReplacer(oldNew...)
		for ix, pl := range c.mutilinePatterns {
			fo := repl.Replace(pl.Patterns[0])
			for index, pattern := range c.Patterns {
				if ix == index {
					continue
				}
				if strings.HasPrefix(fo, repl.Replace(pattern)) {
					c.useLastLine = true
					break
				}
			}
			if c.useLastLine {
				break
			}
		}
	}
	var err error
	if c.TimeRange != nil && len(c.TimeRange.Token) > 0 && (len(c.TimeRange.Start) > 0 || len(c.TimeRange.End) > 0) {
		c.timeRange = &TimeRange{
			Token: c.TimeRange.Token,
		}
		if len(c.TimeRange.Start) > 0 {
			c.timeRange.Start, err = dateparse.ParseAny(c.TimeRange.Start)
			if err != nil {
				log.Error(err)
			}
		}
		if len(c.TimeRange.End) > 0 {
			c.timeRange.End, err = dateparse.ParseAny(c.TimeRange.End)
			if err != nil {
				log.Error(err)
			}
		}
	}
	return err
}

type PatternCount struct {
	LineCount  int
	TokenCount int
}

type Conversion struct {
	Token      string  `json:"token"`
	Multiplier float64 `json:"multiplier"`
}

func LoadConfig(path string) (Config, error) {
	jsonFile, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return Config{}, err
	}

	var config Config
	if err = json.Unmarshal([]byte(byteValue), &config); err != nil {
		fmt.Println(err)
		return Config{}, err
	}
	err = config.SetDefaults()
	return config, err
}
