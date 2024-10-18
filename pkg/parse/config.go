package parse

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type Config struct {
	Tokens           []string              `json:"tokens"`
	Patterns         []string              `json:"patterns"`
	Dependencies     map[string][]string   `json:"dependencies,omitempty"`
	Conversions      map[string]Conversion `json:"conversions,omitempty"`
	LastLines        int                   `json:"lastlines,omitempty"`
	patternCount     []PatternCount
	hasMultiple      bool
	mutilinePatterns map[int][]string //{Patterns.index:[]string}
	useLastLine      bool
}

func (c *Config) SetDefaults() {
	c.mutilinePatterns = map[int][]string{}
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
			c.mutilinePatterns[i] = splitLines(v)
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
			fo := repl.Replace(pl[0])
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
	if err := json.Unmarshal([]byte(byteValue), &config); err != nil {
		fmt.Println(err)
		return Config{}, err
	}
	config.SetDefaults()
	return config, nil
}
