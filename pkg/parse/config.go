package parse

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Config struct {
	Tokens       []string              `json:"tokens"`
	Patterns     []string              `json:"patterns"`
	Dependencies map[string][]string   `json:"dependencies,omitempty"`
	Conversions  map[string]Conversion `json:"conversions,omitempty"`
	LastLines    int                   `json:"lastlines,omitempty"`
	patternModes []PatternMode
	hasMultiple  bool
}

type PatternMode struct {
	LineCount int
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

	config.patternModes = make([]PatternMode, len(config.Patterns))
	for i, v := range config.Patterns {
		md := PatternMode{
			LineCount: patternLineCount(v),
		}
		if !config.hasMultiple && md.LineCount > 1 {
			config.hasMultiple = true
		}
		config.patternModes[i] = md
	}

	return config, nil
}
