package parse

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/admpub/log"
	"github.com/admpub/log-analyzer/pkg/extraction"
	"github.com/admpub/log-analyzer/pkg/geoip"
	"github.com/admpub/log-analyzer/pkg/storage"
	"github.com/admpub/pp"
	"github.com/admpub/tail"
	"github.com/medama-io/go-useragent"
)

type Extraction = extraction.Extraction
type Param = extraction.Param

func dump(v ...interface{}) {
	pp.Println(v...)
}

// getParams extracts all possible group values contained within the regular
// expression from the text and stores the extracted values in a returned
// (groupName => value) map.
func getParams(text string, regEx string) map[string]string {
	compRegEx := regexp.MustCompile(regEx)
	match := compRegEx.FindStringSubmatch(text)

	paramsMap := make(map[string]string)
	for i, name := range compRegEx.SubexpNames() {
		if i > 0 && i <= len(match) {
			paramsMap[name] = strings.TrimSpace(match[i])
		}
	}
	return paramsMap
}

var escapeRegexCharactersReplacer = strings.NewReplacer(
	"(", "\\(",
	")", "\\)",
	"]", "\\]",
	"[", "\\[",
	"“", "\"",
)

// Replace all characters that have a special meaning within a regular
// expression with an escaped version of the character.
func escapeRegexCharacters(regEx string) string {
	regEx = escapeRegexCharactersReplacer.Replace(regEx)
	return regEx
}

// tryPattern attempts to extract the corresponding token values described by
// the given pattern from the log text line. Any extracted values have their
// data types inferred and then converted.
func tryPattern(line string, pattern string, tokens []string) map[string]Param {
	var regEx string = pattern
	regEx = escapeRegexCharacters(regEx)
	// Convert wildcard asterisk into underscore _ so we only have to deal
	// with one wildcard char
	regEx = strings.ReplaceAll(regEx, "*", "_")
	tokens = append(tokens, "_")
	// Sort tokens to try largest first and avoid matching substrings
	sort.Slice(tokens, func(i, j int) bool {
		return len(tokens[i]) > len(tokens[j])
	})
	for _, token := range tokens {
		// Encode token value to create temporary token ID as hex as any
		// brackets in token may break regex
		t := escapeRegexCharacters(token)
		if !strings.Contains(regEx, t) {
			continue
		}

		tokenID := hex.EncodeToString([]byte(t))
		regEx = strings.ReplaceAll(regEx, t, fmt.Sprintf("(?P<%s>.*)", tokenID))
	}
	encodedParams := getParams(line, regEx)

	// Decode back to raw token value
	params := make(map[string]string)
	for tokenID, match := range encodedParams {
		token, err := hex.DecodeString(tokenID)
		if err != nil {
			continue
		}
		stoken := string(token)
		// Avoid adding to final parameters if was a wildcard token
		if stoken == "" || stoken[0] == '_' {
			continue
		}
		if _, ok := params[stoken]; !ok {
			params[stoken] = match
		}
	}

	// Attempt to infer data types
	typedParams := inferDataTypes(params)

	return typedParams
}

var ua = useragent.NewParser()
var geoipdb *geoip.DB
var GeoIPDBPath string
var geoipOnce sync.Once

func initGeoIP() {
	if len(GeoIPDBPath) > 0 {
		var err error
		geoipdb, err = geoip.New(GeoIPDBPath)
		if err != nil {
			log.Fatalf("unable to load GeoIP database: %v", err)
		}
	}
}

func GeoIP() *geoip.DB {
	geoipOnce.Do(initGeoIP)
	return geoipdb
}

// inferDataTypes infers the intended the data type of each extracted parameter
// value from a log text line and performs a data type conversion.
func inferDataTypes(params map[string]string) map[string]Param {
	typedParams := make(map[string]Param)
	for token, match := range params {
		typedParams[token] = extraction.MakeParam(token, match)
	}
	for token, param := range typedParams {
		if param.Type == `time` {
			typedParams[`unix`+token] = Param{
				Value: param.Value.(time.Time).Unix(),
				Type:  "int",
			}
		}
	}
	if userAgent, ok := typedParams["user_agent"]; ok {
		agent := ua.Parse(userAgent.Value.(string))
		typedParams[`browser`] = Param{
			Value: agent.Browser(),
			Type:  "str",
		}
		typedParams[`os`] = Param{
			Value: agent.OS(),
			Type:  "str",
		}
		typedParams[`device`] = Param{
			Value: agent.Device(),
			Type:  "str",
		}
	}
	clientIP, ok := typedParams["ip_address"]
	if !ok {
		clientIP, ok = typedParams["ip"]
	}
	if ok {
		var cc string
		var lo string
		var longtitude, latitude float64
		geoipdb := GeoIP()
		if geoipdb != nil {
			if ip, ok := clientIP.Value.(net.IP); ok && ip != nil {
				record, err := geoipdb.LookupCountry(ip)
				if err != nil {
					log.Warnf("unable to retrieve IP country code: %v", err)
				} else {
					cc = record.Country.ISOCode
					longtitude = record.Location.Longitude
					latitude = record.Location.Latitude
					lo = record.LocationString()
				}
			}
		}
		typedParams[`country_code`] = Param{
			Value: cc,
			Type:  "str",
		}
		typedParams[`location`] = Param{
			Value: lo,
			Type:  "str",
		}
		typedParams[`longtitude`] = Param{
			Value: longtitude,
			Type:  "float",
		}
		typedParams[`latitude`] = Param{
			Value: latitude,
			Type:  "float",
		}
	}

	return typedParams
}

func tokenCounts(pattern string, tokens []string) int {
	var count int
	for _, token := range tokens {
		if strings.Contains(pattern, token) {
			count++
		}
	}
	return count
}

func calcExtractionRank(params map[string]Param, patternTokenCounts int) float64 {
	if patternTokenCounts == 0 {
		return 0
	}
	paramCount := float64(len(params))
	// Penalise parameter count in favour of proportion of tokens in pattern used
	rank := (paramCount * -0.05) + ((paramCount / float64(patternTokenCounts)) * 1)
	return rank
}

type PatternRank struct {
	rank   float64
	params map[string]Param
}

var multiSpaceRegEx = regexp.MustCompile(`[ ]{2,}`)

// parseLine extracts token parameters from each line using the most appropriate
// pattern in the given config.
func parseLineSingle(line string, config *Config) (PatternRank, string) {
	// Attempt to parse the line against each pattern in config, only taking the best
	var patternUsed string
	best := PatternRank{
		rank:   0.0,
		params: make(map[string]Param),
	}
	for pindex, pattern := range config.Patterns {
		if _, ok := config.mutilinePatterns[pindex]; ok {
			continue
		}
		// If pattern containing no tokens is a plain text match for line
		// Ensure usage of this pattern is recorded even if rank may not be best
		patternTokenCounts := config.patternCount[pindex].TokenCount
		if line == pattern && patternTokenCounts == 0 {
			patternUsed = pattern
			break
		}
		params := tryPattern(line, pattern, config.Tokens)
		rank := calcExtractionRank(params, patternTokenCounts)
		if rank > best.rank {
			best.rank = rank
			best.params = params
			patternUsed = pattern
		}

		// Try pattern again after eliminating multi-spaces and tab characters
		if multiSpaceRegEx.MatchString(line) {
			singleSpaceLine := multiSpaceRegEx.ReplaceAllString(strings.ReplaceAll(line, "\t", " "), " ")
			singleSpacePattern := multiSpaceRegEx.ReplaceAllString(strings.ReplaceAll(pattern, "\t", " "), " ")
			params = tryPattern(singleSpaceLine, singleSpacePattern, config.Tokens)
			rank := calcExtractionRank(params, patternTokenCounts)
			if rank > best.rank {
				best.rank = rank
				best.params = params
				patternUsed = pattern
			}
		}
	}

	return best, patternUsed
}

func parseSingleLine(line string, pattern string, patternTokenCounts int, config *Config) PatternRank {
	lineRank := PatternRank{
		rank:   0.0,
		params: make(map[string]Param),
	}

	// If pattern containing no tokens is a plain text match for line
	// Ensure usage of this pattern is recorded
	params := tryPattern(line, pattern, config.Tokens)
	rank := calcExtractionRank(params, patternTokenCounts)
	if rank > lineRank.rank {
		lineRank.rank = rank
		lineRank.params = params
	}

	// Try pattern again after eliminating multi-spaces and tab characters
	if multiSpaceRegEx.MatchString(line) {
		singleSpaceLine := multiSpaceRegEx.ReplaceAllString(strings.ReplaceAll(line, "\t", " "), " ")
		singleSpacePattern := multiSpaceRegEx.ReplaceAllString(strings.ReplaceAll(pattern, "\t", " "), " ")
		params = tryPattern(singleSpaceLine, singleSpacePattern, config.Tokens)
		rank := calcExtractionRank(params, patternTokenCounts)
		if rank > lineRank.rank {
			lineRank.rank = rank
			lineRank.params = params
		}
	}

	return lineRank
}

func parseMultiLine(lines []string, index int, partial PartialPattern, config *Config) PatternRank {
	lineRanks := make([]PatternRank, len(partial.Patterns))
	for i, patternLine := range partial.Patterns {
		line := lines[index+i]
		lineBest := parseSingleLine(line, patternLine, partial.TokenCount[i], config)
		lineRanks[i] = lineBest
	}

	return avgRank(lineRanks)
}

func avgRank(ranks []PatternRank) PatternRank {
	avg := PatternRank{
		rank:   0.0,
		params: make(map[string]Param),
	}
	for _, rank := range ranks {
		avg.rank += rank.rank
		for k, v := range rank.params {
			avg.params[k] = v
		}
	}
	avg.rank = avg.rank / float64(len(ranks))
	return avg
}

func patternLineCount(pattern string) int {
	return strings.Count(pattern, "\n") + 1
}

func splitLines(text string) []string {
	return strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
}

// Parse separates the log text into lines and attempts to extract tokens
// parameters from each line using the most appropriate pattern in the given config.
func ParseText(logtext string, config *Config, storager ...storage.Storager) ([]Extraction, error) {
	lines := splitLines(logtext)
	var em storage.Storager
	if len(storager) > 0 && storager[0] != nil {
		em = storager[0]
	} else {
		var err error
		em, err = storage.New(config.StorageEngine)
		if err != nil {
			return nil, err
		}
		defer em.Close()
	}
	var i int
	var unusedLines []string
	parse := MakeParser(em, &unusedLines, config)
	for _, line := range lines {
		parse(i, line)
		i++
	}
	if len(unusedLines) > 0 {
		for _index, _line := range unusedLines {
			log.Warnf("no pattern matched line %d: %q", i-len(unusedLines)+_index, _line)
		}
	}
	return em.List(config.LastLines)
}

// ParseFile reads the log text from the given file path, separates the text
// into lines and attempts to extract tokens parameters from each line using the
// most appropriate pattern in the given config.
func ParseFile(path string, config *Config, storager ...storage.Storager) ([]Extraction, error) {
	tcfg := tail.Config{
		LastLines: config.LastLines,
	}
	ti, err := tail.TailFile(path, tcfg)
	if err != nil {
		return nil, err
	}
	var em storage.Storager
	if len(storager) > 0 && storager[0] != nil {
		em = storager[0]
	} else {
		em, err = storage.New(config.StorageEngine)
		if err != nil {
			return nil, err
		}
		defer em.Close()
	}
	var i int
	var unusedLines []string
	parse := MakeParser(em, &unusedLines, config)
	for line := range ti.Lines {
		parse(i, line.Text)
		i++
	}
	if len(unusedLines) > 0 {
		for _index, _line := range unusedLines {
			log.Warnf("no pattern matched line %d: %q", i-len(unusedLines)+_index, _line)
		}
	}
	return em.List(config.LastLines)
}

func MakeParser(em storage.Storager, unusedLines *[]string, config *Config) func(index int, line string) {
	var lastRank PatternRank
	var recordedRank *PatternRank
	useLast := config.useLastLine
	var getUnunsed = func(n int) []string {
		var _unuseds []string
		if useLast {
			_unuseds = append(_unuseds, em.GetLastLines(n)...)
			_unuseds = append(_unuseds, *unusedLines...)
		} else {
			_unuseds = *unusedLines
		}
		return _unuseds
	}
	return func(index int, line string) {
		patternRank, patternUsed := parseLineSingle(line, config)
		if patternUsed == "" {
			if config.hasMultiple {
				*unusedLines = append(*unusedLines, line)
				if recordedRank == nil {
					if useLast {
						recordedRank = &lastRank
					} else {
						recordedRank = &PatternRank{
							params: map[string]Param{},
						}
					}
				}
				_unuseds := getUnunsed(0)
				unusedNum := len(_unuseds)
				for pindex, partial := range config.mutilinePatterns {
					plen := len(partial.Patterns)
					if plen < unusedNum {
						continue
					}
					fixedUnused := _unuseds
					if plen > unusedNum {
						fixedUnused = getUnunsed(plen - unusedNum)
					}
					lineRank := parseMultiLine(fixedUnused, 0, partial, config)
					// Record if this pattern is better than others seen so far
					if lineRank.rank > recordedRank.rank {
						recordedRank.rank = lineRank.rank
						recordedRank.params = lineRank.params
						patternUsed = config.Patterns[pindex]
					}
				}
				if len(patternUsed) > 0 {
					extra := Extraction{
						Params:     recordedRank.params,
						Pattern:    patternUsed,
						LineNumber: index - unusedNum,
						Line:       strings.Join(getUnunsed(1), "\n"),
					}
					if !config.useLastLine {
						extra.LineNumber += 1
					}
					if useLast {
						em.Update(extra)
					} else {
						em.Append(extra)
					}
					//dump(extra)
					*unusedLines = (*unusedLines)[0:0]
					recordedRank = nil
				}
			} else {
				log.Warnf("no pattern matched line %d: %q", index+1, line)
			}
		} else {
			lastRank = patternRank
			extra := Extraction{
				Params:     patternRank.params,
				Pattern:    patternUsed,
				LineNumber: index + 1,
				Line:       line,
			}
			em.Append(extra)
			if config.hasMultiple && len(*unusedLines) > 0 {
				for _index, _line := range *unusedLines {
					log.Warnf("no pattern matched line %d: %q", index+1-len(*unusedLines)+_index, _line)
				}
				*unusedLines = (*unusedLines)[0:0]
				recordedRank = nil
			}
		}
	}
}

// ParseFile reads the log text from each of the given file paths, separates the
// text into lines and attempts to extract tokens parameters from each line
// using the most appropriate pattern in the given config.
func ParseFiles(paths []string, config *Config) ([]Extraction, error) {
	extraction := make([]Extraction, 0)
	em, err := storage.New(config.StorageEngine)
	if err != nil {
		return nil, err
	}
	defer em.Close()
	for _, path := range paths {
		ex, err := ParseFile(path, config, em)
		if err != nil {
			return nil, fmt.Errorf("unable to parse file at path %s: %w", path, err)
		}
		extraction = append(extraction, ex...)
	}
	return extraction, nil
}

func writeConfigTest(extractions []Extraction) (int, error) {
	//write data as buffer to json encoder
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent("", "\t")

	err := encoder.Encode(extractions)
	if err != nil {
		return 0, err
	}
	fileDir := fmt.Sprintf("test-%d.json", time.Now().Unix())
	file, err := os.OpenFile(fileDir, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return 0, err
	}
	n, err := file.Write(buffer.Bytes())
	if err != nil {
		return 0, err
	}
	fmt.Println(fileDir) // Display output location
	return n, nil
}

type ExtractionDebug struct {
	Line       string         `json:"line"`
	LineNumber int            `json:"lineNumber"`
	Pattern    string         `json:"pattern"`
	Params     map[string]any `json:"params"`
}

// ParseTest runs Parse and displays a random sample of extracted parameters
// along with the origin lines from the log text.
func ParseTest(logtext string, config *Config, storager ...storage.Storager) []Extraction {
	extractions, err := ParseText(logtext, config, storager...)
	if err != nil {
		panic(err)
	}
	// Random sample...
	writeConfigTest(extractions)
	return extractions
}

// ParseTest runs ParseFile and displays a random sample of extracted parameters
// along with the origin lines from the log file.
func ParseFileTest(path string, config *Config, storager ...storage.Storager) ([]Extraction, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	extractions := ParseTest(string(body), config, storager...)
	return extractions, nil
}

// ParseTest runs ParseFiles and displays a random sample of extracted
// parameters along with the origin lines from the log files.
func ParseFilesTest(paths []string, config *Config) ([]Extraction, error) {
	extractions := make([]Extraction, 0)
	em, err := storage.New(config.StorageEngine)
	if err != nil {
		return nil, err
	}
	defer em.Close()
	var parsedAny bool
	for _, path := range paths {
		r, err := ParseFileTest(path, config, em)
		if err != nil {
			log.Warnf("unable to read file at path %s: %v", path, err)
			continue
		}
		parsedAny = true
		extractions = append(extractions, r...)
	}

	if !parsedAny {
		return nil, errors.New("unable to read log file path provided")
	}

	return extractions, nil
}
