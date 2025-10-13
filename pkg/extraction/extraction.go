package extraction

import (
	"net"
	"strconv"
	"strings"

	"github.com/araddon/dateparse"
)

type Extraction struct {
	Params     map[string]Param `json:"params"` // keys: int_bytes ip_address method path status timestamp url user_agent
	Pattern    string           `json:"pattern"`
	LineNumber int              `json:"lineNumber"`
	Line       string           `json:"line"`
}

type Param struct {
	Value any    `json:"value"`
	Type  string `json:"type"`
}

func MakeParam(token, match string) Param {
	switch strings.SplitN(token, `_`, 2)[0] {
	case `int`:
		if value, err := strconv.Atoi(match); err == nil {
			return Param{Value: value, Type: "int"}
		}
	case `float`:
		if value, err := strconv.ParseFloat(match, 64); strings.Contains(match, ".") && err == nil {
			return Param{Value: value, Type: "float"}
		}
	case `time`:
		if value, err := dateparse.ParseAny(match); err == nil {
			return Param{Value: value, Type: "time"}
		}
	case `unixtimestamp`:
		if value, err := strconv.ParseInt(match, 10, 64); err == nil {
			return Param{Value: value, Type: "int"}
		}
	case `bool`:
		if value, err := strconv.ParseBool(match); err == nil {
			return Param{Value: value, Type: "bool"}
		}
	case `ip`:
		if value := net.ParseIP(match); value != nil {
			return Param{Value: value, Type: "ip"}
		}
	case `str`:
		return Param{Value: match, Type: "str"}
	}
	// Attempt to parse as datetime
	if value, err := dateparse.ParseAny(match); err == nil {
		return Param{Value: value, Type: "time"}
	} else if value := net.ParseIP(match); value != nil {
		return Param{Value: value, Type: "ip"}
	} else if value, err := strconv.ParseFloat(match, 64); strings.Contains(match, ".") && err == nil {
		return Param{Value: value, Type: "float"}
	} else if value, err := strconv.Atoi(match); err == nil {
		return Param{Value: value, Type: "int"}
	} else if value, err := strconv.ParseBool(match); err == nil {
		return Param{Value: value, Type: "bool"}
	} else {
		return Param{Value: match, Type: "str"}
	}
}
