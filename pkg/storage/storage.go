package storage

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/admpub/log-analyzer/pkg/extraction"
)

type Storager interface {
	Append(extraction.Extraction) error
	Update(extraction.Extraction) error
	List(limit int) ([]extraction.Extraction, error)
	GetLastLines(n int) []string
	Close()
}

type ListBy interface {
	ListBy(args map[string]interface{}, limit int) ([]extraction.Extraction, error)
}

type Constructor func(*url.URL) (Storager, error)

var storagers = map[string]Constructor{}

func Register(name string, function Constructor) {
	storagers[name] = function
}

var ErrUnsupported = errors.New(`unsuppored storage`)

func New(name string) (Storager, error) {
	parts := strings.SplitN(name, `:`, 2)
	var dsnURL *url.URL
	if len(parts) == 2 {
		dsnURL, _ = url.Parse(name)
		name = parts[0]
	}
	fn, ok := storagers[name]
	if !ok {
		return nil, fmt.Errorf(`%w: %s`, ErrUnsupported, name)
	}
	return fn(dsnURL)
}
