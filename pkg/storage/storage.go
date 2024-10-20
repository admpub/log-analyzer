package storage

import (
	"errors"
	"fmt"

	"github.com/admpub/log-analyzer/pkg/extraction"
)

type Storager interface {
	Append(extraction.Extraction) error
	Update(extraction.Extraction) error
	List() ([]extraction.Extraction, error)
	GetLastLines(n int) []string
	Close()
}

type Constructor func() (Storager, error)

var storagers = map[string]Constructor{}

func Register(name string, function Constructor) {
	storagers[name] = function
}

var ErrUnsupported = errors.New(`unsuppored storage`)

func New(name string) (Storager, error) {
	fn, ok := storagers[name]
	if !ok {
		return nil, fmt.Errorf(`%w: %s`, ErrUnsupported, name)
	}
	return fn()
}
