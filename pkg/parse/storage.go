package parse

type Storager interface {
	Append(Extraction) error
	Update(Extraction) error
	GetLastLines(n int) []string
}

type storageMemory struct {
	extraction []Extraction
}

func (e *storageMemory) Append(extra Extraction) error {
	e.extraction = append(e.extraction, extra)
	return nil
}

func (e *storageMemory) Update(extra Extraction) error {
	if len(e.extraction) > 0 {
		e.extraction[len(e.extraction)-1] = extra
	} else {
		e.extraction = append(e.extraction, extra)
	}
	return nil
}

func (e *storageMemory) GetLastLines(n int) (unuseds []string) {
	elen := len(e.extraction)
	if elen > 0 {
		if elen > n {
			unuseds = make([]string, 0, n)
			for _, extra := range e.extraction[elen-n:] {
				unuseds = append(unuseds, extra.Line)
			}
		} else {
			unuseds = make([]string, 0, elen)
			for _, extra := range e.extraction {
				unuseds = append(unuseds, extra.Line)
			}
		}
	}
	return
}
