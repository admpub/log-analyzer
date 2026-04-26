package output

import (
	"fmt"
	"os"

	"github.com/admpub/go-pretty/v6/table"
	"github.com/admpub/go-pretty/v6/text"
)

func Table(title interface{}, data []map[string]any, width ...int) {
	if len(data) == 0 {
		return
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	if title != nil {
		t.AppendHeader(table.Row{title})
	}
	if len(width) > 0 {
		t.SetAllowedRowLength(width[0])
	}
	names := make([]string, 0, len(data[0]))
	header := make([]interface{}, 0, len(data[0]))
	for name := range data[0] {
		names = append(names, name)
		header = append(header, name)
	}
	t.AppendRow(header)
	for _, row := range data {
		vals := make([]interface{}, len(names))
		for index, name := range names {
			if iptr, ok := row[name].(*interface{}); ok {
				row[name] = *iptr
			}
			vals[index] = row[name]
		}
		t.AppendRow(vals)
	}
	t.SetStyle(table.StyleColoredRedWhiteOnBlack)
	headerColor := text.Colors{text.BgBlue, text.FgHiWhite, text.Bold}
	t.Style().Color.Header = headerColor
	t.Style().Color.Footer = text.Colors{text.BgWhite, text.FgBlack, text.Italic}
	t.Style().Color.Row = text.Colors{text.BgWhite, text.FgBlack}
	t.Style().Color.RowAlternate = text.Colors{text.BgWhite, text.FgBlack}
	fmt.Println()
	t.Render()
	fmt.Println()
}
