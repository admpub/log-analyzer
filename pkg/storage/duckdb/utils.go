package storage

import (
	"sort"
	"strings"

	"github.com/admpub/log-analyzer/pkg/extraction"
	"github.com/webx-top/com"

	"github.com/marcboeker/go-duckdb/v2"
)

func FromDuckMap(params duckdb.Map) map[string]extraction.Param {
	result := map[string]extraction.Param{}
	for key, param := range params {
		token := key.(string)
		result[token] = extraction.MakeParam(token, param.(string))
	}
	return result
}

func AsDuckMap(params map[string]extraction.Param) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	q := strings.Builder{}
	q.WriteString(`{`)
	for idx, key := range keys {
		if idx > 0 {
			q.WriteString(`,`)
		}
		q.WriteString(`'`)
		q.WriteString(key)
		q.WriteString(`'`)
		q.WriteString(`:`)
		v := params[key]

		q.WriteString(`'`)
		q.WriteString(com.AddSlashes(com.String(v.Value)))
		q.WriteString(`'`)
		continue
		/*
			switch vv := v.Value.(type) {
			case string:
				q.WriteString(`union_value(str := `)
				q.WriteString(`'`)
				q.WriteString(com.AddSlashes(vv))
				q.WriteString(`'`)
				q.WriteString(`)`)
			case int, int32, int64:
				q.WriteString(`union_value(num := `)
				q.WriteString(com.String(vv))
				q.WriteString(`)`)
			case float32, float64:
				q.WriteString(`union_value(float := `)
				q.WriteString(com.String(vv))
				q.WriteString(`)`)
			case bool:
				q.WriteString(`union_value(bool := `)
				q.WriteString(strconv.FormatBool(vv))
				q.WriteString(`)`)
			case time.Time:
				q.WriteString(`union_value(str := `)
				q.WriteString(`'`)
				q.WriteString(vv.Local().Format(time.DateTime))
				q.WriteString(`'`)
				q.WriteString(`)`)
			case net.IP:
				q.WriteString(`union_value(str := `)
				q.WriteString(`'`)
				q.WriteString(vv.String())
				q.WriteString(`'`)
				q.WriteString(`)`)
			default:
				q.WriteString(`union_value(str := `)
				q.WriteString(`'`)
				q.WriteString(com.AddSlashes(com.String(vv)))
				q.WriteString(`'`)
				q.WriteString(`)`)
			}
		*/
	}
	q.WriteString(`}`)
	return q.String()
}
