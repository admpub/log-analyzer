package parse

import (
	"log"
	"strings"
	"testing"

	"github.com/admpub/log-analyzer/pkg/storage"
	"github.com/stretchr/testify/assert"
)

type Profile struct {
	config  Config
	logpath string
}

var profiles = []Profile{
	{
		config:  loadConfig("Apache.json"),
		logpath: logPath("Apache.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"starting...",
				"timestamp :: threadthread_number - starting",
				"timestamp :: threadthread_number - exiting",
				"timestamp :: complete",
			},
			Tokens: []string{"timestamp", "thread_number"},
		},
		logpath: logPath("test.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"starting...",
				"time_timestamp :: threadint_thread_number - starting",
				"time_timestamp :: threadint_thread_number - exiting",
				"time_timestamp :: complete",
			},
			Tokens: []string{"time_timestamp", "int_thread_number"},
		},
		logpath: logPath("test.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[timestamp] ip:dbname LOG: duration: elapsed ms statement: query",
				"[timestamp] ip:dbname LOG: statement: query",
			},
			Tokens: []string{"timestamp", "ip", "dbname", "elapsed", "query"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[time_timestamp] ip_address:str_dbname LOG: duration: float_elapsed ms statement: str_query",
				"[time_timestamp] ip_address:str_dbname LOG: statement: str_query",
			},
			Tokens: []string{"time_timestamp", "ip_address", "str_dbname", "float_elapsed", "str_query"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[INFO ] [taken_msms] result found: page=url status=status_code size=bytes",
				"[INFO ] [taken_ss] result found: page=url status=status_code size=bytes",
			},
			Tokens: []string{"taken_ms", "taken_s", "url", "status_code", "bytes"},
			Conversions: map[string]Conversion{
				"taken_ms": {
					Multiplier: 1000,
					Token:      "taken_s",
				},
			},
		},
		logpath: logPath("test3.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[INFO ] [float_taken_msms] result found: page=str_url status=int_status_code size=int_bytes",
				"[INFO ] [float_taken_ss] result found: page=str_url status=int_status_code size=int_bytes",
			},
			Tokens: []string{"float_taken_ms", "float_taken_s", "str_url", "str_status_code", "str_bytes"},
		},
		logpath: logPath("test3.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[timestamp] rest",
			},
			Tokens: []string{"timestamp", "rest"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[time_timestamp] str_rest",
			},
			Tokens: []string{"time_timestamp", "str_rest"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[timestamp] *",
			},
			Tokens: []string{"timestamp"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"*",
			},
			Tokens: []string{},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"all",
			},
			Tokens: []string{"all"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"str_all",
			},
			Tokens: []string{"str_all"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"rest query",
			},
			Tokens: []string{"query"},
		},
		logpath: logPath("test2.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[time] ip rest",
			},
			Tokens: []string{"time", "ip", "rest"},
		},
		logpath: logPath("demo.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"*",
			},
			Tokens: []string{},
		},
		logpath: logPath("demo.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"***",
			},
			Tokens: []string{},
		},
		logpath: logPath("demo.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[*] *",
			},
			Tokens: []string{},
		},
		logpath: logPath("demo.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[*] * *",
			},
			Tokens: []string{},
		},
		logpath: logPath("demo.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"timestamp x y z source: message",
			},
			Tokens: []string{"timestamp", "x", "y", "z", "source", "message"},
		},
		logpath: logPath("loghub/Android_v1.log"),
	},
	{
		config: Config{
			Patterns: []string{
				"[timestamp] [type] jk2_init() Found child child_number in scoreboard slot slot_number",
				"[timestamp] [type] [client ip] message",
				"[timestamp] [type] mod_jk child workerEnv in error state state_number",
				"[timestamp] [type] workerEnv.init() ok /etc/httpd/conf/workers2.properties",
				"[timestamp] [type] other",
			},
			Tokens: []string{"timestamp", "type", "child_number", "slot_number", "ip", "message", "state_number", "other"},
		},
		logpath: logPath("loghub/Apache.log"),
	},
	{
		config:  loadConfig("multiline.json"),
		logpath: logPath("multiline.log"),
	},
}

func loadConfig(file string) Config {
	config, err := LoadConfig(configPath(file))
	if err != nil {
		panic(err)
	}
	return config
}

func configPath(file string) string {
	return "../../tests/data/configs/" + file
}

func logPath(file string) string {
	return "../../tests/data/logs/" + file
}

func TestParseFile(t *testing.T) {
	for i, profile := range profiles {
		log.Printf("testing profile %d...", i)
		extraction, err := ParseFile(profile.logpath, &profile.config)
		if err != nil {
			t.Error(err)
		}
		if len(extraction) == 0 {
			t.Errorf("profile %d: no lines extracted", i)
		}

		for _, e := range extraction {
			if e.Pattern == "" {
				t.Errorf("profile %d: line %d failed: %s", i, e.LineNumber, e.Line)
			} else if len(e.Params) == 0 {
				t.Logf("profile %d: no params extracted from line %d: %s", i, e.LineNumber, e.Line)
			}
		}
	}
}

func TestParser(t *testing.T) {
	var i int
	var unusedLines []string
	config := Config{
		Tokens: []string{"ip_address", "timestamp", "method", "path", "status", "int_bytes", "url", "user_agent"},
		Patterns: []string{
			"ip_address - - [timestamp] \"method path *\" status int_bytes \"-\" \"user_agent\"",
			"ip_address - - [timestamp] \"method path *\" status int_bytes \"url\" \"user_agent\"",
		},
		Dependencies: map[string][]string{
			`path`: []string{"status", "int_bytes"},
		},
	}
	config.SetDefaults()
	em, err := storage.New(config.StorageEngine)
	assert.NoError(t, err)
	defer em.Close()
	parse := MakeParser(em, &unusedLines, &config)
	lines := []string{
		`- - - [17/May/2015:10:05:03 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1" 200 203023 "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"`,
		`- - - [17/May/2015:10:05:43 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png HTTP/1.1" 200 171717 "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"`,
		`other!!!!!!`,
	}
	for _, line := range lines {
		parse(i, line)
		i++
	}
	if len(unusedLines) > 0 {
		for _index, _line := range unusedLines {
			log.Printf("no pattern matched line %d: \"%s\"\n", i-len(unusedLines)+_index, _line)
		}
	}
	list, _ := em.List(100)
	assert.Equal(t, 2, len(list))
	dump(list)
}

func TestParser2(t *testing.T) {
	var i int
	var unusedLines []string
	config := Config{
		Tokens: []string{"timestamp", "process", "message", "error_msg", "action", "status"},
		Patterns: []string{
			"timestamp :: process - message",
			"timestamp :: process - status\n    -> error_msg",
			"timestamp :: process - status\n    -> error_msg\n    -> action",
		},
	}
	config.SetDefaults()
	em, err := storage.New(config.StorageEngine)
	assert.NoError(t, err)
	defer em.Close()
	parse := MakeParser(em, &unusedLines, &config)
	lines := strings.Split(`2015-07-12 14:59:23 :: process1 - starting process 1
2015-07-12 14:59:23 :: process2 - starting process 2
2015-07-12 14:59:23 :: process3 - starting process 3
2015-07-12 14:59:23 :: process4 - starting process 4
2015-07-12 14:59:24 :: process1 - WARNING
    -> warning from process 1
2015-07-12 14:59:25 :: process2 - WARNING
    -> warning from process 2
2015-07-12 14:59:27 :: process4 - CRITICAL ERROR
    -> error in process 4
    -> shutting down
2015-07-12 14:59:27 :: process4 - stopping process 4
2015-07-12 14:59:30 :: process3 - complete
2015-07-12 14:59:30 :: process3 - stopping process 3
2015-07-12 14:59:31 :: process1 - complete
2015-07-12 14:59:31 :: process1 - stopping process 1
2015-07-12 14:59:33 :: process2 - complete
2015-07-12 14:59:33 :: process2 - stopping process 2`, "\n")
	for _, line := range lines {
		parse(i, line)
		i++
	}
	if len(unusedLines) > 0 {
		for _index, _line := range unusedLines {
			log.Printf("no pattern matched line %d: \"%s\"\n", i-len(unusedLines)+_index, _line)
		}
	}
	list, _ := em.List(100)
	assert.Equal(t, 14, len(list))
	//dump(list)
}
