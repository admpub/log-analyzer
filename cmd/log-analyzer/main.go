package main

import (
	"fmt"
	"os"
)

// ========== Main Entry ==========

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		runServer()
	case "convert":
		runConvert()
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `log-analyzer - Nginx/Apache 日志分析与转换工具

用法:
  log-analyzer <命令> [选项]

命令:
  server   启动 Web 分析服务端
  convert  将日志文件转换为 Parquet 格式

示例:
  log-analyzer server                          # 启动 Web 服务 (默认端口 8080)
  log-analyzer convert -i access.log -o out.parquet  # 转换日志为 Parquet

运行 "log-analyzer convert --help" 查看转换命令详细选项。

`)
}
