#!/bin/bash
# log-analyzer 启动脚本

CMD=${1:-"server"}
shift

case "$CMD" in
  "server")
    CGO_ENABLED=1 go run ./cmd/log-analyzer/ server "$@"
    ;;
  "convert"|"cli")
    CGO_ENABLED=1 go run ./cmd/log-analyzer/ convert "$@"
    ;;
  "build")
    CGO_ENABLED=1 go build -o log-analyzer ./cmd/log-analyzer/
    ;;
  *)
    echo "用法: run.sh [command] [options]"
    echo ""
    echo "Commands:"
    echo "  server    启动Web分析服务 (默认)"
    echo "  convert   转换日志为Parquet格式"
    echo ""
    echo "示例:"
    echo "  ./run.sh server                          # 启动Web服务"
    echo "  ./run.sh convert -input access.log -output out.parquet"
    echo "  ./run.sh convert -input access.log -output out/ --partition --geoip-db GeoLite2-City.mmdb"
    exit 1
    ;;
esac