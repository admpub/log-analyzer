# Nginx 日志分析系统 (Go版本)

基于 DuckDB + Parquet 的高性能 Nginx 访问日志分析系统。

## 特性

- 🚀 高性能：使用 DuckDB 内存列式数据库
- 📊 实时分析：分钟级延迟的实时统计
- 🔍 灵活查询：支持自定义 SQL 查询
- 📈 丰富图表：内置多种可视化图表
- 🐳 容器化：支持 Docker 一键部署
- 🔄 自动刷新：定时自动加载新数据
- 💾 缓存优化：智能缓存提升性能

## 快速开始

### 1. 环境要求
- Go 1.21+
- Nginx
- Vector (用于日志收集)

### 2. 安装部署

克隆项目
```
git clone <repository-url>

cd log-analyzer
```
安装依赖
```
go mod download
```
构建
```
go build -o log-analyzer ./cmd/server/
```
运行
```
./log-analyzer
```

### 3. 使用 Docker

使用 Docker Compose

```
docker-compose up -d
```

或直接使用 Docker

```
docker build -t log-analyzer .

docker run -p 8080:8080 -v /path/to/logs:/data/logs log-analyzer
```

### 4. 配置 Nginx
在 `nginx.conf` 中添加：


```
log_format json_log escape=json
'{'
'"remote_addr":"$remote_addr",'
'"time_local":"$time_local",'
'"request":"$request",'
'"status":$status,'
'"body_bytes_sent":$body_bytes_sent,'
'"http_user_agent":"$http_user_agent",'
'"http_referer":"$http_referer",'
'"request_time":$request_time'
'}';

access_log /var/log/nginx/access.log json_log;
```

## API 接口

### 实时统计

```
GET /api/stats/realtime?hours=1
```

### 热门路径

```
GET /api/stats/top-paths?limit=10&hours=24
```

### 小时统计

```
GET /api/stats/hourly?days=7
```

### 状态码分布

```
GET /api/stats/status-distribution
```

### 可疑IP检测

```
GET /api/stats/suspicious-ips?threshold=100
```

### 自定义查询

```
POST /api/query
{
"sql": "SELECT ip, COUNT(*) as cnt FROM nginx_logs GROUP BY ip ORDER BY cnt DESC LIMIT 10"
}
```

### 刷新数据

```
POST /api/refresh
```

## 性能指标

- 支持每日 1亿+ 日志记录
- 查询响应时间 < 100ms (95% 场景)
- 内存占用 < 500MB
- 支持 100+ 并发查询

## 系统特点

1. 高性能：Go 语言编写，原生并发支持，内存占用低

2. 实时性：基于 Vector 实时采集，DuckDB 内存计算

3. 可扩展：模块化设计，易于扩展新功能

4. 易部署：支持 Docker 一键部署

5. 完整生态：包含 Web 界面、API 接口、监控指标

6. 生产就绪：包含健康检查、优雅关闭、错误处理
