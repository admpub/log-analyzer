# Nginx 日志分析系统

基于 **DuckDB + Parquet** 的高性能 Nginx/Apache 访问日志分析与转换工具。

## 特性

| 特性 | 说明 |
|------|------|
| 🚀 **高性能** | DuckDB 列式内存数据库，亿级日志秒级响应 |
| 🔄 **日志转换** | 支持 Nginx/Apache 日志一键转 Parquet（含分区） |
| 🌍 **GeoIP 解析** | 集成 MaxMind GeoIP，自动填充国家/城市 |
| 📊 **Web 仪表盘** | 内置多维度可视化分析界面 |
| 📈 **丰富接口** | 实时统计、热门路径、慢请求、UV 趋势等 15+ API |
| 💾 **Parquet 分区** | 按年/月/日/时自动分区，海量数据高效存储 |
| 🐳 **容器化** | Docker / Docker Compose 一键部署 |
| ⚙️ **子命令架构** | `server` 服务端 / `convert` 转换工具，灵活组合 |

## 项目结构

```
log-analyzer/
├── cmd/log-analyzer/        # 主入口 (server / convert 子命令)
├── config/                  # 配置文件 (config.yaml)
├── internal/analyzer/       # 核心分析引擎 (DuckDB + Parquet)
├── internal/api/            # HTTP API handlers
├── web/static/              # 前端静态页面 (仪表盘)
├── vector/                  # Vector 日志采集配置
├── scripts/deploy.sh        # 一键部署脚本
├── docker-complose.yaml     # Docker Compose 编排
├── run.sh                   # 开发启动脚本
└── Dockerfile               # 构建镜像
```

## 环境要求

- Go 1.25+
- CGO 环境（DuckDB 需要）
- Nginx 或 Apache（可选，用于日志收集）
- Vector（可选，用于实时日志采集）

## 安装与构建

```bash
git clone <repository-url>
cd log-analyzer
go mod download
go build -o log-analyzer ./cmd/log-analyzer/
```

## 使用方式

### 命令总览

```bash
log-analyzer <命令> [选项]
```

| 命令 | 功能 |
|------|------|
| `server` | 启动 Web 分析服务端（含仪表盘和 API） |
| `convert` | 将原始日志转换为 Parquet 格式 |

---

### 1. server — 启动 Web 服务

```bash
# 使用默认配置启动
./log-analyzer server

# 指定配置文件路径 (默认查找 ./config/config.yaml)
# 支持搜索路径: . → ./config → /etc/log-analyzer
./log-analyzer server
```

启动后访问：
- 仪表盘: http://localhost:8080
- API 文档: http://localhost:8080/api/metadata
- 健康检查: http://localhost:8080/api/health

#### 配置文件说明 (`config/config.yaml`)

```yaml
server:
  host: "0.0.0.0"    # 监听地址
  port: 8080          # 监听端口

analyzer:
  log_table: "nginx_logs"
  log_directory: "./data/logs"   # Parquet 数据目录
  geoip_db_path: ""              # GeoIP 数据库路径（留空则不解析）
  refresh_interval: "5m"         # 数据刷新间隔
  cache_ttl: "1m"                # 缓存 TTL
  max_connections: 20             # DuckDB 最大连接数
  query_timeout: "30s"           # 查询超时

log:
  level: "info"                  # debug / info / warn / error
  file_path: "./mylog/app.log"   # 日志文件路径（留空则仅输出到 stderr）
  max_size: 100                  # 单个日志文件大小 (MB)
  max_age: 7                     # 保留天数
```

---

### 2. convert — 日志转 Parquet

**基本用法**

```bash
# 默认转换（combined 格式，输出单文件）
./log-analyzer convert -i access.log -o output.parquet

# 指定开始时间过滤
./log-analyzer convert -i access.log -o output.parquet --start-time "2006-01-02T15:04:05"

# 指定日志格式 (combined / common / combinedCHD / combinedD)
./log-analyzer convert -i access.log -o output.parquet -format combinedCHD
```

**分区输出**（按年/月/日/时分目录存储）

```bash
./log-analyzer convert -i access.log -o output_dir/ --partition
```

**带 GeoIP 地理位置**

```bash
./log-analyzer convert -i access.log -o output.parquet --geoip-db GeoLite2-City.mmdb
```

**完整选项列表**

```bash
./log-analyzer convert --help
```

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-input` | string | *(必填)* | 输入日志文件路径 |
| `-output` | string | `output.parquet` | 输出 Parquet 文件或目录 |
| `-format` | string | `combined` | 日志格式: combined, common, combinedCHD, combinedD |
| `-geoip-db` | string | *空* | GeoIP 数据库路径 |
| `-partition` | bool | false | 是否按年月日时分区输出 |
| `-overwrite` | bool | false | 是否覆盖已有文件 |
| `-timeout` | duration | `10m` | 查询超时时间 |
| `-start-time` | string | *空* | 开始时间 (RFC3339 格式) |

> **提示**: 未指定 `-start-time` 时，会自动读取日志最新时间戳作为起点；若无历史数据则默认近 30 天。

---

### 3. 开发模式

```bash
# 使用 run.sh 快速启动（无需手动编译）
./run.sh server                    # 启动 Web 服务
./run.sh convert -i a.log -o out   # 转换日志
```

## Docker 部署

### Docker Compose（推荐）

```bash
# 启动全部服务（log-analyzer + vector 日志采集）
docker-compose up -d

# 仅启动 log-analyzer
docker-compose up -d nginx-log-analyzer
```

### 单独 Docker 运行

```bash
docker build -t log-analyzer .
docker run -d \
  --name log-analyzer \
  -p 8080:8080 \
  -v /path/to/logs:/data/logs \
  -v ./config:/app/config \
  log-analyzer server
```

### 一键部署脚本

```bash
sudo bash scripts/deploy.sh
```

该脚本会自动完成：创建目录结构 → 构建 → 复制可执行文件 → 创建 systemd 服务 → 启动服务。

## 配置 Nginx 日志格式

在 `nginx.conf` 中添加 JSON 格式日志定义：

```nginx
http {
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
}
```

## API 接口文档

基础路径: `/api`

### 统计分析接口

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/stats/realtime` | `?hours=1` | 实时概览统计 |
| GET | `/stats/top-paths` | `?limit=10&hours=24` | 热门访问路径 TOP N |
| GET | `/stats/slow-paths` | — | 慢请求路径排行 |
| GET | `/stats/analyze-path` | `?path=/api/test` | 单路径深度分析 |
| GET | `/stats/path-detail` | `?path=/api/test` | 路径详情明细 |
| GET | `/stats/slow-requests` | — | 慢请求列表 |
| GET | `/stats/top-ips` | `?limit=10` | 活跃 IP 排行 |
| GET | `/stats/hourly` | `?days=7` | 按小时流量趋势 |
| GET | `/stats/status-distribution` | — | HTTP 状态码分布 |
| GET | `/stats/countries` | — | 访问国家/地区分布（需 GeoIP） |
| GET | `/stats/uv-trend` | — | UV 访问趋势 |
| GET | `/stats/uv-distribution` | — | UV 分布统计 |
| GET | `/stats/suspicious-ips` | `?threshold=100` | 可疑 IP 检测 |

### 系统接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/refresh` | 手动刷新/加载数据 |
| POST | `/query` | 执行自定义 SQL（Body: `{"sql":"..."}`) |
| GET | `/health` | 健康检查 |
| GET | `/debug` | 调试信息 |
| GET | `/metadata` | API 元数据及版本信息 |

### 自定义查询示例

```bash
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT remote_addr, COUNT(*) AS cnt FROM nginx_logs GROUP BY remote_addr ORDER BY cnt DESC LIMIT 10"}'
```

## 性能指标

- **吞吐量**: 支持每日 1 亿+ 日志记录
- **查询延迟**: < 100ms (95% 分位)
- **内存占用**: < 500MB
- **并发能力**: 支持 100+ 并发查询
- **存储压缩**: Parquet 列式压缩，相比原始文本节省 70%~90% 空间

## 工作流程

```
Nginx 日志 ──→ [convert] ──→ Parquet 文件 (分区存储)
                                    │
                              ┌─────▼─────┐
                              │  DuckDB    │ ← 内存列式引擎
                              │  分析查询  │
                              └─────┬─────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
               Web 仪表盘        REST API         自定义 SQL
```
