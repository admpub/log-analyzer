#!/bin/bash
set -e

echo "=========================================="
echo "Log Analyzer Deployment Script"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
check_command() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

# 检查 root 权限
if [[ $EUID -ne 0 ]]; then
    print_warn "This script requires root privileges. Some operations may fail."
fi

# 检查必要命令
print_info "Checking required commands..."
check_command docker
check_command docker-compose
check_command go
check_command git

# 创建目录结构
print_info "Creating directory structure..."
mkdir -p /data/logs/nginx
mkdir -p /var/log/log-analyzer
mkdir -p /etc/log-analyzer

# 配置 Nginx
print_info "Configuring Nginx..."
if [ -f /etc/nginx/nginx.conf ]; then
    if ! grep -q "json_log" /etc/nginx/nginx.conf; then
        cat >> /etc/nginx/nginx.conf << 'EOF'

# Log format for JSON logging
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

# Access log
access_log /var/log/nginx/access.log json_log;
EOF
        print_info "Nginx configuration updated. Please restart Nginx."
    fi
else
    print_warn "Nginx configuration not found. Please configure Nginx manually."
fi

# 复制配置文件
print_info "Copying configuration files..."
cp -r config/* /etc/log-analyzer/ 2>/dev/null || true
cp vector/vector.toml /etc/vector/ 2>/dev/null || true

# 构建项目
print_info "Building the project..."
if [ -f "go.mod" ]; then
    go mod download
    go build -o log-analyzer ./cmd/log-analyzer/
    print_info "Build successful!"
else
    print_warn "go.mod not found. Skipping build."
fi

# 创建 systemd 服务文件
print_info "Creating systemd service..."
cat > /etc/systemd/system/log-analyzer.service << EOF
[Unit]
Description=Log Analyzer
After=network.target
Requires=network.target

[Service]
Type=simple
User=nobody
Group=nogroup
WorkingDirectory=/opt/log-analyzer
ExecStart=/opt/log-analyzer/log-analyzer
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=log-analyzer
Environment=GIN_MODE=release

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/data/logs/nginx /var/log/log-analyzer

[Install]
WantedBy=multi-user.target
EOF

# 复制可执行文件
if [ -f "log-analyzer" ]; then
    mkdir -p /opt/log-analyzer
    cp log-analyzer /opt/log-analyzer/
    cp -r web/static /opt/log-analyzer/
    cp config/config.yaml /opt/log-analyzer/
    
    chown -R nobody:nogroup /opt/log-analyzer
    chmod 750 /opt/log-analyzer
    chmod 550 /opt/log-analyzer/log-analyzer
    
    print_info "Enabling and starting service..."
    systemctl daemon-reload
    systemctl enable log-analyzer
    systemctl start log-analyzer
fi

# 创建 Docker Compose 配置
print_info "Creating docker-compose.yml for Docker deployment..."
if [ ! -f "docker-compose.yml" ]; then
    cp docker-compose.example.yml docker-compose.yml
fi

# 创建数据目录权限
chmod 755 /data/logs/nginx
chown -R nobody:nogroup /data/logs/nginx 2>/dev/null || true

print_info "=========================================="
print_info "Deployment completed!"
print_info "=========================================="
echo ""
print_info "Access the web interface at: http://localhost:8080"
print_info "API documentation at: http://localhost:8080/api/metadata"
echo ""
print_info "To use Docker Compose:"
echo "  docker-compose up -d"
echo ""
print_info "To use systemd service:"
echo "  systemctl status log-analyzer"
echo ""
print_info "Log files location:"
echo "  Application logs: /var/log/log-analyzer/"
echo "  Nginx logs: /var/log/nginx/"
echo "  Parquet files: /data/logs/nginx/"
echo ""
print_info "Configuration files:"
echo "  Main config: /etc/log-analyzer/config.yaml"
echo "  Vector config: /etc/vector/vector.toml"
echo ""
print_warn "Please restart Nginx for log format changes to take effect:"
echo "  systemctl restart nginx"
echo ""
