# 构建阶段
FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o log-analyzer ./cmd/log-analyzer/

# 运行阶段
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app/
COPY --from=builder /app/log-analyzer .
COPY config/config_docker.yaml ./config/config.yaml
COPY web/static ./web/static

# 创建数据目录
RUN mkdir -p /data/logs

EXPOSE 8080
CMD ["./log-analyzer", "server"]
