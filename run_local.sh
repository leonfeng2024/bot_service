#!/bin/bash

# 设置颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 检查docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# 创建必要的目录
mkdir -p data/postgres data/redis data/neo4j data/opensearch logs

# 创建docker网络（如果不存在）
if ! docker network ls | grep -q bibot; then
    echo -e "${GREEN}Creating bibot network...${NC}"
    docker network create bibot
fi

# 启动服务
start_services() {
    echo -e "${GREEN}Starting services...${NC}"
    docker compose -f docker-compose.yml up -d --build
    echo -e "${GREEN}All services started successfully!${NC}"
    echo -e "${GREEN}Services are running in the background.${NC}"
    echo -e "${GREEN}You can access the services at:${NC}"
    echo -e "  - Main service: http://localhost:8000"
    echo -e "  - Nginx: http://localhost:8088"
    echo -e "  - Neo4j Browser: http://localhost:7474"
    echo -e "  - OpenSearch: http://localhost:9200"
}

# 停止服务
stop_services() {
    echo -e "${GREEN}Stopping services...${NC}"
    docker compose -f docker-compose.yml down
    echo -e "${GREEN}All services stopped successfully!${NC}"
}

# 查看服务状态
status_services() {
    echo -e "${GREEN}Service status:${NC}"
    docker compose -f docker-compose.yml ps
}

# 处理命令行参数
case "$1" in
    "start")
        start_services
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        stop_services
        start_services
        ;;
    "status")
        status_services
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0 