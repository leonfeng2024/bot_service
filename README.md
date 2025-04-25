# Bot Service

## 项目概述
这是一个基于Python的服务，用于处理和分析数据库关系，并生成关系图表。

## 环境要求
- Python 3.12+
- Node.js (用于Mermaid图表生成)
- Mermaid CLI

## Docker环境配置

本项目需要在Docker环境中运行。为了支持Mermaid关系图生成功能，Dockerfile已经配置了所需的所有依赖。

### 重新构建和部署

最新版本的Dockerfile已经包含了生成Mermaid图表所需的所有依赖，包括Node.js、npm和Puppeteer相关库。使用以下命令重新构建和启动服务：

```bash
git pull
docker compose down
docker compose up -d --build
```

## Mermaid图表故障排除

如果在Docker容器中运行时遇到Mermaid图表生成问题，可以使用以下步骤进行故障排除：

### 1. 运行调试脚本

容器中包含一个调试脚本，可以检查和修复Mermaid环境问题：

```bash
docker exec -it <container_id> /app/debug_mermaid.sh
```

### 2. 检查Puppeteer依赖

如果出现与`libnss3.so`或其他库相关的错误，可能是Puppeteer依赖问题。可以在容器内执行：

```bash
docker exec -it <container_id> bash -c "apt-get update && apt-get install -y libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2"
```

### 3. 设置Puppeteer环境变量

确保以下环境变量在容器中已设置：

```
PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
PUPPETEER_NO_SANDBOX=true
```

### 4. 手动测试Mermaid CLI

在容器中执行以下命令测试Mermaid CLI是否正常工作：

```bash
docker exec -it <container_id> bash -c "echo 'graph TD; A-->B' > test.mmd && mmdc -i test.mmd -o test.png -b transparent"
```

## 其他问题

如果仍然遇到问题，请检查：

1. Node.js和npm是否正确安装：
```bash
docker exec -it <container_id> node -v
docker exec -it <container_id> npm -v
```

2. Mermaid CLI是否正确安装：
```bash
docker exec -it <container_id> mmdc -h
```