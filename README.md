# Bot Service

## environment requirment
- Python 3.12+
- Node.js (for Mermaid chart generation)
- Mermaid CLI

## How to deploy
git pull
docker compose down
docker compose up -d --build

# local deploy
./run_local.sh start
./run_local.sh status
./run_local.sh stop
./run_local.sh restart