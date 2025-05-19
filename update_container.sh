#!/bin/bash

# check if the container is running
if ! docker ps | grep -q "bot_service"; then
    echo "Error: bot_service container is not running"
    exit 1
fi

# get container ID
CONTAINER_ID=$(docker ps -q -f name=bot_service)

# define directories and files to sync
DIRS=(
    "Retriever"
    "service"
    "utils"
    "tools"
    "models"
    "Mock"
)

FILES=(
    "config.py"
    "server.py"
    "requirements.txt"
    "puppeteer-config.json"
)

# sync directories
for dir in "${DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "Syncing directory: $dir"
        docker cp "$dir" "$CONTAINER_ID:/app/"
    else
        echo "Warning: Directory $dir does not exist"
    fi
done

# sync files
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "Syncing file: $file"
        docker cp "$file" "$CONTAINER_ID:/app/"
    else
        echo "Warning: File $file does not exist"
    fi
done

# restart uvicorn service
echo "Restarting uvicorn service..."
docker exec "$CONTAINER_ID" sh -c "kill $(ps aux | grep 'uvicorn' | grep -v grep | awk '{print $2}')"
docker exec "$CONTAINER_ID" uvicorn server:app --host 0.0.0.0 --port 8000 &

echo "Update completed successfully!"