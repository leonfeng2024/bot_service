#!/bin/bash

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run with sudo: sudo ./run_local.sh"
  exit 1
fi

echo "=== Setting up local environment ==="

# 1. Replace nginx configuration
echo "Copying nginx configuration to /etc/nginx/nginx.conf..."
cp nginx/nginx_localhost.conf /etc/nginx/nginx.conf

# 2. Restart nginx service
echo "Restarting nginx service..."
if [ "$(uname)" == "Darwin" ]; then
    # macOS
    brew services restart nginx
else
    # Linux
    systemctl restart nginx
fi

# 3. Run the FastAPI application in the background
echo "Starting FastAPI application on port 8000..."
nohup uvicorn server:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &

# Store the PID
echo $! > uvicorn.pid
echo "FastAPI server started with PID $(cat uvicorn.pid)"
echo "Log file: uvicorn.log"

echo "=== Setup complete! ==="
echo "Service is now running at http://localhost:8000" 