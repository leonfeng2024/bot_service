#!/bin/bash

# Check if PID file exists
if [ -f "server.pid" ]; then
    PID=$(cat server.pid)
    echo "Found server process with PID: $PID"
    
    # Check if the process is still running
    if ps -p $PID > /dev/null; then
        echo "Stopping server process..."
        kill $PID
        echo "Server stopped."
    else
        echo "Server process is not running."
    fi
    
    # Remove the PID file
    rm server.pid
    echo "Removed PID file."
else
    echo "No server.pid file found. Server may not be running."
fi 