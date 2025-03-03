#!/bin/bash

# Start the server in the background and redirect output to app.log
echo "Starting server..."
nohup python server.py > app.log 2>&1 &

# Get and display the process ID
PID=$!
echo "Server started with PID: $PID"
echo $PID > server.pid
echo "PID saved to server.pid" 