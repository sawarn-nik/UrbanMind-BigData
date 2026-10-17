#!/usr/bin/env bash
# Step 08: start the pipeline dashboard at http://localhost:8050
mkdir -p /workspace/output/logs
pkill -f dashboard/server.py || true
nohup python3 -u /workspace/dashboard/server.py > /workspace/output/logs/dashboard.log 2>&1 &
sleep 2 && cat /workspace/output/logs/dashboard.log
