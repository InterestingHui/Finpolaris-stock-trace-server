#!/bin/bash
# Finpolaris Stock-Trace-Server 一键部署脚本
# 用法: bash deploy.sh
set -e

REMOTE="root@47.239.193.29"
REMOTE_DIR="/opt/stock-trace"
SERVICE="stock-trace"

echo "=== Deploying Stock-Trace-Server to $REMOTE ==="

# 1. 上传核心文件
echo "[1/4] Uploading files..."
scp signal_scorer.py "$REMOTE:$REMOTE_DIR/signal_scorer.py"
scp technical_indicators.py "$REMOTE:$REMOTE_DIR/technical_indicators.py"
scp gunicorn_config.py "$REMOTE:$REMOTE_DIR/gunicorn_config.py"
scp app.py "$REMOTE:$REMOTE_DIR/app.py"

# 2. 上传脚本
echo "[2/4] Uploading scripts..."
ssh "$REMOTE" "mkdir -p $REMOTE_DIR/scripts"
scp scripts/backup_db.sh "$REMOTE:$REMOTE_DIR/scripts/backup_db.sh"
scp scripts/health_check.sh "$REMOTE:$REMOTE_DIR/scripts/health_check.sh" 2>/dev/null || true
ssh "$REMOTE" "chmod +x $REMOTE_DIR/scripts/*.sh"

# 3. 安装依赖
echo "[3/4] Installing dependencies..."
ssh "$REMOTE" "cd $REMOTE_DIR && pip3 install -r requirements.txt -q 2>&1 | tail -3"

# 4. 重启服务 + 健康检查
echo "[4/4] Restarting service..."
ssh "$REMOTE" "systemctl restart $SERVICE && sleep 3"
echo ""
echo "Health check:"
ssh "$REMOTE" "curl -s http://127.0.0.1:5000/api/health | python3 -m json.tool"
echo ""
echo "=== Deploy complete ==="
