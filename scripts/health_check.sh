#!/bin/bash
# Finpolaris 健康检查脚本 — stock-trace / market-tracing / MySQL / 磁盘
LOG="/var/log/health-check.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

check_http() {
    curl -sf --max-time 5 "$1" > /dev/null 2>&1 && echo "OK" || echo "FAIL"
}

ST=$(check_http "http://127.0.0.1:5000/api/health")
MT=$(check_http "http://127.0.0.1:8003/api/health")
MYSQL=$(systemctl is-active --quiet mysql 2>/dev/null && echo "OK" || echo "FAIL")
DISK=$(df / | tail -1 | awk '{print $5}' | tr -d '%')

echo "[$TIMESTAMP] stock-trace: $ST | market-tracing: $MT | mysql: $MYSQL | disk: ${DISK}%" >> "$LOG"

# 磁盘告警
if [ "$DISK" -gt 85 ]; then
    echo "[$TIMESTAMP] WARNING: Disk usage at ${DISK}%" >> "$LOG"
fi

# 服务异常告警
if [ "$ST" = "FAIL" ] || [ "$MT" = "FAIL" ]; then
    echo "[$TIMESTAMP] ALERT: Service failure detected (ST=$ST, MT=$MT)" >> "$LOG"
fi
