#!/bin/bash
# Finpolaris MySQL 备份脚本 — mysqldump + gzip + 7天轮转
set -e

BACKUP_DIR="/var/backups/stock-trace"
RETENTION_DAYS=7
DB_NAME="${DB_NAME:-stock_trace}"
DB_USER="${DB_USER:-root}"
DB_PASS="${DB_PASSWORD:-lianghui}"

mkdir -p "$BACKUP_DIR"

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_${DATE}.sql.gz"

mysqldump -u "$DB_USER" -p"$DB_PASS" --single-transaction --routines --triggers "$DB_NAME" | gzip > "$BACKUP_FILE"

echo "[$(date)] Backup created: $BACKUP_FILE ($(du -h "$BACKUP_FILE" | cut -f1))"

# 清理过期备份
DELETED=$(find "$BACKUP_DIR" -name "*.sql.gz" -mtime +$RETENTION_DAYS -delete -print | wc -l)
if [ "$DELETED" -gt 0 ]; then
    echo "[$(date)] Cleaned up $DELETED expired backup(s)"
fi
