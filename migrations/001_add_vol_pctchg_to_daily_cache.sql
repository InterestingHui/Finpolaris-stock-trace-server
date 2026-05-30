-- 迁移：stock_daily_cache 增加 vol 和 pct_chg 字段
ALTER TABLE stock_daily_cache
  ADD COLUMN vol BIGINT DEFAULT NULL COMMENT '成交量（手）' AFTER low,
  ADD COLUMN pct_chg DECIMAL(10,3) DEFAULT NULL COMMENT '涨跌幅（%）' AFTER vol;
