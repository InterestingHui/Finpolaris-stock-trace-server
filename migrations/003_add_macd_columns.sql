-- 新增 MACD 相关字段
ALTER TABLE signal_technical_analysis
    ADD COLUMN macd_line DECIMAL(10,4) DEFAULT NULL AFTER momentum_5d_pct,
    ADD COLUMN macd_signal DECIMAL(10,4) DEFAULT NULL AFTER macd_line,
    ADD COLUMN macd_hist DECIMAL(10,4) DEFAULT NULL AFTER macd_signal,
    ADD COLUMN macd_cross VARCHAR(10) DEFAULT NULL AFTER macd_hist,
    ADD COLUMN macd_score DECIMAL(10,2) DEFAULT NULL AFTER macd_cross;
