-- 004: 新增 V2 技术指标列（布林带、相对强度、KDJ）
ALTER TABLE signal_technical_analysis
    ADD COLUMN bb_percent_b DECIMAL(10,4) DEFAULT NULL AFTER macd_score,
    ADD COLUMN bb_bandwidth DECIMAL(10,4) DEFAULT NULL AFTER bb_percent_b,
    ADD COLUMN bb_score DECIMAL(10,2) DEFAULT NULL AFTER bb_bandwidth,
    ADD COLUMN relative_strength_10d DECIMAL(10,4) DEFAULT NULL AFTER bb_score,
    ADD COLUMN rs_score DECIMAL(10,2) DEFAULT NULL AFTER relative_strength_10d,
    ADD COLUMN kdj_k DECIMAL(10,2) DEFAULT NULL AFTER rs_score,
    ADD COLUMN kdj_d DECIMAL(10,2) DEFAULT NULL AFTER kdj_k,
    ADD COLUMN kdj_j DECIMAL(10,2) DEFAULT NULL AFTER kdj_d,
    ADD COLUMN kdj_cross VARCHAR(10) DEFAULT NULL AFTER kdj_j,
    ADD COLUMN kdj_score DECIMAL(10,2) DEFAULT NULL AFTER kdj_cross;
