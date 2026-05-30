-- 信号技术分析结果表
CREATE TABLE IF NOT EXISTS signal_technical_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    raw_sentiment_score DECIMAL(10,2),
    rsi_14 DECIMAL(10,2),
    volume_ratio DECIMAL(10,2),
    ma5 DECIMAL(10,3),
    ma20 DECIMAL(10,3),
    ma_trend VARCHAR(10),
    momentum_5d_pct DECIMAL(10,2),
    technical_score DECIMAL(10,2),
    composite_score DECIMAL(10,2),
    overheat_penalty DECIMAL(10,2),
    rejection_reason VARCHAR(255),
    filter_result ENUM('passed','rejected','skipped') NOT NULL,
    components_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_signal (strategy_id, stock_code, trade_date),
    KEY idx_composite (composite_score),
    KEY idx_filter (filter_result)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
