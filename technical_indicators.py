"""技术指标计算模块 — 用于信号过滤和复合评分"""

import os
import sys
from datetime import date, timedelta
import numpy as np
import pandas as pd
import tushare as ts

# 复用 app.py 的数据库和 Tushare 配置
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

import pymysql
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', 'lianghui'),
    'database': os.environ.get('DB_NAME', 'stock_trace'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def _get_db():
    return pymysql.connect(**DB_CONFIG)


def get_historical_daily(stock_code: str, trade_date: date, lookback: int = 25):
    """获取指定股票的历史日线数据（优先 MySQL 缓存，miss 则从 Tushare 拉取）"""
    start_date = trade_date - timedelta(days=lookback * 2)  # 多取一些确保足够交易日
    start_str = start_date.strftime('%Y%m%d')
    end_str = trade_date.strftime('%Y%m%d')

    # 1. 尝试从 MySQL 批量读取
    rows = _fetch_cached_range(stock_code, start_date, trade_date)
    if rows is not None and len(rows) >= lookback:
        df = pd.DataFrame(rows)
        df = df.sort_values('trade_date').tail(lookback).reset_index(drop=True)
        return df

    # 2. Tushare 拉取
    try:
        df = pro.daily(ts_code=stock_code, start_date=start_str, end_date=end_str)
        if df.empty:
            return None
        df = df.sort_values('trade_date').tail(lookback).reset_index(drop=True)
        # 缓存到 MySQL
        _cache_range(stock_code, df)
        return df
    except Exception as e:
        print(f"[技术指标] Tushare 获取历史数据失败 {stock_code}: {e}")
        return None


def _fetch_cached_range(stock_code, start_date, end_date):
    """从 stock_daily_cache 批量读取（含 vol, pct_chg）"""
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date, open, close, high, low, vol, pct_chg
                FROM stock_daily_cache
                WHERE stock_code = %s AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (stock_code, start_date, end_date))
            rows = cursor.fetchall()
            if not rows:
                return None
            # 转换类型
            for r in rows:
                r['trade_date'] = r['trade_date'] if isinstance(r['trade_date'], date) else date.fromisoformat(str(r['trade_date']))
                for k in ('open', 'close', 'high', 'low'):
                    r[k] = float(r[k]) if r[k] is not None else 0.0
                r['vol'] = int(r['vol']) if r.get('vol') is not None else 0
                r['pct_chg'] = float(r['pct_chg']) if r.get('pct_chg') is not None else 0.0
            return rows
    except Exception as e:
        print(f"[技术指标] MySQL 缓存读取失败: {e}")
        return None
    finally:
        conn.close()


def _cache_range(stock_code, df):
    """批量缓存日线数据到 MySQL"""
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                td = pd.to_datetime(row['trade_date']).date()
                cursor.execute("""
                    INSERT INTO stock_daily_cache
                    (stock_code, trade_date, open, close, high, low, vol, pct_chg)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    open=VALUES(open), close=VALUES(close), high=VALUES(high), low=VALUES(low),
                    vol=VALUES(vol), pct_chg=VALUES(pct_chg)
                """, (
                    stock_code, td,
                    float(row['open']), float(row['close']),
                    float(row['high']), float(row['low']),
                    int(row['vol']) if pd.notna(row.get('vol')) else 0,
                    float(row['pct_chg']) if pd.notna(row.get('pct_chg')) else 0.0,
                ))
        conn.commit()
    except Exception as e:
        print(f"[技术指标] MySQL 缓存写入失败: {e}")
    finally:
        conn.close()


def calculate_rsi(closes: pd.Series, period: int = 14) -> float:
    """计算 Wilder's RSI"""
    if len(closes) < period + 1:
        return 50.0  # 数据不足返回中性值
    deltas = closes.diff().dropna()
    gains = deltas.where(deltas > 0, 0.0)
    losses = (-deltas).where(deltas < 0, 0.0)

    avg_gain = gains.iloc[:period].mean()
    avg_loss = losses.iloc[:period].mean()

    if avg_loss == 0:
        return 100.0

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains.iloc[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses.iloc[i]) / period

    rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_volume_ratio(volumes: pd.Series, window: int = 20) -> float:
    """计算量比：今日成交量 / N日平均成交量"""
    if len(volumes) < window + 1 or volumes.iloc[-1] == 0:
        return 1.0
    avg_vol = volumes.iloc[-(window + 1):-1].mean()
    if avg_vol == 0:
        return 1.0
    return float(volumes.iloc[-1] / avg_vol)


def calculate_ma_trend(closes: pd.Series):
    """计算 MA5/MA20 趋势"""
    ma5 = float(closes.rolling(5, min_periods=1).mean().iloc[-1])
    if len(closes) >= 20:
        ma20 = float(closes.rolling(20, min_periods=1).mean().iloc[-1])
    else:
        ma20 = ma5  # 数据不足时取 MA5

    if ma5 > ma20:
        trend = 'bullish'
        strength = (ma5 - ma20) / ma20 * 100
    elif ma5 < ma20:
        trend = 'bearish'
        strength = (ma5 - ma20) / ma20 * 100
    else:
        trend = 'neutral'
        strength = 0.0

    return {'ma5': ma5, 'ma20': ma20, 'trend': trend, 'strength_pct': strength}


def calculate_momentum(closes: pd.Series, period: int = 5) -> float:
    """计算 N日涨跌幅（%）"""
    if len(closes) < period + 1:
        return 0.0
    return float((closes.iloc[-1] - closes.iloc[-(period + 1)]) / closes.iloc[-(period + 1)] * 100)


def calculate_macd(closes: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """计算 MACD 指标（DIF/DEA/柱状图）"""
    result = {
        'macd_line': 0.0, 'macd_signal': 0.0,
        'macd_hist': 0.0, 'cross': 'none',
    }
    if len(closes) < slow + signal:
        return result

    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    result['macd_line'] = float(macd_line.iloc[-1])
    result['macd_signal'] = float(macd_signal.iloc[-1])
    result['macd_hist'] = float(macd_hist.iloc[-1])

    # 检测金叉/死叉（最近两根柱子符号变化）
    if len(macd_hist) >= 2:
        prev = float(macd_hist.iloc[-2])
        curr = float(macd_hist.iloc[-1])
        if prev <= 0 < curr:
            result['cross'] = 'golden'
        elif prev >= 0 > curr:
            result['cross'] = 'death'

    return result


def calculate_bollinger_bands(closes: pd.Series, period: int = 20, num_std: float = 2.0) -> dict:
    """计算布林带：percent_b 和 bandwidth"""
    if len(closes) < period:
        return {'upper': 0, 'middle': 0, 'lower': 0, 'percent_b': 0.5, 'bandwidth': 0}
    ma = closes.rolling(period).mean()
    std = closes.rolling(period).std()
    upper = ma + num_std * std
    lower = ma - num_std * std
    curr_close = float(closes.iloc[-1])
    curr_upper = float(upper.iloc[-1])
    curr_lower = float(lower.iloc[-1])
    curr_middle = float(ma.iloc[-1])
    band_width = (curr_upper - curr_lower) / curr_middle if curr_middle != 0 else 0
    percent_b = (curr_close - curr_lower) / (curr_upper - curr_lower) if curr_upper != curr_lower else 0.5
    return {
        'upper': round(curr_upper, 4),
        'middle': round(curr_middle, 4),
        'lower': round(curr_lower, 4),
        'percent_b': round(max(0, min(1.5, percent_b)), 4),
        'bandwidth': round(float(band_width), 4),
    }


def calculate_kdj(highs: pd.Series, lows: pd.Series, closes: pd.Series,
                  k_period: int = 9, d_period: int = 3) -> dict:
    """计算 KDJ 随机指标"""
    if len(closes) < k_period:
        return {'k': 50, 'd': 50, 'j': 50, 'cross': 'none'}

    lowest_low = lows.rolling(k_period, min_periods=1).min()
    highest_high = highs.rolling(k_period, min_periods=1).max()
    rsv = (closes - lowest_low) / (highest_high - lowest_low) * 100
    rsv = rsv.fillna(50)

    k = rsv.ewm(com=d_period - 1, adjust=False).mean()
    d = k.ewm(com=d_period - 1, adjust=False).mean()
    j = 3 * k - 2 * d

    curr_k = float(k.iloc[-1])
    curr_d = float(d.iloc[-1])
    curr_j = float(j.iloc[-1])

    cross = 'none'
    if len(k) >= 2:
        prev_k, prev_d = float(k.iloc[-2]), float(d.iloc[-2])
        if prev_k <= prev_d and curr_k > curr_d:
            cross = 'golden'
        elif prev_k >= prev_d and curr_k < curr_d:
            cross = 'death'

    return {'k': round(curr_k, 2), 'd': round(curr_d, 2), 'j': round(curr_j, 2), 'cross': cross}


def calculate_relative_strength(stock_closes: pd.Series, index_closes: pd.Series,
                                 period: int = 10) -> float:
    """计算相对强度：股票 vs 指数的 N 日收益率差"""
    if len(stock_closes) < period + 1 or len(index_closes) < period + 1:
        return 0.0
    stock_ret = (float(stock_closes.iloc[-1]) / float(stock_closes.iloc[-(period + 1)]) - 1) * 100
    index_ret = (float(index_closes.iloc[-1]) / float(index_closes.iloc[-(period + 1)]) - 1) * 100
    return round(stock_ret - index_ret, 4)


def get_index_closes(index_code: str, trade_date: date, lookback: int = 15) -> pd.Series:
    """获取指数收盘价序列"""
    start_date = trade_date - timedelta(days=lookback * 2)
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT close FROM index_prices
                WHERE index_code = %s AND date BETWEEN %s AND %s
                ORDER BY date
            """, (index_code, start_date, trade_date))
            rows = cursor.fetchall()
            if len(rows) >= lookback:
                return pd.Series([float(r['close']) for r in rows])
    except Exception:
        pass
    finally:
        conn.close()
    # fallback: 从 Tushare 拉
    try:
        df = pro.index_daily(ts_code=index_code,
                             start_date=start_date.strftime('%Y%m%d'),
                             end_date=trade_date.strftime('%Y%m%d'))
        if df is not None and len(df) >= lookback:
            df = df.sort_values('trade_date').tail(lookback)
            return df['close'].astype(float).reset_index(drop=True)
    except Exception:
        pass
    return pd.Series()


def compute_technical_profile(stock_code: str, trade_date: date) -> dict:
    """计算单只股票的完整技术面档案"""
    result = {
        'rsi': 50.0, 'volume_ratio': 1.0,
        'ma5': 0.0, 'ma20': 0.0,
        'ma_trend': 'neutral', 'ma_strength_pct': 0.0,
        'momentum_5d_pct': 0.0,
        'macd_line': 0.0, 'macd_signal': 0.0,
        'macd_hist': 0.0, 'macd_cross': 'none', 'macd_score': 40.0,
        'rsi_score': 50.0, 'ma_score': 50.0,
        'momentum_score': 50.0, 'volume_score': 50.0,
        'bb_percent_b': 0.5, 'bb_bandwidth': 0.0, 'bb_score': 50.0,
        'relative_strength_10d': 0.0, 'rs_score': 50.0,
        'kdj_k': 50.0, 'kdj_d': 50.0, 'kdj_j': 50.0, 'kdj_cross': 'none', 'kdj_score': 50.0,
        'technical_score': 50.0,
        'data_available': False, 'data_error': None,
    }

    df = get_historical_daily(stock_code, trade_date)
    if df is None or len(df) < 15:
        result['data_error'] = f'数据不足（获取 {0 if df is None else len(df)} 行，需 15+）'
        return result

    closes = df['close'].astype(float)
    volumes = df['vol'].astype(float)
    highs = df['high'].astype(float) if 'high' in df.columns else closes
    lows = df['low'].astype(float) if 'low' in df.columns else closes

    # RSI
    result['rsi'] = calculate_rsi(closes)
    rsi = result['rsi']
    if 40 <= rsi <= 65:
        result['rsi_score'] = 100.0
    elif 30 <= rsi < 40:
        result['rsi_score'] = 50.0 + (rsi - 30) * 5
    elif 65 < rsi <= 75:
        result['rsi_score'] = 100.0 - (rsi - 65) * 6
    elif rsi < 30:
        result['rsi_score'] = max(0, rsi * 1.5)
    else:
        result['rsi_score'] = max(0, 40.0 - (rsi - 75) * 2.67)

    # 量比
    result['volume_ratio'] = calculate_volume_ratio(volumes)
    vr = result['volume_ratio']
    if 1.2 <= vr <= 2.0:
        result['volume_score'] = 100.0
    elif 0.8 <= vr < 1.2:
        result['volume_score'] = 70.0 + (vr - 0.8) * 75
    elif 2.0 < vr <= 3.0:
        result['volume_score'] = 100.0 - (vr - 2.0) * 30
    elif vr < 0.8:
        result['volume_score'] = max(0, vr / 0.8 * 70)
    else:
        result['volume_score'] = max(0, 70.0 - (vr - 3.0) * 35)

    # MA 趋势
    ma_info = calculate_ma_trend(closes)
    result['ma5'] = ma_info['ma5']
    result['ma20'] = ma_info['ma20']
    result['ma_trend'] = ma_info['trend']
    result['ma_strength_pct'] = ma_info['strength_pct']
    if ma_info['trend'] == 'bullish':
        result['ma_score'] = min(100, 60 + abs(ma_info['strength_pct']) * 10)
    elif ma_info['trend'] == 'bearish':
        result['ma_score'] = max(0, 40 - abs(ma_info['strength_pct']) * 10)
    else:
        result['ma_score'] = 50.0

    # 动量
    result['momentum_5d_pct'] = calculate_momentum(closes)
    mom = result['momentum_5d_pct']
    if 1.0 <= mom <= 3.0:
        result['momentum_score'] = 100.0
    elif 0 <= mom < 1.0:
        result['momentum_score'] = 70.0 + mom * 30
    elif -2.0 <= mom < 0:
        result['momentum_score'] = 50.0 + (mom + 2.0) * 10
    elif 3.0 < mom <= 5.0:
        result['momentum_score'] = 100.0 - (mom - 3.0) * 30
    elif mom > 5.0:
        result['momentum_score'] = max(0, 40.0 - (mom - 5.0) * 10)
    else:
        result['momentum_score'] = max(0, 30.0 + (mom + 2.0) * 10)

    # MACD
    macd_info = calculate_macd(closes)
    result['macd_line'] = macd_info['macd_line']
    result['macd_signal'] = macd_info['macd_signal']
    result['macd_hist'] = macd_info['macd_hist']
    result['macd_cross'] = macd_info['cross']
    if macd_info['cross'] == 'golden':
        result['macd_score'] = 100.0
    elif macd_info['macd_hist'] > 0 and macd_info['macd_line'] > 0:
        result['macd_score'] = 80.0
    elif macd_info['macd_hist'] > 0:
        result['macd_score'] = 65.0
    elif macd_info['cross'] == 'death':
        result['macd_score'] = 10.0
    elif macd_info['macd_hist'] < 0 and macd_info['macd_line'] < 0:
        result['macd_score'] = 20.0
    else:
        result['macd_score'] = 40.0

    # 布林带
    bb = calculate_bollinger_bands(closes)
    result['bb_percent_b'] = bb['percent_b']
    result['bb_bandwidth'] = bb['bandwidth']
    pb = bb['percent_b']
    if 0.3 <= pb <= 0.7:
        result['bb_score'] = 100.0
    elif 0.1 <= pb < 0.3:
        result['bb_score'] = 50.0 + (pb - 0.1) / 0.2 * 50
    elif 0.7 < pb <= 0.9:
        result['bb_score'] = 100.0 - (pb - 0.7) / 0.2 * 50
    elif pb < 0.1:
        result['bb_score'] = 40.0
    else:
        result['bb_score'] = max(0, 50.0 - (pb - 0.9) / 0.1 * 50)

    # KDJ
    kdj = calculate_kdj(highs, lows, closes)
    result['kdj_k'] = kdj['k']
    result['kdj_d'] = kdj['d']
    result['kdj_j'] = kdj['j']
    result['kdj_cross'] = kdj['cross']
    k, d = kdj['k'], kdj['d']
    if k < 20 and d < 20 and kdj['cross'] == 'golden':
        result['kdj_score'] = 100.0
    elif k < 20 and d < 20:
        result['kdj_score'] = 80.0
    elif k > 80 and d > 80:
        result['kdj_score'] = 10.0
    elif k > 80:
        result['kdj_score'] = 25.0
    elif kdj['cross'] == 'golden':
        result['kdj_score'] = 80.0
    elif kdj['cross'] == 'death':
        result['kdj_score'] = 20.0
    elif 40 <= k <= 60:
        result['kdj_score'] = 60.0
    else:
        result['kdj_score'] = 50.0

    # 相对强度 vs 沪深300
    index_closes = get_index_closes('000300.SH', trade_date)
    rs = calculate_relative_strength(closes, index_closes) if len(index_closes) > 10 else 0.0
    result['relative_strength_10d'] = rs
    if rs > 3.0:
        result['rs_score'] = 100.0
    elif rs > 1.0:
        result['rs_score'] = 70.0 + (rs - 1.0) / 2.0 * 30
    elif rs > -1.0:
        result['rs_score'] = 50.0 + (rs + 1.0) / 2.0 * 20
    elif rs > -3.0:
        result['rs_score'] = 30.0 + (rs + 3.0) / 2.0 * 20
    else:
        result['rs_score'] = max(0, 30.0 + (rs + 3.0) * 5)

    # 复合技术分（V2 权重）
    result['technical_score'] = (
        result['rsi_score'] * 0.15 +
        result['ma_score'] * 0.15 +
        result['momentum_score'] * 0.10 +
        result['volume_score'] * 0.15 +
        result['macd_score'] * 0.10 +
        result['bb_score'] * 0.15 +
        result['kdj_score'] * 0.10 +
        result['rs_score'] * 0.10
    )

    result['data_available'] = True
    return result


def check_signal_confirmation(stock_code: str, trade_date: date) -> dict:
    """检查股票在多日内是否持续出现在舆情报告中（持续性确认）"""
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as days_present,
                       AVG(total_score) as avg_score,
                       AVG(average_score) as avg_direction
                FROM omni_stock_digest_report
                WHERE stock_code = %s
                  AND report_date BETWEEN %s AND %s
                  AND stock_code != '000000'
            """, (stock_code, trade_date - timedelta(days=4), trade_date))
            row = cursor.fetchone()
            if row and row['days_present'] >= 2:
                return {
                    'confirmed': True,
                    'days_present': int(row['days_present']),
                    'avg_sentiment': float(row['avg_score'] or 0),
                    'avg_direction': float(row['avg_direction'] or 0),
                }
            return {'confirmed': False, 'days_present': int(row['days_present'] if row else 0)}
    except Exception:
        return {'confirmed': False, 'days_present': 0}
    finally:
        conn.close()
