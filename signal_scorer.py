"""信号复合评分与过滤模块"""

import math
import os
from datetime import date, timedelta

import pymysql

# ========== 可调参数 ==========
W_SENTIMENT = 0.35    # 舆情权重
W_TECHNICAL = 0.65    # 技术面权重

# 硬过滤阈值
RSI_REJECT_THRESHOLD = 80
VOL_RATIO_LOW = 0.4
MOMENTUM_HIGH_WITH_LOW_VOL = 3.0
MOMENTUM_DOWNTREND = -5.0
MOMENTUM_CHASE_REJECT = 6.0

# 舆情趋势惩罚参数
SENTIMENT_TREND_LOOKBACK = 3       # 回溯天数
SENTIMENT_DECLINE_PENALTY = 15     # 舆情减速惩罚
SENTIMENT_ACCEL_BONUS = 5          # 舆情加速加成

# 连续信号衰减参数
CONSECUTIVE_SIGNAL_LOOKBACK = 3    # 同股票 N 天内重复信号
CONSECUTIVE_DECAY_PENALTY = 20     # 重复信号惩罚分

# 过热惩罚梯度：(sentiment阈值, RSI阈值, 惩罚分)
OVERHEAT_TIERS = [
    (0.90, 60, 100),   # 极端舆情 + 中等RSI = 严重过热
    (0.85, 75, 90),    # 高舆情 + 高RSI
    (0.80, 70, 60),
    (0.75, 65, 35),
    (0.70, 80, 40),    # 中等舆情 + 极高RSI
]

# 数据库配置（与 app.py 共用环境变量）
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', 'lianghui'),
    'database': os.environ.get('DB_NAME', 'stock_trace'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
}


def _get_db():
    return pymysql.connect(**DB_CONFIG)


def compute_sentiment_trend(stock_code: str, trade_date: date, lookback: int = None) -> dict:
    """查询舆情趋势：最近 N 天舆情得分变化方向"""
    if lookback is None:
        lookback = SENTIMENT_TREND_LOOKBACK
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT report_date, total_score
                FROM omni_stock_digest_report
                WHERE stock_code = %s AND report_date BETWEEN %s AND %s
                ORDER BY report_date DESC
                LIMIT %s
            """, (stock_code, trade_date - timedelta(days=lookback * 2), trade_date, lookback))
            rows = cursor.fetchall()
            if len(rows) < 2:
                return {'trend': 'stable', 'momentum': 0.0, 'scores': []}
            scores = [float(r['total_score'] or 0) for r in rows]
            scores.reverse()
            if len(scores) >= 2:
                last = scores[-1]
                prev = scores[-2]
                if prev != 0:
                    momentum = (last - prev) / abs(prev)
                else:
                    momentum = 0.0
            else:
                momentum = 0.0
            if momentum > 0.2:
                trend = 'accelerating'
            elif momentum < -0.2:
                trend = 'declining'
            else:
                trend = 'stable'
            return {'trend': trend, 'momentum': round(momentum, 4), 'scores': scores}
    except Exception:
        return {'trend': 'stable', 'momentum': 0.0, 'scores': []}
    finally:
        conn.close()


def compute_consecutive_penalty(stock_code: str, trade_date: date, lookback: int = None) -> float:
    """计算同股票连续信号衰减惩罚：同一股票 N 天内重复出现则惩罚"""
    if lookback is None:
        lookback = CONSECUTIVE_SIGNAL_LOOKBACK
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt
                FROM omni_strategy_trade_signal
                WHERE stock_code = %s AND trade_date BETWEEN %s AND %s
                  AND signal_type = 'buy'
            """, (stock_code, trade_date - timedelta(days=lookback), trade_date - timedelta(days=1)))
            row = cursor.fetchone()
            cnt = int(row['cnt']) if row else 0
            if cnt >= 3:
                return CONSECUTIVE_DECAY_PENALTY * 1.5
            elif cnt >= 2:
                return CONSECUTIVE_DECAY_PENALTY
            elif cnt >= 1:
                return CONSECUTIVE_DECAY_PENALTY * 0.5
            return 0.0
    except Exception:
        return 0.0
    finally:
        conn.close()


def normalize_sentiment(raw_score: float, consensus: float = 1.0) -> float:
    """Sigmoid 归一化舆情得分到 [0, 1]，受 consensus 调整"""
    if raw_score == 0:
        return 0.5
    base = 1.0 / (1.0 + math.exp(-raw_score / 100.0))
    if consensus < 0.3:
        base *= 0.7  # 多空分歧，降权
    elif consensus > 0.7:
        base *= 1.1  # 强共识，轻微加成
    return min(1.0, base)


def compute_overheat_penalty(sentiment_normalized: float, rsi: float) -> float:
    """舆情过热惩罚：高舆情 + 高 RSI = 大概率见顶"""
    for sent_threshold, rsi_threshold, penalty in OVERHEAT_TIERS:
        if sentiment_normalized >= sent_threshold and rsi >= rsi_threshold:
            return float(penalty)
    return 0.0


def should_reject_signal(technical_profile: dict, sentiment_norm: float = 0.5) -> tuple:
    """硬过滤：满足任一条件直接拒绝信号"""
    rsi = technical_profile.get('rsi', 50.0)
    vol_ratio = technical_profile.get('volume_ratio', 1.0)
    momentum = technical_profile.get('momentum_5d_pct', 0.0)
    ma_trend = technical_profile.get('ma_trend', 'neutral')
    macd_cross = technical_profile.get('macd_cross', 'none')
    bb_percent_b = technical_profile.get('bb_percent_b', 0.5)
    rs_10d = technical_profile.get('relative_strength_10d', 0.0)

    # RSI 超买
    if rsi > RSI_REJECT_THRESHOLD:
        return True, f'RSI超买({rsi:.1f}>{RSI_REJECT_THRESHOLD})'

    # 量缩价涨（看跌背离）
    if vol_ratio < VOL_RATIO_LOW and momentum > MOMENTUM_HIGH_WITH_LOW_VOL:
        return True, f'量缩价涨(量比{vol_ratio:.2f}<{VOL_RATIO_LOW}, 动量{momentum:.1f}%>{MOMENTUM_HIGH_WITH_LOW_VOL}%)'

    # 明确下跌趋势（RSI >= 35 时才拒绝，超卖可能是反转机会）
    if ma_trend == 'bearish' and momentum < MOMENTUM_DOWNTREND:
        if rsi < 35:
            # 超卖 + 高舆情 = 潜在反转，不拒绝但记录
            pass
        else:
            return True, f'下跌趋势中(MA空头, 动量{momentum:.1f}%<{MOMENTUM_DOWNTREND}%)'

    # 追涨过猛
    if momentum > MOMENTUM_CHASE_REJECT:
        return True, f'追涨过猛(5日涨{momentum:.1f}%>{MOMENTUM_CHASE_REJECT}%)'

    # MACD 死叉 + 高 RSI
    if macd_cross == 'death' and rsi > 65:
        return True, f'MACD死叉+RSI偏高(死叉, RSI{rsi:.1f}>65)'

    # 布林带上轨突破 + 巨量（抛物线见顶）
    if bb_percent_b > 0.95 and vol_ratio > 2.5:
        return True, f'布林上轨突破+巨量(%B={bb_percent_b:.2f}, 量比={vol_ratio:.2f})'

    # 相对强度极弱 + 高舆情（弱股强舆情=资金陷阱）
    # RS < -8%: 极度弱势，直接拒绝
    if rs_10d < -8.0 and sentiment_norm > 0.8:
        return True, f'相对强度极弱+高舆情(RS={rs_10d:.1f}%<-8%, 舆情={sentiment_norm:.2f})'
    # RS -8%~-3% + 高舆情: 只有在 RSI >= 35（非超卖）时才拒绝 — 超卖可能是反转
    if rs_10d < -3.0 and sentiment_norm > 0.8 and rsi >= 35:
        return True, f'相对强度偏弱+高舆情(RS={rs_10d:.1f}%, 舆情={sentiment_norm:.2f}, RSI{rsi:.1f})'

    # 派发模式：高舆情 + RSI偏高 + MA空头（聪明钱借利好出货）
    if sentiment_norm > 0.8 and rsi > 65 and ma_trend == 'bearish':
        return True, f'派发模式(舆情{sentiment_norm:.2f}>0.8, RSI{rsi:.1f}>65, MA空头)'

    # 舆情陷阱：极端舆情 + 极度超卖 + 弱于大盘（价值陷阱，接飞刀）
    if sentiment_norm > 0.85 and rsi < 30 and rs_10d < -3.0:
        return True, f'舆情陷阱(舆情{sentiment_norm:.2f}>0.85, RSI{rsi:.1f}<30, RS{rs_10d:.1f}%<-3%)'

    return False, ''


def compute_composite_score(raw_sentiment_score: float, technical_profile: dict,
                            consensus: float = 1.0, confirmation: dict = None,
                            stock_code: str = None, trade_date: date = None) -> dict:
    """计算复合评分"""
    sentiment_norm = normalize_sentiment(raw_sentiment_score, consensus)
    technical_score = technical_profile.get('technical_score', 50.0)
    rsi = technical_profile.get('rsi', 50.0)

    overheat = compute_overheat_penalty(sentiment_norm, rsi)

    # 多日确认加分
    confirmation_bonus = 0.0
    if confirmation and confirmation.get('confirmed'):
        confirmation_bonus = 10.0

    # 舆情趋势调整
    sentiment_trend_bonus = 0.0
    sentiment_trend_info = {'trend': 'stable', 'momentum': 0.0}
    if stock_code and trade_date:
        sentiment_trend_info = compute_sentiment_trend(stock_code, trade_date)
        if sentiment_trend_info['trend'] == 'accelerating':
            sentiment_trend_bonus = SENTIMENT_ACCEL_BONUS
        elif sentiment_trend_info['trend'] == 'declining':
            sentiment_trend_bonus = -SENTIMENT_DECLINE_PENALTY

    # 连续信号衰减
    consecutive_penalty = 0.0
    if stock_code and trade_date:
        consecutive_penalty = compute_consecutive_penalty(stock_code, trade_date)

    composite = (
        W_SENTIMENT * sentiment_norm * 100 +
        W_TECHNICAL * technical_score -
        overheat +
        confirmation_bonus +
        sentiment_trend_bonus -
        consecutive_penalty
    )

    rejected, reason = should_reject_signal(technical_profile, sentiment_norm)

    # 连续信号过多：同一股票 N 天内 >= 3 个信号 → 硬拒绝
    if not rejected and consecutive_penalty >= CONSECUTIVE_DECAY_PENALTY * 1.5:
        rejected = True
        reason = f'连续信号过多(连续惩罚={consecutive_penalty:.0f})'

    return {
        'composite_score': round(composite, 2),
        'sentiment_normalized': round(sentiment_norm, 4),
        'sentiment_raw': raw_sentiment_score,
        'technical_score': round(technical_score, 2),
        'overheat_penalty': overheat,
        'confirmation_bonus': confirmation_bonus,
        'sentiment_trend_bonus': sentiment_trend_bonus,
        'sentiment_trend': sentiment_trend_info['trend'],
        'consecutive_penalty': consecutive_penalty,
        'rejected': rejected,
        'rejection_reason': reason,
        'components': {
            'rsi': round(rsi, 2),
            'rsi_score': round(technical_profile.get('rsi_score', 50), 2),
            'volume_ratio': round(technical_profile.get('volume_ratio', 1.0), 2),
            'volume_score': round(technical_profile.get('volume_score', 50), 2),
            'ma_trend': technical_profile.get('ma_trend', 'neutral'),
            'ma_score': round(technical_profile.get('ma_score', 50), 2),
            'momentum_5d_pct': round(technical_profile.get('momentum_5d_pct', 0.0), 2),
            'momentum_score': round(technical_profile.get('momentum_score', 50), 2),
            'macd_cross': technical_profile.get('macd_cross', 'none'),
            'macd_score': round(technical_profile.get('macd_score', 40), 2),
            'bb_percent_b': round(technical_profile.get('bb_percent_b', 0.5), 4),
            'bb_score': round(technical_profile.get('bb_score', 50), 2),
            'relative_strength_10d': round(technical_profile.get('relative_strength_10d', 0.0), 4),
            'rs_score': round(technical_profile.get('rs_score', 50), 2),
            'kdj_k': round(technical_profile.get('kdj_k', 50), 2),
            'kdj_cross': technical_profile.get('kdj_cross', 'none'),
            'kdj_score': round(technical_profile.get('kdj_score', 50), 2),
        }
    }
