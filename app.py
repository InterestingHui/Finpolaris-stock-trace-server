import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, date
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts
import akshare as ak
import pandas as pd
import baostock as bs
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import logging
from functools import lru_cache
import random
import time
import threading

# ========== 日志配置 ==========
def setup_logging():
    """配置日志系统"""
    log_level_str = os.environ.get('LOG_LEVEL', 'WARN').upper()
    log_level = getattr(logging, log_level_str, logging.WARNING)

    # 配置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 配置处理器
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # 设置第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pymysql').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

setup_logging()

# 获取应用日志器
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ========== 配置 ==========
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'lianghui')
DB_NAME = os.environ.get('DB_NAME', 'stock_trace')
DB_CONFIG = {
    'host': DB_HOST,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db():
    return pymysql.connect(**DB_CONFIG)

# ========== Tushare 请求限流器 ==========
class TushareRateLimiter:
    """Tushare API 请求限流器 - 防止触发流控"""
    def __init__(self, max_requests_per_minute=100):
        self.max_requests_per_minute = max_requests_per_minute
        self.requests = []
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """如果需要等待，则暂停以避免超出限流"""
        with self.lock:
            now = time.time()
            # 移除超过 1 分钟的记录
            self.requests = [t for t in self.requests if now - t < 60]

            # 如果达到限制，等待
            if len(self.requests) >= self.max_requests_per_minute:
                sleep_time = 60 - (now - self.requests[0])
                if sleep_time > 0:
                    logger.warning(f"Tushare API 请求达到限制，等待 {sleep_time:.1f} 秒...")
                    time.sleep(sleep_time)
                    # 等待后再次清理
                    now = time.time()
                    self.requests = [t for t in self.requests if now - t < 60]

            self.requests.append(now)

# 全局限流器实例
rate_limiter = TushareRateLimiter(max_requests_per_minute=120)

# ========== 交易日历缓存 ==========
_trade_calendar_cache = {}

def load_trade_calendar(start_date: date = None, end_date: date = None):
    """预加载指定区间的交易日历到内存"""
    if start_date is None:
        start_date = date.today() - timedelta(days=365)
    if end_date is None:
        end_date = date.today() + timedelta(days=365)
    rate_limiter.wait_if_needed()
    try:
        df = pro.trade_cal(exchange='SSE',
                           start_date=start_date.strftime('%Y%m%d'),
                           end_date=end_date.strftime('%Y%m%d'))
        for _, row in df.iterrows():
            _trade_calendar_cache[row['cal_date']] = (row['is_open'] == 1)
        logger.info(f"交易日历成功加载 {len(_trade_calendar_cache)} 条记录")
    except Exception as e:
        logger.error(f"交易日历加载失败: {e}")

def is_trading_day(dt: date) -> bool:
    """从缓存判断是否为交易日（若缺失则实时查询并补充缓存）"""
    date_str = dt.strftime('%Y%m%d')
    if date_str in _trade_calendar_cache:
        return _trade_calendar_cache[date_str]
    rate_limiter.wait_if_needed()
    try:
        df = pro.trade_cal(exchange='SSE', start_date=date_str, end_date=date_str)
        if df.empty:
            _trade_calendar_cache[date_str] = False
            return False
        is_open = df.iloc[0]['is_open'] == 1
        _trade_calendar_cache[date_str] = is_open
        return is_open
    except Exception as e:
        logger.error(f"is_trading_day 失败: {e}")
        return False

def get_next_trading_day(dt: date, direction='next') -> date:
    step = 1 if direction == 'next' else -1
    current = dt + timedelta(days=step)
    for _ in range(30):
        if is_trading_day(current):
            return current
        current += timedelta(days=step)
    raise ValueError(f"无法找到 {direction} 交易日，起始日期 {dt}")

def count_trading_days(start_date: date, end_date: date) -> int:
    count = 0
    current = start_date
    while current <= end_date:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count

# ========== 价格获取（使用缓存） ==========
def get_price_from_tushare(stock_code, trade_date, price_type='open', auto_next=False):
    """获取价格，优先使用缓存"""
    if isinstance(trade_date, date):
        target_date = trade_date
        trade_date = trade_date.strftime('%Y%m%d')
    else:
        target_date = datetime.strptime(trade_date, '%Y%m%d').date()

    # 先尝试从缓存读取
    cached = _get_cached_daily(stock_code, target_date)
    if cached:
        price = cached.get(price_type)
        if price is not None:
            return price, target_date

    # 缓存未命中，从 Tushare 获取
    rate_limiter.wait_if_needed()
    try:
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
            if not auto_next:
                return None, None
            next_date = get_next_trading_day(target_date, 'next')
            next_str = next_date.strftime('%Y%m%d')
            rate_limiter.wait_if_needed()
            df = pro.daily(ts_code=stock_code, start_date=next_str, end_date=next_str)
            if df.empty:
                return None, None
            actual_date = next_date
        else:
            actual_date = target_date
        row = df.iloc[0]
        price = float(row[price_type])

        # 保存到缓存
        data = {
            'open': float(row['open']),
            'close': float(row['close']),
            'high': float(row['high']),
            'low': float(row['low'])
        }
        _save_cached_daily(stock_code, actual_date, data)

        return price, actual_date
    except Exception as e:
        logger.error(f"get_price_from_tushare 失败: {e}")
        return None, None


# ========== 日线缓存 ==========
def _get_cached_daily(stock_code: str, trade_date: date):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT open, close, high, low
                FROM stock_daily_cache
                WHERE stock_code = %s AND trade_date = %s
            """, (stock_code, trade_date))
            row = cursor.fetchone()
            if row:
                return {
                    'open': float(row['open']),
                    'close': float(row['close']),
                    'high': float(row['high']),
                    'low': float(row['low'])
                }
    except Exception as e:
        logger.debug(f"读取日线缓存失败: {e}")
    finally:
        conn.close()
    return None

def _save_cached_daily(stock_code: str, trade_date: date, data: dict):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_daily_cache
                (stock_code, trade_date, open, close, high, low)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), close=VALUES(close), high=VALUES(high), low=VALUES(low)
            """, (
                stock_code, trade_date,
                data['open'], data['close'], data['high'], data['low']
            ))
        conn.commit()
    except Exception as e:
        logger.debug(f"保存日线缓存失败: {e}")
    finally:
        conn.close()

def fetch_stock_daily_info(stock_code: str, trade_date: date):
    cached = _get_cached_daily(stock_code, trade_date)
    if cached:
        return cached
    date_str = trade_date.strftime('%Y%m%d')
    rate_limiter.wait_if_needed()
    try:
        df = pro.daily(ts_code=stock_code, start_date=date_str, end_date=date_str)
        if df.empty:
            return None
        row = df.iloc[0]
        data = {
            'open': float(row['open']),
            'close': float(row['close']),
            'high': float(row['high']),
            'low': float(row['low'])
        }
        _save_cached_daily(stock_code, trade_date, data)
        return data
    except Exception as e:
        logger.debug(f"获取日线数据失败 {stock_code} {date_str}: {e}")
        return None

# ========== 批量获取价格数据 ==========
def batch_fetch_daily_prices(stock_codes: list, start_date: date, end_date: date, price_type='close') -> dict:
    """
    批量获取多只股票在指定日期范围内的价格数据
    返回: {(stock_code, trade_date): price}
    """
    price_cache = {}

    # 先从数据库缓存中批量读取
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(stock_codes))
            cursor.execute(f"""
                SELECT stock_code, trade_date, open, close, high, low
                FROM stock_daily_cache
                WHERE stock_code IN ({placeholders}) AND trade_date BETWEEN %s AND %s
            """, stock_codes + [start_date, end_date])

            for row in cursor.fetchall():
                stock_code = row['stock_code']
                trade_date = row['trade_date']
                # 获取日期部分
                if isinstance(trade_date, datetime):
                    trade_date = trade_date.date()
                price_cache[(stock_code, trade_date, 'open')] = float(row['open'])
                price_cache[(stock_code, trade_date, 'close')] = float(row['close'])
                price_cache[(stock_code, trade_date, 'high')] = float(row['high'])
                price_cache[(stock_code, trade_date, 'low')] = float(row['low'])
    except Exception as e:
        logger.debug(f"批量读取缓存失败: {e}")
    finally:
        conn.close()

    # 找出需要从 Tushare 获取的股票代码
    stocks_to_fetch = []
    for stock_code in stock_codes:
        has_all_cached = True
        current = start_date
        while current <= end_date:
            if (stock_code, current, price_type) not in price_cache:
                has_all_cached = False
                break
            current += timedelta(days=1)
        if not has_all_cached:
            stocks_to_fetch.append(stock_code)

    # 批量从 Tushare 获取缺失的数据
    if stocks_to_fetch:
        logger.info(f"需要从 Tushare 获取 {len(stocks_to_fetch)} 只股票的数据")
        for stock_code in stocks_to_fetch:
            rate_limiter.wait_if_needed()
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            try:
                df = pro.daily(ts_code=stock_code, start_date=start_str, end_date=end_str)
                if not df.empty:
                    # 保存到数据库缓存
                    for _, row in df.iterrows():
                        trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                        data = {
                            'open': float(row['open']),
                            'close': float(row['close']),
                            'high': float(row['high']),
                            'low': float(row['low'])
                        }
                        _save_cached_daily(stock_code, trade_date, data)

                        # 更新内存缓存
                        price_cache[(stock_code, trade_date, 'open')] = data['open']
                        price_cache[(stock_code, trade_date, 'close')] = data['close']
                        price_cache[(stock_code, trade_date, 'high')] = data['high']
                        price_cache[(stock_code, trade_date, 'low')] = data['low']
            except Exception as e:
                logger.debug(f"批量获取 {stock_code} 失败: {e}")

    return price_cache

# ========== 指数数据缓存 ==========
_index_cache = {}  # 缓存指数数据，格式: {(index_code, start_date, end_date, price_type): data}

def get_index_data_cached(index_code: str, start_date: date, end_date: date, price_type: str):
    """从缓存或 API 获取指数数据"""
    cache_key = (index_code, start_date, end_date, price_type)

    # 检查缓存
    if cache_key in _index_cache:
        return _index_cache[cache_key]

    # 从 Tushare 获取
    index_map = {
        'sh000300': '000300.SH',
        'sh000852': '000852.SH'
    }
    ts_code = index_map.get(index_code.lower())
    if not ts_code:
        return None

    rate_limiter.wait_if_needed()
    try:
        df = pro.index_daily(
            ts_code=ts_code,
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d')
        )

        if df.empty:
            return None

        df = df.sort_values('trade_date')
        price_map = {}
        for _, row in df.iterrows():
            date_obj = datetime.strptime(row['trade_date'], '%Y%m%d').date()
            price_map[date_obj] = float(row[price_type])

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        base_price = None
        last_price = None
        result = []
        for d in dates:
            if d in price_map:
                price = price_map[d]
                last_price = price
            else:
                price = last_price
            if price is None:
                continue
            if base_price is None:
                base_price = price
                relative = 100.0
            else:
                relative = (price / base_price) * 100
            result.append({
                'date': d.strftime('%Y-%m-%d'),
                'value': round(price, 2),
                'percent_change': round(relative, 2)
            })

        # 缓存结果
        _index_cache[cache_key] = result
        return result

    except Exception as e:
        logger.error(f"获取指数数据失败 {index_code}: {e}")
        return None

# ========== 股票名称缓存（基于 Tushare） ==========
def _get_cached_name(stock_code: str):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT stock_name FROM stock_name_cache WHERE stock_code = %s", (stock_code,))
            row = cursor.fetchone()
            return row['stock_name'] if row else None
    except Exception as e:
        logger.debug(f"读取名称缓存失败: {e}")
        return None
    finally:
        conn.close()

def _save_cached_name(stock_code: str, stock_name: str):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_name_cache (stock_code, stock_name) VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE stock_name = VALUES(stock_name)
            """, (stock_code, stock_name))
        conn.commit()
    except Exception as e:
        logger.debug(f"保存名称缓存失败: {e}")
    finally:
        conn.close()

def init_stock_basic_cache():
    """启动时从 Tushare 拉取全量股票基本信息，写入数据库缓存"""
    logger.info("开始从 Tushare 初始化股票基本信息...")
    rate_limiter.wait_if_needed()
    try:
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        if df.empty:
            logger.warning("Tushare 未返回股票名称数据")
            return

        conn = get_db()
        try:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO stock_name_cache (stock_code, stock_name)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE stock_name = VALUES(stock_name)
                    """, (row['ts_code'], row['name']))
            conn.commit()
            logger.info(f"成功缓存 {len(df)} 只股票名称")
        except Exception as e:
            conn.rollback()
            logger.error(f"写入股票名称数据库失败: {e}")
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"从 Tushare 获取股票名称失败: {e}")

@lru_cache(maxsize=5000)
def get_stock_name(stock_code: str) -> str:
    """从数据库缓存获取股票名称（启动时已预加载）"""
    cached = _get_cached_name(stock_code)
    return cached if cached else None

# ========== 股票市值缓存 ==========
def get_stock_market_value(stock_code: str, trade_date: date):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT total_mv FROM stock_market_value_cache WHERE stock_code = %s AND trade_date = %s",
                (stock_code, trade_date)
            )
            row = cursor.fetchone()
            if row and row['total_mv'] is not None:
                return float(row['total_mv'])
    except Exception as e:
        logger.debug(f"读取市值缓存失败: {e}")
    finally:
        conn.close()

    date_str = trade_date.strftime('%Y%m%d')
    rate_limiter.wait_if_needed()
    try:
        df = pro.daily_basic(ts_code=stock_code, trade_date=date_str,
                             fields='ts_code,trade_date,total_mv,circ_mv')
        if not df.empty:
            total_mv = float(df.iloc[0]['total_mv']) * 10000
            circ_mv = float(df.iloc[0]['circ_mv']) * 10000
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO stock_market_value_cache (stock_code, trade_date, total_mv, circ_mv)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE total_mv = VALUES(total_mv), circ_mv = VALUES(circ_mv)
                    """, (stock_code, trade_date, total_mv, circ_mv))
                conn.commit()
            finally:
                conn.close()
            return total_mv
    except Exception as e:
        logger.debug(f"获取市值失败 {stock_code} {trade_date}: {e}")
    return None

# ========== VWAP 缓存 ==========
def _get_cached_vwap(stock_code: str, trade_date: date):
    """从数据库缓存获取 VWAP"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT vwap
                FROM stock_vwap_cache
                WHERE stock_code = %s AND trade_date = %s
            """, (stock_code, trade_date))
            row = cursor.fetchone()
            return float(row['vwap']) if row else None
    except Exception as e:
        logger.debug(f"读取VWAP缓存失败: {e}")
        return None
    finally:
        conn.close()

def _save_cached_vwap(stock_code: str, trade_date: date, vwap: float):
    """保存 VWAP 到数据库缓存"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_vwap_cache (stock_code, trade_date, vwap)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE vwap = VALUES(vwap)
            """, (stock_code, trade_date, vwap))
        conn.commit()
    except Exception as e:
        logger.debug(f"保存VWAP缓存失败: {e}")
    finally:
        conn.close()

def get_vwap(stock_code: str, trade_date: date) -> float:
    """获取 VWAP（优先从缓存读取）"""
    # 先尝试从缓存读取
    cached = _get_cached_vwap(stock_code, trade_date)
    if cached is not None:
        return cached

    # 缓存未命中，从 Tushare 获取
    rate_limiter.wait_if_needed()
    date_str = trade_date.strftime('%Y%m%d')
    try:
        df = pro.daily(ts_code=stock_code, start_date=date_str, end_date=date_str)
        if df.empty:
            return None
        row = df.iloc[0]
        amount = float(row['amount'])
        vol = float(row['vol'])
        if vol == 0:
            return None
        vwap = (amount * 1000) / (vol * 100)
        vwap = round(vwap, 3)

        # 保存到缓存
        _save_cached_vwap(stock_code, trade_date, vwap)
        return vwap
    except Exception as e:
        logger.debug(f"从Tushare获取VWAP失败 {stock_code} {date_str}: {e}")
        return None

# ========== 限价缓存 ==========
def _get_cached_limit_price(stock_code: str, trade_date: date):
    """从数据库缓存获取限价"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT up_limit, down_limit
                FROM stock_limit_cache
                WHERE stock_code = %s AND trade_date = %s
            """, (stock_code, trade_date))
            row = cursor.fetchone()
            if row:
                return float(row['up_limit']), float(row['down_limit'])
            return None, None
    except Exception as e:
        logger.debug(f"读取限价缓存失败: {e}")
        return None, None
    finally:
        conn.close()

def _save_cached_limit_price(stock_code: str, trade_date: date, up_limit: float, down_limit: float):
    """保存限价到数据库缓存"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_limit_cache (stock_code, trade_date, up_limit, down_limit)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    up_limit = VALUES(up_limit),
                    down_limit = VALUES(down_limit)
            """, (stock_code, trade_date, up_limit, down_limit))
        conn.commit()
    except Exception as e:
        logger.debug(f"保存限价缓存失败: {e}")
    finally:
        conn.close()

def get_limit_price(stock_code, trade_date):
    """获取限价（优先从缓存读取）"""
    # 先尝试从缓存读取
    up_limit, down_limit = _get_cached_limit_price(stock_code, trade_date)
    if up_limit is not None and down_limit is not None:
        return up_limit, down_limit

    # 缓存未命中，从 Tushare 获取
    rate_limiter.wait_if_needed()
    try:
        if isinstance(trade_date, date):
            trade_date = trade_date.strftime('%Y%m%d')
        df = pro.stk_limit(ts_code=stock_code, trade_date=trade_date)
        if not df.empty:
            up = float(df.iloc[0]['up_limit'])
            down = float(df.iloc[0]['down_limit'])
            # 保存到缓存
            _save_cached_limit_price(stock_code, datetime.strptime(trade_date, '%Y%m%d').date(), up, down)
            return up, down
        return None, None
    except Exception as e:
        logger.debug(f"获取限价失败: {e}")
        return None, None

# ========== 最新价格缓存 ==========
def get_latest_price(stock_code, price_type='close'):
    """获取最新价格（优先从缓存读取）"""
    # 先尝试从数据库缓存读取最新日期的价格
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date, close
                FROM stock_daily_cache
                WHERE stock_code = %s
                ORDER BY trade_date DESC
                LIMIT 1
            """, (stock_code,))
            row = cursor.fetchone()
            if row:
                # 使用缓存的最新日期的价格（假设价格变化不大）
                # 对于实时性要求不高的场景，缓存1小时内的数据是可以接受的
                latest_date = row['trade_date']
                now = datetime.now()
                # 如果缓存的数据是今天且在6小时内的，直接返回
                if isinstance(latest_date, datetime):
                    latest_date = latest_date.date()
                if (now.date() - latest_date).days <= 1:
                    if price_type == 'close':
                        return float(row['close'])
                    # 如果需要其他价格类型，尝试获取
                    rate_limiter.wait_if_needed()
                    df = pro.daily(ts_code=stock_code, limit=1)
                    if not df.empty:
                        return float(df.iloc[0][price_type])
    except Exception as e:
        logger.debug(f"读取最新价格缓存失败: {e}")
    finally:
        conn.close()

    # 缓存未命中或过期，从 Tushare 获取
    rate_limiter.wait_if_needed()
    try:
        df = pro.daily(ts_code=stock_code, limit=1)
        if not df.empty:
            return float(df.iloc[0][price_type])
    except Exception as e:
        logger.debug(f"获取最新价格失败: {e}")
    return None

# ========== 分钟级价格缓存 ==========
stock_min_cache = {}

def is_minute_data_available(target_datetime: datetime) -> bool:
    """检查分钟数据是否可用（BaoStock 支持 5 年）"""
    days_diff = (datetime.now().date() - target_datetime.date()).days
    return days_diff <= 1825  # 5 年

def get_minute_price(stock_code: str, target_datetime: datetime) -> dict:
    """获取指定分钟的股票价格（使用 BaoStock）"""
    cache_key = (stock_code, target_datetime)
    if cache_key in stock_min_cache:
        return stock_min_cache[cache_key]

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT open_price, close_price, high_price, low_price, volume, amount
                   FROM stock_min_cache WHERE stock_code = %s AND trade_datetime = %s""",
                (stock_code, target_datetime)
            )
            result = cursor.fetchone()
            if result:
                price_data = {
                    'open': float(result['open_price']), 'close': float(result['close_price']),
                    'high': float(result['high_price']), 'low': float(result['low_price']),
                    'volume': result['volume'], 'amount': result['amount']
                }
                stock_min_cache[cache_key] = price_data
                return price_data
    except Exception as e:
        logger.debug(f"查询分钟价格缓存失败: {e}")
    finally:
        conn.close()

    if not is_minute_data_available(target_datetime):
        return None

    try:
        code_only = stock_code.split('.')[0]
        bs_code = f'sz.{code_only}' if 'SZ' in stock_code else f'sh.{code_only}'
        date_str = target_datetime.strftime('%Y-%m-%d')

        lg = bs.login()
        if lg.error_code != '0':
            return None

        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,time,open,high,low,close,volume,amount",
            start_date=date_str,
            end_date=date_str,
            frequency="5",  # 5分钟线（BaoStock 1分钟线受限）
            adjustflag="1"
        )

        if rs.error_code != '0' or len(rs.data) == 0:
            bs.logout()
            return None

        df = pd.DataFrame(rs.data, columns=rs.fields)

        # 修复BaoStock时间格式解析：YYYYMMDDHHmmss
        def parse_baostock_datetime(date_str, time_str):
            if len(time_str) >= 14:
                year = time_str[0:4]
                month = time_str[4:6]
                day = time_str[6:8]
                hour = time_str[8:10]
                minute = time_str[10:12]
                second = time_str[12:14] if len(time_str) >= 14 else '00'
                return f"{year}-{month}-{day} {hour}:{minute}:{second}"
            return f"{date_str} {time_str}"

        df['datetime'] = df.apply(lambda row: parse_baostock_datetime(row['date'], row['time']), axis=1)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')

        # 找精确匹配的目标分钟
        target_minute = target_datetime.replace(second=0, microsecond=0)
        exact_match = df[df['datetime'] == target_minute]

        if len(exact_match) > 0:
            row = exact_match.iloc[0]
        else:
            # 如果没有精确匹配，找最接近的（5分钟线容错范围扩大）
            df['diff'] = abs(df['datetime'] - target_minute)
            row = df.loc[df['diff'].idxmin()]
            if row['diff'].total_seconds() > 150:  # 5分钟线容错范围：2.5分钟
                bs.logout()
                return None

        # BaoStock价格字段不是实际价格，需要用成交额/成交量重新计算
        # 价格单位：元/股
        volume = int(row['volume'])
        amount = float(row['amount'])
        actual_close = amount / volume if volume > 0 else 0

        # 使用成交额/成交量计算实际价格，其他价格用比例估算
        price_ratio = actual_close / float(row['close'])

        price_data = {
            'open': float(row['open']) * price_ratio,
            'close': actual_close,
            'high': float(row['high']) * price_ratio,
            'low': float(row['low']) * price_ratio,
            'volume': volume,
            'amount': amount
        }

        bs.logout()
        stock_min_cache[cache_key] = price_data

        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO stock_min_cache
                       (stock_code, trade_datetime, open_price, close_price,
                        high_price, low_price, volume, amount)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                       open_price = VALUES(open_price), close_price = VALUES(close_price),
                       high_price = VALUES(high_price), low_price = VALUES(low_price),
                       volume = VALUES(volume), amount = VALUES(amount)""",
                    (stock_code, target_datetime, price_data['open'], price_data['close'],
                     price_data['high'], price_data['low'], price_data['volume'], price_data['amount'])
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
        finally:
            conn.close()

        return price_data

    except Exception as e:
        try:
            bs.logout()
        except:
            pass
        return None

# ========== 策略计算 ==========
def calculate_strategy_cash_and_positions(strategy_id):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return None, None
            initial_capital = float(row['initial_capital'])
            cursor.execute("SELECT action, amount FROM trades WHERE strategy_id = %s", (strategy_id,))
            trades = cursor.fetchall()
            cash = initial_capital
            for t in trades:
                amount = float(t['amount'])
                if t['action'] == 'buy':
                    cash -= amount
                else:
                    cash += amount
            cursor.execute("""
                SELECT stock_code, 
                       SUM(CASE WHEN action='buy' THEN quantity ELSE -quantity END) as net_qty
                FROM trades
                WHERE strategy_id = %s
                GROUP BY stock_code
                HAVING net_qty != 0
            """, (strategy_id,))
            positions = {row['stock_code']: row['net_qty'] for row in cursor.fetchall()}
        return cash, positions
    except Exception as e:
        logger.error(f"计算策略现金和持仓失败: {e}")
        return None, None
    finally:
        conn.close()

def get_current_nav(strategy_id, stock_list=None, price_type='close'):
    cash, positions = calculate_strategy_cash_and_positions(strategy_id)
    if cash is None:
        return None
    if stock_list is None:
        total_mv = 0.0
        for stock_code, qty in positions.items():
            qty = float(qty)
            price = get_latest_price(stock_code, price_type)
            if price:
                total_mv += qty * price
        return cash + total_mv
    else:
        result = []
        for stock_code in stock_list:
            qty = positions.get(stock_code, 0)
            qty = float(qty)
            if qty == 0:
                result.append({'stock_code': stock_code, 'nav': 0.0})
            else:
                price = get_latest_price(stock_code, price_type)
                nav = qty * price if price else 0.0
                result.append({'stock_code': stock_code, 'nav': nav})
        return result

def get_strategy_nav_at_date(strategy_id: str, target_date: date, price_type='close') -> float:
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return None
            initial_capital = float(row['initial_capital'])
            cursor.execute("""
                SELECT trade_date, stock_code, action, quantity, price
                FROM trades
                WHERE strategy_id = %s AND trade_date <= %s
                ORDER BY trade_date, id
            """, (strategy_id, target_date))
            trades = cursor.fetchall()
    finally:
        conn.close()
    cash = initial_capital
    positions = defaultdict(float)
    for t in trades:
        qty = float(t['quantity'])
        price = float(t['price'])
        if t['action'] == 'buy':
            cash -= qty * price
            positions[t['stock_code']] += qty
        else:
            cash += qty * price
            positions[t['stock_code']] -= qty
            if positions[t['stock_code']] == 0:
                del positions[t['stock_code']]
    market_value = 0.0
    for stock_code, qty in positions.items():
        price, _ = get_price_from_tushare(stock_code, target_date, price_type, auto_next=True)
        if price:
            market_value += qty * price
    return cash + market_value

# ========== 交易执行 ==========
def execute_trade(log_id, strategy_id, stock_code, trade_id, action, quantity,
                  target_datetime, order_type='daily_order', limit_price=None, cutoff_time=None):
    """
    执行交易订单

    Args:
        log_id: 交易日志 ID
        strategy_id: 策略 ID
        stock_code: 股票代码
        trade_id: 交易 ID
        action: 'buy' 或 'sell'
        quantity: 交易数量
        target_datetime: 目标执行时间（精确到分钟）
        order_type: 订单类型（daily_order/limit_order/market_order）
        limit_price: 限价单价格
        cutoff_time: 限价单截止时间
    """
    logger.debug(f"execute_trade 开始: trade_id={trade_id}, action={action}, order_type={order_type}, target_datetime={target_datetime}")
    target_date = target_datetime.date() if isinstance(target_datetime, datetime) else target_datetime

    # 根据订单类型获取成交价
    actual_price = None
    vwap_price = None
    price_type = 'vwap'

    if order_type == 'daily_order':
        # 日度单：使用 VWAP
        vwap_price = get_vwap(stock_code, target_date)
        actual_price = vwap_price
        price_type = 'vwap'

    elif order_type == 'market_order':
        minute_data = get_minute_price(stock_code, target_datetime)
        if minute_data is None:
            vwap_price = get_vwap(stock_code, target_date)
            actual_price = vwap_price
            price_type = 'vwap'
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE trade_logs SET fail_reason = %s WHERE id = %s",
                                 ("分钟数据不可用，已回退到 VWAP", log_id))
                conn.commit()
            finally:
                conn.close()
        else:
            actual_price = minute_data['close']
            vwap_price = get_vwap(stock_code, target_date)
            price_type = 'actual'

    elif order_type == 'limit_order':
        # 限价单：检查价格是否满足条件
        if limit_price is None:
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                        ("限价单缺少价格参数", log_id)
                    )
                conn.commit()
            finally:
                conn.close()
            return False

        minute_data = get_minute_price(stock_code, target_datetime)
        if minute_data is None:
            # 分钟数据不可用，无法执行限价单
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                        ("分钟数据不可用，限价单无法执行", log_id)
                    )
                conn.commit()
            finally:
                conn.close()
            return False

        # 检查价格是否满足限价条件
        if action == 'buy':
            # 买入：价格必须 ≤ 限价
            if minute_data['close'] <= limit_price:
                actual_price = minute_data['close']  # 以实际价格成交
                price_type = 'actual'
            else:
                conn = get_db()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                            (f"价格 {minute_data['close']} 超过限价 {limit_price}", log_id)
                        )
                        conn.commit()
                finally:
                    conn.close()
                return False
        else:  # sell
            # 卖出：价格必须 ≥ 限价
            if minute_data['close'] >= limit_price:
                actual_price = minute_data['close']  # 以实际价格成交
                price_type = 'actual'
            else:
                conn = get_db()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                            (f"价格 {minute_data['close']} 低于限价 {limit_price}", log_id)
                        )
                        conn.commit()
                finally:
                    conn.close()
                return False

        vwap_price = get_vwap(stock_code, target_date)
    else:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                    (f"不支持的订单类型: {order_type}", log_id)
                )
            conn.commit()
        finally:
            conn.close()
        return False

    if actual_price is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_logs SET status = 'failed', fail_reason = %s WHERE id = %s",
                    ("无法获取成交价", log_id)
                )
            conn.commit()
        finally:
            conn.close()
        return False

    # 获取开盘价用于涨跌停检查
    open_price, _ = get_price_from_tushare(stock_code, target_date, 'open', auto_next=False)
    if open_price is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_logs SET status='failed', fail_reason='无法获取开盘价', actual_date=%s WHERE id=%s",
                    (target_datetime, log_id)
                )
            conn.commit()
        finally:
            conn.close()
        return False

    # 涨跌停检查
    up_limit, down_limit = get_limit_price(stock_code, target_date)
    if action == 'buy' and up_limit is not None and open_price >= up_limit - 0.001:
        fail_reason = '涨停无法买入'
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s",
                    (fail_reason, target_datetime, actual_price, log_id)
                )
            conn.commit()
        finally:
            conn.close()
        return False
    if action == 'sell' and down_limit is not None and open_price <= down_limit + 0.001:
        fail_reason = '跌停无法卖出'
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s",
                    (fail_reason, target_datetime, actual_price, log_id)
                )
            conn.commit()
        finally:
            conn.close()
        return False

    # 资金/持仓检查
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id=%s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return False
            initial_capital = float(row['initial_capital'])
            cursor.execute("SELECT action, amount FROM trades WHERE strategy_id = %s", (strategy_id,))
            trades = cursor.fetchall()
            cash = initial_capital
            for t in trades:
                amount = float(t['amount'])
                if t['action'] == 'buy':
                    cash -= amount
                else:
                    cash += amount
            cursor.execute("""
                SELECT stock_code, SUM(CASE WHEN action='buy' THEN quantity ELSE -quantity END) as net_qty
                FROM trades WHERE strategy_id = %s GROUP BY stock_code HAVING net_qty != 0
            """, (strategy_id,))
            positions = {row['stock_code']: row['net_qty'] for row in cursor.fetchall()}
    finally:
        conn.close()

    if action == 'buy':
        cost = quantity * actual_price
        if cash < cost:
            fail_reason = '资金不足'
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s",
                        (fail_reason, target_datetime, actual_price, log_id)
                    )
                conn.commit()
            finally:
                conn.close()
            return False
    else:
        current_qty = positions.get(stock_code, 0)
        if current_qty < quantity:
            fail_reason = '持仓不足'
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s",
                        (fail_reason, target_datetime, actual_price, log_id)
                    )
                conn.commit()
            finally:
                conn.close()
            return False

    # 获取日线信息
    daily_info = fetch_stock_daily_info(stock_code, target_date)
    stock_name = get_stock_name(stock_code)

    update_fields = {
        'status': 'success',
        'actual_date': target_datetime,
        'price': actual_price,
        'vwap': vwap_price,
        'actual_price': actual_price,
        'order_type': order_type,
        'stock_name': stock_name,
        'fail_reason': None,  # 清除之前的错误信息
    }
    if daily_info:
        update_fields.update({
            'open_price': daily_info['open'],
            'close_price': daily_info['close'],
            'high_price': daily_info['high'],
            'low_price': daily_info['low']
        })

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO trades (strategy_id, stock_code, trade_id, trade_date, action, quantity, price, price_type, order_type, limit_price, cutoff_time, actual_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (strategy_id, stock_code, trade_id, target_datetime, action, quantity,
                  actual_price, price_type, order_type, limit_price, cutoff_time, actual_price))
            set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
            values = list(update_fields.values()) + [log_id]
            cursor.execute(f"UPDATE trade_logs SET {set_clause} WHERE id=%s", values)
        conn.commit()
        logger.info(f"交易成功: {strategy_id} trade_id={trade_id} {action} {quantity}@{actual_price} ({order_type})")
    except pymysql.err.IntegrityError as e:
        conn.rollback()
        logger.warning(f"唯一约束冲突，交易已存在，忽略本次重复执行: {e}")
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE trade_logs SET status='success', actual_date=%s, fail_reason=NULL WHERE id=%s",
                (target_datetime, log_id)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"交易执行失败: {e}")
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE trade_logs SET status='failed', fail_reason=%s WHERE id=%s",
                (str(e)[:255], log_id)
            )
        conn.commit()
    finally:
        conn.close()

    return True

def process_pending_orders():
    """
    处理待成交订单（非限价单：日度单、市价单）
    """
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            today = datetime.now()
            cursor.execute("""
                SELECT id, strategy_id, stock_code, trade_id, action, quantity,
                       target_date, order_type
                FROM trade_logs
                WHERE status='pending' AND target_date <= %s
                  AND order_type IN ('daily_order', 'market_order')
            """, (today,))
            pending_orders = cursor.fetchall()
    finally:
        conn.close()

    for order in pending_orders:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM trades WHERE strategy_id = %s AND trade_id = %s",
                               (order['strategy_id'], order['trade_id']))
                if cursor.fetchone():
                    with conn.cursor() as cursor2:
                        cursor2.execute("UPDATE trade_logs SET status='success', actual_date=%s WHERE id=%s",
                                        (order['target_date'], order['id']))
                    conn.commit()
                    logger.info(f"交易已存在，日志标记为 success: {order['strategy_id']} trade_id={order['trade_id']}")
                    continue
        finally:
            conn.close()
        execute_trade(order['id'], order['strategy_id'], order['stock_code'],
                      order['trade_id'], order['action'], order['quantity'],
                      order['target_date'], order['order_type'])

def process_limit_orders():
    """
    处理限价单：模拟持续监控价格
    对于回测系统，在目标时间和截止时间之间查找第一个满足条件的时间点
    """
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 查询待处理的限价单，按时间顺序处理
            cursor.execute(
                """SELECT id, strategy_id, stock_code, trade_id, action, quantity,
                          target_date, limit_price, cutoff_time
                   FROM trade_logs
                   WHERE status = 'pending'
                     AND order_type = 'limit_order'
                     AND target_date <= NOW()
                   ORDER BY target_date ASC"""
            )

            pending_orders = cursor.fetchall()
    finally:
        conn.close()

    logger.debug(f"process_limit_orders 查询到 {len(pending_orders)} 个待处理限价单")

    for order in pending_orders:
        log_id = order['id']
        strategy_id = order['strategy_id']
        stock_code = order['stock_code']
        trade_id = order['trade_id']
        action = order['action']
        quantity = order['quantity']
        target_datetime = order['target_date']
        limit_price = order['limit_price']
        cutoff_time = order['cutoff_time']

        logger.debug(f"处理限价单: trade_id={trade_id}, target_datetime={target_datetime}, limit_price={limit_price}, cutoff_time={cutoff_time}")

        # 获取该日期的5分钟K线数据
        date_str = target_datetime.date().strftime('%Y-%m-%d')
        day_minute_data = get_day_minute_data(stock_code, date_str)

        if not day_minute_data:
            logger.debug(f"无法获取 {date_str} 的分钟数据")
            continue

        # 在目标时间到截止时间之间查找满足条件的时间点
        filled = False
        fill_time = None
        fill_price = None

        for minute_time, price_data in day_minute_data:
            if minute_time < target_datetime:
                continue  # 跳过目标时间之前的时间点

            if cutoff_time and minute_time > cutoff_time:
                break  # 超过截止时间

            actual_price = price_data['close']
            price_satisfied = False

            if action == 'buy':
                # 买入：价格必须 ≤ 限价
                if actual_price <= limit_price:
                    price_satisfied = True
            else:  # sell
                # 卖出：价格必须 ≥ 限价
                if actual_price >= limit_price:
                    price_satisfied = True

            if price_satisfied:
                filled = True
                fill_time = minute_time
                fill_price = actual_price
                logger.debug(f"找到满足条件的时间点: {fill_time}, 价格: {fill_price:.3f}")
                break

        if filled:
            # 在找到的时间点执行交易
            execute_limit_order_fill(
                log_id, strategy_id, stock_code, trade_id, action, quantity,
                fill_time, fill_price, limit_price
            )
        else:
            # 没有找到满足条件的时间点
            logger.debug(f"限价单未找到满足条件的时间点，标记为失败")
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    # 获取目标时间的价格用于失败原因
                    target_price = None
                    for minute_time, price_data in day_minute_data:
                        if minute_time == target_datetime:
                            target_price = price_data['close']
                            break

                    fail_reason = f"未找到满足条件的成交时间。目标时间价格: {target_price:.3f}"
                    cursor.execute(
                        "UPDATE trade_logs SET status = 'failed', fail_reason = %s, actual_date = %s WHERE id = %s",
                        (fail_reason, target_datetime, log_id)
                    )
                    conn.commit()
            finally:
                conn.close()

def get_day_minute_data(stock_code: str, date_str: str) -> list:
    """获取一天的5分钟K线数据，返回 [(datetime, price_data), ...]"""
    import baostock as bs
    import pandas as pd

    code_only = stock_code.split('.')[0]
    bs_code = f'sz.{code_only}' if 'SZ' in stock_code else f'sh.{code_only}'

    lg = bs.login()
    if lg.error_code != '0':
        logger.debug(f"BaoStock登录失败: {lg.error_msg}")
        bs.logout()
        return None

    rs = bs.query_history_k_data_plus(
        code=bs_code,
        fields="date,time,open,high,low,close,volume,amount",
        start_date=date_str,
        end_date=date_str,
        frequency="5",
        adjustflag="1"
    )

    if rs.error_code != '0' or len(rs.data) == 0:
        bs.logout()
        return None

    df = pd.DataFrame(rs.data, columns=rs.fields)

    # 解析时间
    def parse_baostock_datetime(date_str, time_str):
        if len(time_str) >= 14:
            year = time_str[0:4]
            month = time_str[4:6]
            day = time_str[6:8]
            hour = time_str[8:10]
            minute = time_str[10:12]
            second = time_str[12:14] if len(time_str) >= 14 else '00'
            return f"{year}-{month}-{day} {hour}:{minute}:{second}"
        return f"{date_str} {time_str}"

    df['datetime'] = df.apply(lambda row: parse_baostock_datetime(row['date'], row['time']), axis=1)
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')

    # 处理价格数据
    result = []
    for _, row in df.iterrows():
        volume = int(row['volume'])
        amount = float(row['amount'])
        actual_close = amount / volume if volume > 0 else 0
        price_ratio = actual_close / float(row['close'])

        price_data = {
            'open': float(row['open']) * price_ratio,
            'close': actual_close,
            'high': float(row['high']) * price_ratio,
            'low': float(row['low']) * price_ratio,
            'volume': volume,
            'amount': amount
        }
        result.append((row['datetime'], price_data))

    bs.logout()
    return result

def execute_limit_order_fill(log_id, strategy_id, stock_code, trade_id, action, quantity,
                             fill_time, fill_price, limit_price):
    """执行限价单成交"""
    try:
        # 获取VWAP
        vwap_price = get_vwap(stock_code, fill_time.date())

        # 获取日线信息
        daily_info = fetch_stock_daily_info(stock_code, fill_time.date())
        stock_name = get_stock_name(stock_code)

        update_fields = {
            'status': 'success',
            'actual_date': fill_time,
            'price': fill_price,
            'vwap': vwap_price,
            'actual_price': fill_price,
            'order_type': 'limit_order',
            'stock_name': stock_name,
            'fail_reason': None,
        }
        if daily_info:
            update_fields.update({
                'open_price': daily_info['open'],
                'close_price': daily_info['close'],
                'high_price': daily_info['high'],
                'low_price': daily_info['low']
            })

        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO trades (strategy_id, stock_code, trade_id, trade_date, action, quantity, price, price_type, order_type, limit_price, cutoff_time, actual_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (strategy_id, stock_code, trade_id, fill_time, action, quantity,
                          fill_price, 'actual', 'limit_order', limit_price, None, fill_price))
                set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
                values = list(update_fields.values()) + [log_id]
                cursor.execute(f"UPDATE trade_logs SET {set_clause} WHERE id=%s", values)
            conn.commit()
            logger.info(f"限价单成交: {strategy_id} trade_id={trade_id} {action} {quantity}@{fill_price:.3f} @ {fill_time}")
        except pymysql.err.IntegrityError as e:
            conn.rollback()
            logger.warning(f"限价单已存在: {e}")
        except Exception as e:
            conn.rollback()
            logger.error(f"限价单执行失败: {e}")
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"execute_limit_order_fill 失败: {e}")

# ========== 路由 ==========
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/strategies', methods=['POST'])
def add_strategies():
    logger.debug("POST /api/strategies")
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({'error': '请求体应为数组'}), 400

    conn = get_db()
    try:
        for strategy in data:
            strategy_id = strategy.get('strategy_id')
            if not strategy_id:
                return jsonify({'error': '缺少 strategy_id'}), 400

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
                exists = cursor.fetchone()

            if exists and 'initial_capital' in strategy:
                return jsonify({'error': f'策略 {strategy_id} 已存在，不能修改初始资金'}), 400

            if not exists:
                initial_capital = strategy.get('initial_capital', 1000000.0)
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO strategies (strategy_id, initial_capital) VALUES (%s, %s)",
                                   (strategy_id, initial_capital))

            stocks = strategy.get('stocks', [])

            # 收集当前策略中已有的 trade_id，用于去重
            used_trade_ids = set()
            with conn.cursor() as cursor:
                cursor.execute("SELECT trade_id FROM trade_logs WHERE strategy_id = %s", (strategy_id,))
                for row in cursor.fetchall():
                    used_trade_ids.add(row['trade_id'])

            for item in stocks:
                stock_code = item.get('stock_code')
                trade_id = item.get('trade_id')
                action = item.get('action')
                quantity = item.get('quantity')
                date_input = item.get('date')
                order_type = item.get('order_type', 'daily_order')
                limit_price = item.get('limit_price')
                cutoff_time_input = item.get('cutoff_time')

                # 如果未提供 trade_id，自动生成一个随机且不重复的 ID
                if not trade_id:
                    while True:
                        trade_id = random.randint(10000, 999999)
                        if trade_id not in used_trade_ids:
                            used_trade_ids.add(trade_id)
                            break

                if not all([stock_code, action, quantity]):
                    return jsonify({'error': '股票交易信息不完整，必须包含 stock_code, action, quantity'}), 400

                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM trade_logs WHERE strategy_id = %s AND trade_id = %s",
                                   (strategy_id, trade_id))
                    if cursor.fetchone():
                        return jsonify({'error': f'trade_id {trade_id} 在策略 {strategy_id} 中已存在'}), 400

                # 解析日期/时间
                try:
                    if date_input:
                        if ' ' in str(date_input):
                            # 包含时间，解析为 datetime
                            target_datetime = datetime.strptime(date_input, '%Y-%m-%d %H:%M')
                        else:
                            # 只有日期，解析为 date，时间为默认 09:30
                            target_date = datetime.strptime(date_input, '%Y-%m-%d').date()
                            target_datetime = datetime.combine(target_date, datetime.min.time()) + timedelta(hours=9, minutes=30)
                            # 向后兼容：如果只传日期（不含时间），强制 order_type 为 daily_order
                            order_type = 'daily_order'
                    else:
                        # 如果不传 date，使用当前时间
                        target_datetime = datetime.now()
                except ValueError:
                    return jsonify({'error': f'日期格式错误: {date_input}，应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM'}), 400

                intended_datetime = target_datetime

                # 验证订单类型
                if order_type not in ['daily_order', 'limit_order', 'market_order']:
                    return jsonify({'error': f'不支持的订单类型: {order_type}，必须是 daily_order/limit_order/market_order'}), 400

                # 限价单验证
                if order_type == 'limit_order':
                    if limit_price is None:
                        return jsonify({'error': f'限价单必须提供 limit_price 参数'}), 400

                    # 解析截止时间
                    if cutoff_time_input:
                        try:
                            cutoff_time = datetime.strptime(cutoff_time_input, '%Y-%m-%d %H:%M')
                        except ValueError:
                            return jsonify({'error': f'截止时间格式错误: {cutoff_time_input}，应为 YYYY-MM-DD HH:MM'}), 400
                    else:
                        # 默认截止时间为目标时间 + 10 分钟
                        cutoff_time = target_datetime + timedelta(minutes=10)

                    # 验证数据可用性
                    if not is_minute_data_available(target_datetime):
                        return jsonify({
                            'error': f'日期 {target_datetime} 的分钟数据不可用（超过 5 年历史），仅支持日度单'
                        }), 400
                elif order_type == 'market_order':
                    # 市价单也需要验证数据可用性
                    if not is_minute_data_available(target_datetime):
                        return jsonify({
                            'error': f'日期 {target_datetime} 的分钟数据不可用（超过 5 年历史），仅支持日度单'
                        }), 400
                    cutoff_time = None
                    limit_price = None
                else:  # daily_order
                    cutoff_time = None
                    limit_price = None

                # 检查交易日
                target_date_val = target_datetime.date()
                if is_trading_day(target_date_val):
                    target_date_for_db = target_datetime
                else:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs
                            (strategy_id, stock_code, trade_id, action, quantity, intended_date, target_date,
                             order_type, limit_price, cutoff_time, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'failed', '非交易日')
                        """, (strategy_id, stock_code, trade_id, action, quantity, intended_datetime,
                              intended_datetime, order_type, limit_price, cutoff_time))
                    continue

                # 插入交易日志
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trade_logs
                        (strategy_id, stock_code, trade_id, action, quantity, intended_date, target_date,
                         order_type, limit_price, cutoff_time, status, fail_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NULL)
                    """, (strategy_id, stock_code, trade_id, action, quantity,
                          intended_datetime, intended_datetime, order_type, limit_price, cutoff_time))

        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        logger.error(f"add_strategies 失败: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies', methods=['GET'])
def list_strategies():
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT strategy_id FROM strategies")
            rows = cursor.fetchall()
        return jsonify([r['strategy_id'] for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/nav', methods=['GET'])
def get_strategy_nav_history(strategy_id):
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date', date.today().strftime('%Y-%m-%d'))
    price_type = request.args.get('price_type', 'open')
    if price_type not in ['open', 'close']:
        price_type = 'open'

    try:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        else:
            conn = get_db()
            with conn.cursor() as cursor:
                cursor.execute("SELECT MIN(trade_date) as first_date FROM trades WHERE strategy_id = %s", (strategy_id,))
                row = cursor.fetchone()
                first_date = row['first_date']
            if not first_date:
                return jsonify([])
            # first_date 现在是 datetime 类型，需要转换为 date 类型进行计算
            first_date_val = first_date.date() if isinstance(first_date, datetime) else first_date
            start_date = first_date_val - timedelta(days=1)
            if start_date > end_date:
                return jsonify([])
    except Exception as e:
        return jsonify({'error': f'日期格式错误: {e}'}), 400

    # 将 start_date 和 end_date 转换为 datetime 类型，以匹配数据库中的 trade_date 字段
    start_datetime = datetime.combine(start_date, datetime.min.time()) if isinstance(start_date, date) else start_date
    end_datetime = datetime.combine(end_date, datetime.min.time()) if isinstance(end_date, date) else end_date

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])
    finally:
        conn.close()

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT stock_code FROM trades WHERE strategy_id = %s", (strategy_id,))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
    finally:
        conn.close()

    if not stock_codes:
        return jsonify([])

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    # 使用批量获取代替单独请求
    price_cache_all = batch_fetch_daily_prices(stock_codes, start_date, end_date)

    # 提取所需的价格类型
    price_cache = {}
    for (stock_code, trade_date, pt), price in price_cache_all.items():
        if pt == price_type:
            price_cache[(stock_code, trade_date)] = price

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date, stock_code, action, quantity, price
                FROM trades
                WHERE strategy_id = %s
                ORDER BY trade_date
            """, (strategy_id,))
            trades = cursor.fetchall()
    finally:
        conn.close()

    trades_by_date = defaultdict(list)
    for t in trades:
        # 使用 trade_date 的 date 部分作为字典键，确保类型一致
        trade_date_key = t['trade_date'].date() if isinstance(t['trade_date'], datetime) else t['trade_date']
        trades_by_date[trade_date_key].append(t)

    first_trade_date = trades[0]['trade_date'] if trades else None

    cash = initial_capital
    positions = defaultdict(float)
    for t in trades:
        # 将 t['trade_date'] 转换为 date 类型进行比较
        trade_date_val = t['trade_date'].date() if isinstance(t['trade_date'], datetime) else t['trade_date']
        if trade_date_val < start_date:
            qty = float(t['quantity'])
            price = float(t['price'])
            if t['action'] == 'buy':
                cash -= qty * price
                positions[t['stock_code']] += qty
            else:
                cash += qty * price
                positions[t['stock_code']] -= qty
                if positions[t['stock_code']] == 0:
                    del positions[t['stock_code']]
        else:
            break

    last_price = {}
    result = []
    for d in dates:
        if d == first_trade_date:
            day_trades = trades_by_date.get(d, [])
            for t in day_trades:
                qty = float(t['quantity'])
                price = float(t['price'])
                if t['action'] == 'buy':
                    cash -= qty * price
                    positions[t['stock_code']] += qty
                else:
                    cash += qty * price
                    positions[t['stock_code']] -= qty
                    if positions[t['stock_code']] == 0:
                        del positions[t['stock_code']]
            market_value = 0.0
            for stock_code, qty in positions.items():
                price = price_cache.get((stock_code, d))
                if price is None:
                    price = last_price.get(stock_code)
                    if price is None:
                        continue
                else:
                    last_price[stock_code] = price
                market_value += qty * price
            nav = cash + market_value
            nav_percent = (nav / initial_capital) * 100
            result.append({'date': d.strftime('%Y-%m-%d'), 'nav': round(nav, 2), 'nav_percent': round(nav_percent, 2)})
            continue

        market_value = 0.0
        for stock_code, qty in positions.items():
            price = price_cache.get((stock_code, d))
            if price is None:
                price = last_price.get(stock_code)
                if price is None:
                    continue
            else:
                last_price[stock_code] = price
            market_value += qty * price
        nav = cash + market_value
        nav_percent = (nav / initial_capital) * 100
        result.append({'date': d.strftime('%Y-%m-%d'), 'nav': round(nav, 2), 'nav_percent': round(nav_percent, 2)})

        day_trades = trades_by_date.get(d, [])
        for t in day_trades:
            qty = float(t['quantity'])
            price = float(t['price'])
            if t['action'] == 'buy':
                cash -= qty * price
                positions[t['stock_code']] += qty
            else:
                cash += qty * price
                positions[t['stock_code']] -= qty
                if positions[t['stock_code']] == 0:
                    del positions[t['stock_code']]
        for stock_code in positions.keys():
            if (stock_code, d) in price_cache:
                last_price[stock_code] = price_cache[(stock_code, d)]

    return jsonify(result)

@app.route('/api/strategies/<strategy_id>/current_nav', methods=['GET'])
def get_current_nav_endpoint(strategy_id):
    stocks_param = request.args.get('stocks')
    price_type = request.args.get('price_type', 'close')
    stock_list = stocks_param.split(',') if stocks_param else None
    result = get_current_nav(strategy_id, stock_list, price_type)
    if result is None:
        return jsonify({'error': '策略不存在'}), 404
    if stock_list is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
                row = cursor.fetchone()
                initial_capital = float(row['initial_capital']) if row else 1000000
        finally:
            conn.close()
        percent = (result / initial_capital) * 100
        return jsonify({'total_nav': result, 'nav_percent': round(percent, 2)})
    else:
        return jsonify(result)

@app.route('/api/index/<index_code>', methods=['GET'])
def get_index_data(index_code):
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    price_type = request.args.get('price_type', 'close')
    if not start_date_str or not end_date_str:
        return jsonify({'error': '缺少 start_date 或 end_date 参数'}), 400

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': '日期格式错误，应为 YYYY-MM-DD'}), 400

    # 使用缓存函数获取指数数据
    result = get_index_data_cached(index_code, start_date, end_date, price_type)

    if result is None:
        return jsonify({'error': f'指数 {index_code} 在指定区间无数据或获取失败'}), 500

    return jsonify(result)

@app.route('/api/strategies/<strategy_id>/holdings', methods=['GET'])
def get_strategy_holdings_at_date(strategy_id):
    date_str = request.args.get('date')
    price_type = request.args.get('price_type', 'open')
    if not date_str:
        return jsonify({'error': '缺少 date 参数'}), 400
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return jsonify({'error': '日期格式错误'}), 400
    display_date = target_date
    if not is_trading_day(target_date):
        target_date = get_next_trading_day(target_date, 'prev')
    # 将 target_date 转换为 datetime 类型以匹配数据库中的 trade_date 字段
    target_datetime = datetime.combine(target_date, datetime.min.time()) if isinstance(target_date, date) else target_date
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])
            cursor.execute("""
                SELECT stock_code, action, quantity, price, trade_date
                FROM trades
                WHERE strategy_id = %s AND trade_date <= %s
                ORDER BY trade_date, id
            """, (strategy_id, target_datetime))
            trades = cursor.fetchall()
            cursor.execute("""
                SELECT stock_code, trade_id, action, quantity, intended_date
                FROM trade_logs
                WHERE strategy_id = %s AND status='pending' AND target_date = %s
            """, (strategy_id, target_date))
            pending_orders = cursor.fetchall()
    finally:
        conn.close()

    cash = initial_capital
    positions = defaultdict(float)
    stock_batches = defaultdict(list)
    for t in trades:
        qty = float(t['quantity'])
        price = float(t['price'])
        trade_date = t['trade_date']
        if t['action'] == 'buy':
            cash -= qty * price
            positions[t['stock_code']] += qty
            stock_batches[t['stock_code']].append({'quantity': qty, 'buy_date': trade_date})
        else:
            cash += qty * price
            remaining_sell = qty
            batches = stock_batches[t['stock_code']]
            while remaining_sell > 0 and batches:
                first_batch = batches[0]
                if first_batch['quantity'] > remaining_sell:
                    first_batch['quantity'] -= remaining_sell
                    remaining_sell = 0
                else:
                    remaining_sell -= first_batch['quantity']
                    batches.pop(0)
            positions[t['stock_code']] -= qty
            if positions[t['stock_code']] == 0:
                del positions[t['stock_code']]
                stock_batches[t['stock_code']] = []

    price_cache_open = {}
    price_cache_close = {}
    for stock_code in positions.keys():
        price_open, _ = get_price_from_tushare(stock_code, target_date, 'open', auto_next=True)
        if price_open:
            price_cache_open[stock_code] = price_open
        price_close, _ = get_price_from_tushare(stock_code, target_date, 'close', auto_next=True)
        if price_close:
            price_cache_close[stock_code] = price_close

    total_mv_open = 0.0
    total_mv_close = 0.0
    holdings_list = []
    for stock_code, qty in positions.items():
        price_open = price_cache_open.get(stock_code)
        price_close = price_cache_close.get(stock_code)
        mv_open = qty * (price_open if price_open else 0)
        mv_close = qty * (price_close if price_close else 0)
        total_mv_open += mv_open
        total_mv_close += mv_close
        batches_detail = []
        for batch in stock_batches[stock_code]:
            # 将 buy_date 转换为 date 类型
            buy_date_val = batch['buy_date'].date() if isinstance(batch['buy_date'], datetime) else batch['buy_date']
            holding_days = count_trading_days(buy_date_val, display_date)
            batches_detail.append([batch['quantity'], holding_days])
        holdings_list.append({
            'stock_code': stock_code,
            'quantity': qty,
            'open_price': price_open if price_open else None,
            'close_price': price_close if price_close else None,
            'open_market_value': mv_open,
            'close_market_value': mv_close,
            'batches': batches_detail
        })

    total_mv = total_mv_open if price_type == 'open' else total_mv_close
    nav = cash + total_mv
    nav_percent = (nav / initial_capital) * 100

    pending_list = [{
        'stock_code': p['stock_code'],
        'trade_id': p['trade_id'],
        'action': p['action'],
        'quantity': p['quantity'],
        'intended_date': p['intended_date'].strftime('%Y-%m-%d') if p['intended_date'] else None,
        'target_date': target_date.strftime('%Y-%m-%d')
    } for p in pending_orders]

    return jsonify({
        'date': display_date.strftime('%Y-%m-%d'),
        'cash': round(cash, 2),
        'nav': round(nav, 2),
        'nav_percent': round(nav_percent, 2),
        'holdings': holdings_list,
        'pending_orders': pending_list
    })

@app.route('/api/strategies/<strategy_id>/trades', methods=['GET'])
def get_strategy_trades(strategy_id):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM trades WHERE strategy_id = %s ORDER BY trade_date, id", (strategy_id,))
            trades = cursor.fetchall()
        return jsonify(trades)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/logs', methods=['GET'])
def get_strategy_logs(strategy_id):
    date_str = request.args.get('date')
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            if date_str:
                filter_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                cursor.execute("SELECT * FROM trade_logs WHERE strategy_id = %s AND intended_date = %s ORDER BY intended_date DESC, created_at", (strategy_id, filter_date))
            else:
                cursor.execute("SELECT * FROM trade_logs WHERE strategy_id = %s ORDER BY intended_date DESC, created_at", (strategy_id,))
            logs = cursor.fetchall()

        # 格式化 datetime 字段
        for log in logs:
            for key in ['intended_date', 'target_date', 'actual_date', 'cutoff_time']:
                if log.get(key) and isinstance(log[key], datetime):
                    log[key] = log[key].strftime('%Y-%m-%d %H:%M')
                elif log.get(key):
                    # 如果是字符串，去掉秒数
                    log[key] = str(log[key])[:16]  # YYYY-MM-DD HH:MM

        return jsonify(logs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/trades/<int:trade_id>', methods=['DELETE'])
def delete_pending_trade(strategy_id, trade_id):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                return jsonify({'error': '策略不存在'}), 404
            cursor.execute("SELECT id, status FROM trade_logs WHERE strategy_id = %s AND trade_id = %s", (strategy_id, trade_id))
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': f'交易 {trade_id} 不存在'}), 404
            if row['status'] != 'pending':
                return jsonify({'error': f'交易状态为 {row["status"]}，无法撤销'}), 400
            cursor.execute("DELETE FROM trade_logs WHERE id = %s", (row['id'],))
        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                return jsonify({'error': '策略不存在'}), 404
            cursor.execute("DELETE FROM trade_logs WHERE strategy_id = %s", (strategy_id,))
            cursor.execute("DELETE FROM strategies WHERE strategy_id = %s", (strategy_id,))
        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/returns', methods=['GET'])
def get_strategy_returns(strategy_id):
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    if not start_date_str or not end_date_str:
        return jsonify({'error': '缺少 start_date 或 end_date'}), 400
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except:
        return jsonify({'error': '日期格式错误'}), 400

    # 将 start_date 和 end_date 转换为 datetime 类型，以匹配数据库中的 trade_date 字段
    start_datetime = datetime.combine(start_date, datetime.min.time()) if isinstance(start_date, date) else start_date
    end_datetime = datetime.combine(end_date, datetime.max.time()) if isinstance(end_date, date) else end_date

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date, stock_code, quantity, price, amount
                FROM trades
                WHERE strategy_id = %s AND action = 'buy'
                  AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date, stock_code
            """, (strategy_id, start_datetime, end_datetime))
            trades = cursor.fetchall()
    finally:
        conn.close()

    if not trades:
        return jsonify([])

    groups = defaultdict(list)
    for t in trades:
        # 使用 trade_date 的 date 部分作为字典键，确保类型一致
        trade_date_key = t['trade_date'].date() if isinstance(t['trade_date'], datetime) else t['trade_date']
        groups[trade_date_key].append(t)

    result = []
    for trade_date, group_trades in groups.items():
        total_cost = 0.0
        stock_info = []
        for t in group_trades:
            qty = float(t['quantity'])
            price = float(t['price'])
            cost = qty * price
            total_cost += cost
            stock_info.append({
                'stock_code': t['stock_code'],
                'quantity': qty,
                'price': price,
                'cost': cost
            })

        prev_trade_date = get_next_trading_day(trade_date, 'prev')
        prev_nav = get_strategy_nav_at_date(strategy_id, prev_trade_date, 'close')
        if prev_nav is None:
            conn = get_db()
            with conn.cursor() as cursor:
                cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
                prev_nav = float(cursor.fetchone()['initial_capital'])
            conn.close()
        turnover = total_cost / prev_nav if prev_nav > 0 else 0.0

        mv_list = []
        weighted_mv_sum = 0.0
        for info in stock_info:
            mv = get_stock_market_value(info['stock_code'], trade_date)
            if mv is not None:
                mv_list.append(mv)
                weighted_mv_sum += mv * info['cost']
            else:
                mv_list.append(None)

        valid_mvs = [m for m in mv_list if m is not None]
        holding_size_mean = weighted_mv_sum / total_cost if total_cost > 0 and valid_mvs else None
        holding_size_median = None
        if valid_mvs:
            sorted_mvs = sorted(valid_mvs)
            n = len(sorted_mvs)
            if n % 2 == 1:
                holding_size_median = sorted_mvs[n // 2]
            else:
                holding_size_median = (sorted_mvs[n // 2 - 1] + sorted_mvs[n // 2]) / 2

        # 计算 T0 收益率（当日收盘市值相对成本）
        mv_t0 = 0.0
        price_count_t0 = 0
        for info in stock_info:
            price_t0, _ = get_price_from_tushare(info['stock_code'], trade_date, 'close', auto_next=True)
            if price_t0:
                mv_t0 += info['quantity'] * price_t0
                price_count_t0 += 1
        # 只有当至少有一只股票获取到价格时，才计算收益率
        t0_return = (mv_t0 / total_cost - 1) if total_cost > 0 and price_count_t0 > 0 else None

        # 计算 T1~T5 收益率
        returns = {}
        for offset in range(1, 6):
            target_d = trade_date
            for _ in range(offset):
                target_d = get_next_trading_day(target_d, 'next')
            mv = 0.0
            price_count = 0
            for info in stock_info:
                price, _ = get_price_from_tushare(info['stock_code'], target_d, 'close', auto_next=True)
                if price:
                    mv += info['quantity'] * price
                    price_count += 1
            # 只有当至少有一只股票获取到价格时，才计算收益率
            returns[f'T{offset}'] = (mv / total_cost - 1) if total_cost > 0 and price_count > 0 else None

        comp_list = [(info['stock_code'], info['quantity']) for info in stock_info]
        comp_str = '[' + ', '.join([f"({code},{qty})" for code, qty in comp_list]) + ']'

        result.append({
            'date': trade_date.strftime('%Y-%m-%d'),
            'composition': comp_str,
            'composition_detail': comp_list,
            'holding_sum': sum(info['quantity'] for info in stock_info),
            'Holding_Size_Mean': round(holding_size_mean, 2) if holding_size_mean is not None else None,
            'Holding_Size_Median': round(holding_size_median, 2) if holding_size_median is not None else None,
            'T0_Return': round(t0_return * 100, 2) if t0_return is not None else None,
            'T1_Return': round(returns['T1'] * 100, 2) if returns['T1'] is not None else None,
            'T2_Return': round(returns['T2'] * 100, 2) if returns['T2'] is not None else None,
            'T3_Return': round(returns['T3'] * 100, 2) if returns['T3'] is not None else None,
            'T4_Return': round(returns['T4'] * 100, 2) if returns['T4'] is not None else None,
            'T5_Return': round(returns['T5'] * 100, 2) if returns['T5'] is not None else None,
            'turnover': round(turnover * 100, 2)
        })

    return jsonify(result)

# ========== 启动初始化 ==========
# 加载交易日历
load_trade_calendar()
# 初始化股票名称缓存（基于 Tushare）
init_stock_basic_cache()

# 启动调度器
scheduler = BackgroundScheduler()
# 原有的待成交订单处理（日度单、市价单），每 30 秒执行一次
scheduler.add_job(func=process_pending_orders, trigger="interval", seconds=30)
# 新增：限价单处理，每分钟执行一次
scheduler.add_job(func=process_limit_orders, trigger="interval", minutes=1)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
    