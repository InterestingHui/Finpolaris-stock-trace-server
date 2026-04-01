import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, date
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts
import akshare as ak
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import logging
from functools import lru_cache

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

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

# ========== 交易日工具 ==========
def is_trading_day(dt: date) -> bool:
    try:
        df = pro.trade_cal(exchange='SSE', start_date=dt.strftime('%Y%m%d'),
                           end_date=dt.strftime('%Y%m%d'))
        if df.empty:
            return False
        return df.iloc[0]['is_open'] == 1
    except Exception as e:
        print(f"[ERROR] is_trading_day: {e}")
        return False

def get_next_trading_day(dt: date, direction='next') -> date:
    step = 1 if direction == 'next' else -1
    current = dt + timedelta(days=step)
    while True:
        if is_trading_day(current):
            return current
        current += timedelta(days=step)

def count_trading_days(start_date: date, end_date: date) -> int:
    """计算两个日期之间（包含两端）的交易日数量"""
    count = 0
    current = start_date
    while current <= end_date:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count

def get_price_from_tushare(stock_code, trade_date, price_type='open', auto_next=False):
    if isinstance(trade_date, date):
        trade_date = trade_date.strftime('%Y%m%d')
    try:
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
            if not auto_next:
                return None, None
            next_date = get_next_trading_day(datetime.strptime(trade_date, '%Y%m%d').date(), 'next')
            next_str = next_date.strftime('%Y%m%d')
            df = pro.daily(ts_code=stock_code, start_date=next_str, end_date=next_str)
            if df.empty:
                return None, None
            actual_date = next_date
        else:
            actual_date = datetime.strptime(trade_date, '%Y%m%d').date()
        price = float(df.iloc[0][price_type])
        return price, actual_date
    except Exception as e:
        print(f"[ERROR] get_price_from_tushare: {e}")
        return None, None

def get_limit_price(stock_code, trade_date):
    try:
        if isinstance(trade_date, date):
            trade_date = trade_date.strftime('%Y%m%d')
        df = pro.stk_limit(ts_code=stock_code, trade_date=trade_date)
        if not df.empty:
            return float(df.iloc[0]['up_limit']), float(df.iloc[0]['down_limit'])
        return None, None
    except Exception as e:
        print(f"[ERROR] get_limit_price: {e}")
        return None, None

def get_latest_price(stock_code, price_type='close'):
    try:
        df = pro.daily(ts_code=stock_code, limit=1)
        if not df.empty:
            return float(df.iloc[0][price_type])
    except Exception as e:
        print(f"get_latest_price error: {e}")
    return None

# ========== 缓存辅助函数（日线仅开高低收） ==========
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
        print(f"[缓存] 读取日线失败: {e}")
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
        print(f"[缓存] 保存日线失败: {e}")
    finally:
        conn.close()

def fetch_stock_daily_info(stock_code: str, trade_date: date):
    """获取股票日线行情（开高低收），优先缓存，否则 Tushare"""
    cached = _get_cached_daily(stock_code, trade_date)
    if cached:
        print(f"[日线缓存] 命中 {stock_code} {trade_date}")
        return cached

    date_str = trade_date.strftime('%Y%m%d')
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
        print(f"[日线] Tushare 获取失败 {stock_code} {date_str}: {e}")
        return None

# ========== 股票名称缓存 ==========
def _get_cached_name(stock_code: str):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT stock_name FROM stock_name_cache WHERE stock_code = %s", (stock_code,))
            row = cursor.fetchone()
            return row['stock_name'] if row else None
    except Exception as e:
        print(f"[缓存] 读取名称失败: {e}")
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
        print(f"[缓存] 保存名称失败: {e}")
    finally:
        conn.close()

@lru_cache(maxsize=5000)
def get_stock_name(stock_code: str) -> str:
    cached = _get_cached_name(stock_code)
    if cached:
        return cached

    pure_code = stock_code.split('.')[0]
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == pure_code]
        if not row.empty:
            name = row.iloc[0]['名称']
            _save_cached_name(stock_code, name)
            return name
    except Exception as e:
        print(f"[股票名称] 获取失败 {stock_code}: {e}")
    return None

# ========== VWAP 计算 ==========
# ========== VWAP 计算（使用 Tushare 日线） ==========
def get_vwap(stock_code: str, trade_date: date) -> float:
    """
    使用 Tushare 日线数据计算 VWAP。
    Tushare 数据说明:
        - vol: 成交量，单位是“手” (1手 = 100股)
        - amount: 成交额，单位是“千元”
    因此，VWAP (元/股) = (amount * 1000) / (vol * 100)
    返回 None 表示获取失败。
    """
    date_str = trade_date.strftime('%Y%m%d')
    try:
        df = pro.daily(ts_code=stock_code, start_date=date_str, end_date=date_str)
        if df.empty:
            return None
        row = df.iloc[0]
        amount = float(row['amount'])   # 成交额，单位：千元
        vol = float(row['vol'])         # 成交量，单位：手
        if vol == 0:
            return None
        # 转换为正确的单位并计算
        vwap = (amount * 1000) / (vol * 100)     # 元/股
        return round(vwap, 3)
    except Exception as e:
        print(f"[VWAP] Tushare 获取失败 {stock_code} {date_str}: {e}")
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
        print(f"[ERROR] calculate_strategy_cash_and_positions: {e}")
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

# ========== 交易执行 ==========
def execute_trade(log_id, strategy_id, stock_code, trade_id, action, quantity, target_date):
    # 1. 获取 VWAP
    vwap = get_vwap(stock_code, target_date)
    if vwap is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE trade_logs SET status='failed', fail_reason='停牌或无数据', actual_date=%s WHERE id=%s", (target_date, log_id))
            conn.commit()
        finally:
            conn.close()
        return

    # 2. 获取开盘价
    open_price, _ = get_price_from_tushare(stock_code, target_date, 'open', auto_next=False)
    if open_price is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE trade_logs SET status='failed', fail_reason='无法获取开盘价', actual_date=%s WHERE id=%s", (target_date, log_id))
            conn.commit()
        finally:
            conn.close()
        return

    # 3. 涨跌停判断
    up_limit, down_limit = get_limit_price(stock_code, target_date)
    if action == 'buy' and up_limit is not None and open_price >= up_limit - 0.001:
        fail_reason = '涨停无法买入'
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s", (fail_reason, target_date, vwap, log_id))
            conn.commit()
        finally:
            conn.close()
        return
    if action == 'sell' and down_limit is not None and open_price <= down_limit + 0.001:
        fail_reason = '跌停无法卖出'
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s", (fail_reason, target_date, vwap, log_id))
            conn.commit()
        finally:
            conn.close()
        return

    # 4. 资金/持仓检查
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id=%s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return
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
        cost = quantity * vwap
        if cash < cost:
            fail_reason = '资金不足'
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s", (fail_reason, target_date, vwap, log_id))
                conn.commit()
            finally:
                conn.close()
            return
    else:
        current_qty = positions.get(stock_code, 0)
        if current_qty < quantity:
            fail_reason = '持仓不足'
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE trade_logs SET status='failed', fail_reason=%s, actual_date=%s, price=%s WHERE id=%s", (fail_reason, target_date, vwap, log_id))
                conn.commit()
            finally:
                conn.close()
            return

    # 5. 成功：获取日线行情和股票名称
    daily_info = fetch_stock_daily_info(stock_code, target_date)
    stock_name = get_stock_name(stock_code)

    update_fields = {
        'status': 'success',
        'actual_date': target_date,
        'price': vwap,
        'vwap': vwap,
        'stock_name': stock_name,
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
                INSERT INTO trades (strategy_id, stock_code, trade_id, trade_date, action, quantity, price, price_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'vwap')
            """, (strategy_id, stock_code, trade_id, target_date, action, quantity, vwap))
            set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
            values = list(update_fields.values()) + [log_id]
            cursor.execute(f"UPDATE trade_logs SET {set_clause} WHERE id=%s", values)
        conn.commit()
        print(f"[INFO] 交易成功: {strategy_id} trade_id={trade_id} {action} {quantity}@{vwap}")
    except pymysql.err.IntegrityError as e:
        conn.rollback()
        print(f"[WARN] 唯一约束冲突，交易已存在，忽略本次重复执行: {e}")
        with conn.cursor() as cursor:
            cursor.execute("UPDATE trade_logs SET status='success', actual_date=%s WHERE id=%s", (target_date, log_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 交易执行失败: {e}")
        with conn.cursor() as cursor:
            cursor.execute("UPDATE trade_logs SET status='failed', fail_reason=%s WHERE id=%s", (str(e)[:255], log_id))
        conn.commit()
    finally:
        conn.close()

def process_pending_orders():
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            today = date.today()
            cursor.execute("""
                SELECT id, strategy_id, stock_code, trade_id, action, quantity, target_date
                FROM trade_logs
                WHERE status='pending' AND target_date <= %s
            """, (today,))
            pending_orders = cursor.fetchall()
    finally:
        conn.close()

    for order in pending_orders:
        # 快速检查是否已存在成功交易
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
                    print(f"[INFO] 交易已存在，日志标记为 success: {order['strategy_id']} trade_id={order['trade_id']}")
                    continue
        finally:
            conn.close()

        execute_trade(order['id'], order['strategy_id'], order['stock_code'],
                      order['trade_id'], order['action'], order['quantity'], order['target_date'])

# ========== 路由 ==========
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/strategies', methods=['POST'])
def add_strategies():
    print("[DEBUG] POST /api/strategies")
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
            for item in stocks:
                stock_code = item.get('stock_code')
                trade_id = item.get('trade_id')
                action = item.get('action')
                quantity = item.get('quantity')
                if not all([stock_code, trade_id, action, quantity]):
                    return jsonify({'error': '股票交易信息不完整，必须包含 trade_id'}), 400

                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM trade_logs WHERE strategy_id = %s AND trade_id = %s",
                                   (strategy_id, trade_id))
                    if cursor.fetchone():
                        return jsonify({'error': f'trade_id {trade_id} 在策略 {strategy_id} 中已存在'}), 400

                if 'date' in item:
                    intended_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                else:
                    intended_date = date.today()

                if is_trading_day(intended_date):
                    target_date = intended_date
                else:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs
                            (strategy_id, stock_code, trade_id, action, quantity, intended_date, target_date, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'failed', '非交易日')
                        """, (strategy_id, stock_code, trade_id, action, quantity, intended_date, intended_date))
                    continue

                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trade_logs
                        (strategy_id, stock_code, trade_id, action, quantity, intended_date, target_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                    """, (strategy_id, stock_code, trade_id, action, quantity, intended_date, target_date))

        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] add_strategies: {e}")
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
            start_date = first_date - timedelta(days=1)
            if start_date > end_date:
                return jsonify([])
    except Exception as e:
        return jsonify({'error': f'日期格式错误: {e}'}), 400

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

    price_cache = {}
    for stock_code in stock_codes:
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        try:
            df = pro.daily(ts_code=stock_code, start_date=start_str, end_date=end_str)
            if not df.empty:
                for _, row in df.iterrows():
                    trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                    price = float(row[price_type])
                    price_cache[(stock_code, trade_date)] = price
        except Exception as e:
            print(f"[ERROR] 获取 {stock_code} 价格失败: {e}")

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
        trades_by_date[t['trade_date']].append(t)

    first_trade_date = trades[0]['trade_date'] if trades else None

    cash = initial_capital
    positions = defaultdict(float)
    for t in trades:
        if t['trade_date'] < start_date:
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

@app.route('/api/index/sh000300', methods=['GET'])
def get_index_sh000300():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    price_type = request.args.get('price_type')
    if not all([start_date_str, end_date_str, price_type]):
        return jsonify({'error': '缺少必要参数'}), 400
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except:
        return jsonify({'error': '日期格式错误'}), 400
    df = pro.index_daily(ts_code='000300.SH',
                         start_date=start_date.strftime('%Y%m%d'),
                         end_date=end_date.strftime('%Y%m%d'))
    if df.empty:
        return jsonify({'error': '无数据'}), 404
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
    result = []
    last_price = None
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
        result.append({'date': d.strftime('%Y-%m-%d'), 'value': price, 'percent_change': round(relative, 2)})
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
            """, (strategy_id, target_date))
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
            # 使用交易日计数函数计算实际持股天数
            holding_days = count_trading_days(batch['buy_date'], display_date)
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

# ========== 启动调度器 ==========
scheduler = BackgroundScheduler()
scheduler.add_job(func=process_pending_orders, trigger="interval", seconds=30)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)