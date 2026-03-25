import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, date, time
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

# 配置日志输出到控制台
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ========== 配置 ==========
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41')
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 读取环境变量
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

MARKET_OPEN_TIME = time(9, 30)

def get_db():
    return pymysql.connect(**DB_CONFIG)

# ========== 交易日工具 ==========
def is_trading_day(dt: date) -> bool:
    """判断是否为交易日（上交所）"""
    print(f"[DEBUG] 检查交易日: {dt}")
    try:
        df = pro.trade_cal(exchange='SSE', start_date=dt.strftime('%Y%m%d'),
                           end_date=dt.strftime('%Y%m%d'))
        if df.empty:
            print(f"[WARN] 交易日历无数据: {dt}")
            return False
        is_open = df.iloc[0]['is_open'] == 1
        print(f"[DEBUG] {dt} 交易日: {is_open}")
        return is_open
    except Exception as e:
        print(f"[ERROR] is_trading_day 异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_next_trading_day(dt: date, direction='next') -> date:
    """获取下一个/上一个交易日（排除自身）"""
    step = 1 if direction == 'next' else -1
    current = dt + timedelta(days=step)
    while True:
        if is_trading_day(current):
            return current
        current += timedelta(days=step)

def get_price_from_tushare(stock_code, trade_date, price_type='open', auto_next=False):
    """
    获取指定股票在指定日期的价格。
    auto_next: 若为 True 且当天无数据，则自动取下一个交易日；若为 False 则直接返回 None。
    返回 (price, actual_date) 或 (None, None)
    """
    print(f"[DEBUG] 获取价格: {stock_code} {trade_date} {price_type} auto_next={auto_next}")
    if isinstance(trade_date, date):
        trade_date = trade_date.strftime('%Y%m%d')
    try:
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
            if not auto_next:
                print(f"[WARN] {trade_date} 无数据，且 auto_next=False，返回 None")
                return None, None
            print(f"[WARN] {trade_date} 无数据，尝试下一个交易日")
            next_date = get_next_trading_day(datetime.strptime(trade_date, '%Y%m%d').date(), 'next')
            next_str = next_date.strftime('%Y%m%d')
            df = pro.daily(ts_code=stock_code, start_date=next_str, end_date=next_str)
            if df.empty:
                print(f"[ERROR] 仍无数据: {stock_code} {next_str}")
                return None, None
            actual_date = next_date
        else:
            actual_date = datetime.strptime(trade_date, '%Y%m%d').date()
        price = float(df.iloc[0][price_type])
        print(f"[DEBUG] 获取成功: 价格={price}, 实际日期={actual_date}")
        return price, actual_date
    except Exception as e:
        print(f"[ERROR] get_price_from_tushare 异常: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def get_limit_price(stock_code, trade_date):
    """获取涨停跌停价"""
    print(f"[DEBUG] 获取涨跌停: {stock_code} {trade_date}")
    try:
        if isinstance(trade_date, date):
            trade_date = trade_date.strftime('%Y%m%d')
        df = pro.stk_limit(ts_code=stock_code, trade_date=trade_date)
        if not df.empty:
            up = float(df.iloc[0]['up_limit'])
            down = float(df.iloc[0]['down_limit'])
            print(f"[DEBUG] 涨停={up}, 跌停={down}")
            return up, down
        else:
            print(f"[WARN] 涨跌停无数据: {stock_code} {trade_date}")
            return None, None
    except Exception as e:
        print(f"[ERROR] get_limit_price 异常: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def get_latest_price(stock_code, price_type='close'):
    """获取最新交易日价格"""
    try:
        df = pro.daily(ts_code=stock_code, limit=1)
        if not df.empty:
            return float(df.iloc[0][price_type])
    except Exception as e:
        print(f"get_latest_price error: {e}")
    return None

def calculate_strategy_cash_and_positions(strategy_id):
    """
    计算当前现金和持仓（基于数据库已有成功交易）
    直接查询 trades 表，因为该表只存储成功交易
    """
    print(f"[DEBUG] 计算策略 {strategy_id} 现金和持仓")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                print(f"[WARN] 策略 {strategy_id} 不存在")
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
        print(f"[DEBUG] 现金={cash}, 持仓={positions}")
        return cash, positions
    except Exception as e:
        print(f"[ERROR] calculate_strategy_cash_and_positions 异常: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    finally:
        conn.close()

def get_current_nav(strategy_id, stock_list=None, price_type='close'):
    """获取当前净值"""
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

# ========== 交易执行函数 ==========
def execute_trade(log_id, strategy_id, stock_code, stock_id, action, quantity, target_date):
    """
    执行单笔交易，根据资金/持仓、涨跌停等条件决定成功或失败
    更新 trade_logs 状态，成功时插入 trades 表
    """
    print(f"[DEBUG] 执行交易: log_id={log_id}, {strategy_id} {stock_code} {action} {quantity} on {target_date}")

    # 获取价格（当天无数据直接失败，不再顺延）
    price, actual_date = get_price_from_tushare(stock_code, target_date, 'open', auto_next=False)
    if price is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE trade_logs 
                    SET status='failed', fail_reason='停牌或无数据', actual_date=%s
                    WHERE id=%s
                """, (target_date, log_id))
            conn.commit()
            print(f"[DEBUG] 交易失败（停牌或无数据），日志已更新")
        except Exception as e:
            print(f"[ERROR] 更新日志失败: {e}")
        finally:
            conn.close()
        return

    # 获取当前策略现金和持仓（基于已完成交易）
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id=%s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                print(f"[ERROR] 策略 {strategy_id} 不存在")
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
                SELECT stock_code, 
                       SUM(CASE WHEN action='buy' THEN quantity ELSE -quantity END) as net_qty
                FROM trades
                WHERE strategy_id = %s
                GROUP BY stock_code
                HAVING net_qty != 0
            """, (strategy_id,))
            positions = {row['stock_code']: row['net_qty'] for row in cursor.fetchall()}
    finally:
        conn.close()

    # 检查涨跌停
    up_limit, down_limit = get_limit_price(stock_code, target_date)
    if action == 'buy' and up_limit is not None and abs(price - up_limit) < 0.001:
        fail_reason = '涨停无法买入'
        print(f"[WARN] {fail_reason}")
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE trade_logs 
                    SET status='failed', fail_reason=%s, actual_date=%s, price=%s
                    WHERE id=%s
                """, (fail_reason, target_date, price, log_id))
            conn.commit()
        finally:
            conn.close()
        return
    if action == 'sell' and down_limit is not None and abs(price - down_limit) < 0.001:
        fail_reason = '跌停无法卖出'
        print(f"[WARN] {fail_reason}")
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE trade_logs 
                    SET status='failed', fail_reason=%s, actual_date=%s, price=%s
                    WHERE id=%s
                """, (fail_reason, target_date, price, log_id))
            conn.commit()
        finally:
            conn.close()
        return

    # 检查资金/持仓
    if action == 'buy':
        cost = quantity * price
        if cash < cost:
            fail_reason = '资金不足'
            print(f"[WARN] {fail_reason}: 需要{cost}, 当前现金{cash}")
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE trade_logs 
                        SET status='failed', fail_reason=%s, actual_date=%s, price=%s
                        WHERE id=%s
                    """, (fail_reason, target_date, price, log_id))
                conn.commit()
            finally:
                conn.close()
            return
    else:  # sell
        current_qty = positions.get(stock_code, 0)
        if current_qty < quantity:
            fail_reason = '持仓不足'
            print(f"[WARN] {fail_reason}: 需要{quantity}, 当前持仓{current_qty}")
            conn = get_db()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE trade_logs 
                        SET status='failed', fail_reason=%s, actual_date=%s, price=%s
                        WHERE id=%s
                    """, (fail_reason, target_date, price, log_id))
                conn.commit()
            finally:
                conn.close()
            return

    # 执行成功
    print(f"[INFO] 交易成功: {strategy_id} {stock_code} {action} {quantity}@{price} {target_date}")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 插入 trades 表（包含 stock_id）
            cursor.execute("""
                INSERT INTO trades 
                (strategy_id, stock_code, stock_id, trade_date, action, quantity, price, price_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'open')
            """, (strategy_id, stock_code, stock_id, target_date, action, quantity, price))

            # 更新 trade_logs 状态
            cursor.execute("""
                UPDATE trade_logs 
                SET status='success', actual_date=%s, price=%s
                WHERE id=%s
            """, (target_date, price, log_id))
        conn.commit()
        print(f"[DEBUG] 交易成功，日志状态已更新为 success")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 插入 trades 或更新日志失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

# ========== 调度器：处理 pending 订单 ==========
def process_pending_orders():
    """扫描所有 pending 且 target_date <= 当前日期的订单，尝试执行"""
    print("[DEBUG] 扫描 pending 订单...")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            today = date.today()
            cursor.execute("""
                SELECT id, strategy_id, stock_code, stock_id, action, quantity, target_date
                FROM trade_logs
                WHERE status='pending' AND target_date <= %s
            """, (today,))
            pending_orders = cursor.fetchall()
        print(f"[DEBUG] 找到 {len(pending_orders)} 条待处理订单")
    finally:
        conn.close()

    for order in pending_orders:
        execute_trade(order['id'], order['strategy_id'], order['stock_code'],
                      order['stock_id'], order['action'], order['quantity'], order['target_date'])

# ========== 路由 ==========
@app.route('/api/strategies', methods=['POST'])
def add_strategies():
    """
    提交策略交易列表，插入 pending 日志
    每个交易必须包含 stock_id，且在同一策略下唯一
    """
    print("[DEBUG] 收到 POST /api/strategies 请求")
    data = request.get_json()
    print(f"[DEBUG] 请求数据: {data}")
    if not isinstance(data, list):
        print("[ERROR] 请求体不是数组")
        return jsonify({'error': '请求体应为数组'}), 400

    conn = get_db()
    try:
        for strategy in data:
            strategy_id = strategy.get('strategy_id')
            if not strategy_id:
                print("[ERROR] 缺少 strategy_id")
                return jsonify({'error': '缺少 strategy_id'}), 400

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
                exists = cursor.fetchone()
                print(f"[DEBUG] 策略 {strategy_id} 存在: {exists}")

            if exists and 'initial_capital' in strategy:
                print(f"[ERROR] 策略 {strategy_id} 已存在，不能修改初始资金")
                return jsonify({'error': f'策略 {strategy_id} 已存在，不能修改初始资金'}), 400

            if not exists:
                initial_capital = strategy.get('initial_capital', 1000000.0)
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO strategies (strategy_id, initial_capital) VALUES (%s, %s)",
                                   (strategy_id, initial_capital))
                print(f"[DEBUG] 插入新策略 {strategy_id}, 初始资金={initial_capital}")

            stocks = strategy.get('stocks', [])
            if not stocks:
                print(f"[WARN] 策略 {strategy_id} 无股票交易")
                continue

            for item in stocks:
                stock_code = item.get('stock_code')
                stock_id = item.get('stock_id')
                action = item.get('action')
                quantity = item.get('quantity')
                if not all([stock_code, stock_id, action, quantity]):
                    print("[ERROR] 股票交易信息不完整，缺少 stock_id 或其它字段")
                    return jsonify({'error': '股票交易信息不完整，必须包含 stock_id'}), 400

                # 检查 stock_id 是否已存在（同一策略下）
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 1 FROM trade_logs 
                        WHERE strategy_id = %s AND stock_id = %s
                    """, (strategy_id, stock_id))
                    if cursor.fetchone():
                        print(f"[ERROR] stock_id {stock_id} 在策略 {strategy_id} 中已存在")
                        return jsonify({'error': f'stock_id {stock_id} 在策略 {strategy_id} 中已存在'}), 400

                # 确定 intended_date（用户意图）
                if 'date' in item:
                    try:
                        intended_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                        print(f"[DEBUG] 用户提供日期: {intended_date}")
                    except:
                        print(f"[ERROR] 日期格式错误: {item['date']}")
                        return jsonify({'error': f'日期格式错误: {item["date"]}'}), 400
                else:
                    intended_date = date.today()
                    print(f"[DEBUG] 未提供日期，使用今天: {intended_date}")

                # 计划执行日期：如果是交易日则用当天，否则非交易日直接失败（不再顺延）
                if is_trading_day(intended_date):
                    target_date = intended_date
                else:
                    # 非交易日直接失败
                    fail_reason = '非交易日'
                    print(f"[WARN] {intended_date} 非交易日，交易失败")
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs 
                            (strategy_id, stock_code, stock_id, action, quantity, intended_date, target_date, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'failed', %s)
                        """, (strategy_id, stock_code, stock_id, action, quantity, intended_date, intended_date, fail_reason))
                    continue

                # 插入 pending 日志
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trade_logs 
                        (strategy_id, stock_code, stock_id, action, quantity, intended_date, target_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                    """, (strategy_id, stock_code, stock_id, action, quantity, intended_date, target_date))

        conn.commit()
        print("[DEBUG] 所有交易意向已记录为 pending")
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        print("[ERROR] 发生异常，回滚事务")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies', methods=['GET'])
def list_strategies():
    """获取所有策略ID"""
    print("[DEBUG] 收到 GET /api/strategies 请求")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT strategy_id FROM strategies")
            rows = cursor.fetchall()
        print(f"[DEBUG] 返回策略列表: {[r['strategy_id'] for r in rows]}")
        return jsonify([r['strategy_id'] for r in rows])
    except Exception as e:
        print(f"[ERROR] list_strategies 异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/nav', methods=['GET'])
def get_strategy_nav_history(strategy_id):
    """
    获取策略历史净值，支持指定价格类型（open/close）
    从最早交易日前一天开始，展示初始净值（100%），最早交易日净值反映收盘后状态。
    参数:
        start_date: YYYY-MM-DD (可选)
        end_date: YYYY-MM-DD (可选，默认今天)
        price_type: open 或 close，默认 open
    """
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/nav 请求")
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
                print(f"[WARN] 策略 {strategy_id} 无成功交易")
                return jsonify([])
            # 起始日设为第一个交易日的前一天（用于展示初始净值）
            start_date = first_date - timedelta(days=1)
            if start_date > end_date:
                print(f"[WARN] 起始日期 {start_date} 晚于结束日期 {end_date}")
                return jsonify([])
            print(f"[DEBUG] 自动计算起始日: {start_date} (最早交易日: {first_date})")
    except Exception as e:
        print(f"[ERROR] 日期解析错误: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'日期格式错误: {e}'}), 400

    # 获取初始资金
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                print(f"[ERROR] 策略 {strategy_id} 不存在")
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])
    finally:
        conn.close()

    # 获取涉及的所有股票代码（从成功交易）
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT stock_code FROM trades WHERE strategy_id = %s", (strategy_id,))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
    finally:
        conn.close()

    if not stock_codes:
        print(f"[WARN] 策略 {strategy_id} 无股票代码")
        return jsonify([])

    # 生成连续日期范围（包含起始日到结束日）
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    print(f"[DEBUG] 日期范围: {dates[0]} 到 {dates[-1]}, 共 {len(dates)} 天")

    # 批量获取价格（根据 price_type）
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
            print(f"[DEBUG] 股票 {stock_code} 获取到 {len(df)} 条价格记录")
        except Exception as e:
            print(f"[ERROR] 获取 {stock_code} 价格失败: {e}")

    # 获取所有成功交易并按日期排序
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

    # 找到第一个有交易的日期
    first_trade_date = trades[0]['trade_date'] if trades else None

    # 初始化现金和持仓
    cash = initial_capital
    positions = defaultdict(float)

    # 预处理：对于 start_date 之前的交易（如果用户指定了更早的 start_date），先应用
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
        # 特殊处理第一个交易日：先应用交易再计算净值
        if d == first_trade_date:
            # 先处理当天的交易
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

            # 计算当日净值
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
            result.append({
                'date': d.strftime('%Y-%m-%d'),
                'nav': round(nav, 2),
                'nav_percent': round(nav_percent, 2)
            })
            continue

        # 其他日期：先计算开盘前净值，再应用当日交易
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
        result.append({
            'date': d.strftime('%Y-%m-%d'),
            'nav': round(nav, 2),
            'nav_percent': round(nav_percent, 2)
        })

        # 处理当日交易（更新现金和持仓，用于下一天）
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

        # 更新 last_price
        for stock_code in positions.keys():
            if (stock_code, d) in price_cache:
                last_price[stock_code] = price_cache[(stock_code, d)]

    print(f"[DEBUG] 返回 {len(result)} 条净值数据")
    return jsonify(result)

@app.route('/api/strategies/<strategy_id>/current_nav', methods=['GET'])
def get_current_nav_endpoint(strategy_id):
    """获取当前净值（总净值或指定股票）"""
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/current_nav")
    stocks_param = request.args.get('stocks')
    price_type = request.args.get('price_type', 'close')
    stock_list = stocks_param.split(',') if stocks_param else None

    result = get_current_nav(strategy_id, stock_list, price_type)
    if result is None:
        print(f"[ERROR] 策略 {strategy_id} 不存在")
        return jsonify({'error': '策略不存在'}), 404

    if stock_list is None:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
                row = cursor.fetchone()
                if row:
                    initial_capital = float(row['initial_capital'])
                else:
                    return jsonify({'error': '策略不存在'}), 404
        finally:
            conn.close()
        percent = (result / initial_capital) * 100
        return jsonify({'total_nav': result, 'nav_percent': round(percent, 2)})
    else:
        return jsonify(result)

@app.route('/api/index/sh000300', methods=['GET'])
def get_index_sh000300():
    """沪深300，返回连续日期（非交易日沿用前值）"""
    print("[DEBUG] 收到 GET /api/index/sh000300 请求")
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    price_type = request.args.get('price_type')

    if not all([start_date_str, end_date_str, price_type]):
        print("[ERROR] 缺少必要参数")
        return jsonify({'error': '缺少必要参数'}), 400

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except:
        print("[ERROR] 日期格式错误")
        return jsonify({'error': '日期格式错误'}), 400

    df = pro.index_daily(ts_code='000300.SH',
                         start_date=start_date.strftime('%Y%m%d'),
                         end_date=end_date.strftime('%Y%m%d'))
    if df.empty:
        print("[ERROR] 沪深300无数据")
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

        result.append({
            'date': d.strftime('%Y-%m-%d'),
            'value': price,
            'percent_change': round(relative, 2)
        })

    print(f"[DEBUG] 返回 {len(result)} 条沪深300数据")
    return jsonify(result)

@app.route('/api/strategies/<strategy_id>/holdings', methods=['GET'])
def get_strategy_holdings_at_date(strategy_id):
    """
    获取指定日期收盘后的持仓快照，包含开盘价和收盘价相关数据
    参数:
        date: YYYY-MM-DD
        price_type: open 或 close，用于计算整体净值（默认为 open）
    """
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/holdings")
    date_str = request.args.get('date')
    price_type = request.args.get('price_type', 'open')
    if not date_str:
        print("[ERROR] 缺少 date 参数")
        return jsonify({'error': '缺少 date 参数'}), 400
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        print("[ERROR] 日期格式错误")
        return jsonify({'error': '日期格式错误'}), 400

    # 如果 target_date 不是交易日，向前找到最近交易日（用于价格获取）
    display_date = target_date
    if not is_trading_day(target_date):
        target_date = get_next_trading_day(target_date, 'prev')
        print(f"[DEBUG] {display_date} 非交易日，调整为最近交易日: {target_date}")

    # 计算下一个交易日（用于查询预购股）
    next_trading_day = get_next_trading_day(target_date, 'next')

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                print(f"[ERROR] 策略 {strategy_id} 不存在")
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])

            # 获取 target_date 及之前的成功交易（包含当天，即收盘后状态）
            cursor.execute("""
                SELECT stock_code, action, quantity, price
                FROM trades
                WHERE strategy_id = %s AND trade_date <= %s
                ORDER BY trade_date
            """, (strategy_id, target_date))
            trades = cursor.fetchall()

            # 获取下一个交易日的 pending 订单（包含 stock_id）
            cursor.execute("""
                SELECT stock_code, stock_id, action, quantity, intended_date
                FROM trade_logs
                WHERE strategy_id = %s AND status='pending' AND target_date = %s
            """, (strategy_id, next_trading_day))
            pending_orders = cursor.fetchall()
    finally:
        conn.close()

    # 计算现金和持仓（按时间顺序累加）
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

    # 获取 target_date 的开盘价和收盘价
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
        if price_open is None and price_close is None:
            continue
        mv_open = qty * (price_open if price_open else 0)
        mv_close = qty * (price_close if price_close else 0)
        total_mv_open += mv_open
        total_mv_close += mv_close
        holdings_list.append({
            'stock_code': stock_code,
            'quantity': qty,
            'open_price': price_open if price_open else None,
            'close_price': price_close if price_close else None,
            'open_market_value': mv_open,
            'close_market_value': mv_close
        })

    # 根据 price_type 参数计算整体净值
    if price_type == 'open':
        total_mv = total_mv_open
    else:
        total_mv = total_mv_close

    nav = cash + total_mv
    nav_percent = (nav / initial_capital) * 100

    # 构建预购股列表（包含 stock_id）
    pending_list = []
    for p in pending_orders:
        pending_list.append({
            'stock_code': p['stock_code'],
            'stock_id': p['stock_id'],
            'action': p['action'],
            'quantity': p['quantity'],
            'intended_date': p['intended_date'].strftime('%Y-%m-%d') if p['intended_date'] else None,
            'target_date': next_trading_day.strftime('%Y-%m-%d')
        })

    print(f"[DEBUG] 返回持仓快照: 日期={display_date}, 现金={cash}, 净值={nav}, 预购股数={len(pending_list)}")
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
    """获取策略的所有成功交易记录"""
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/trades")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM trades
                WHERE strategy_id = %s
                ORDER BY trade_date, id
            """, (strategy_id,))
            trades = cursor.fetchall()
        print(f"[DEBUG] 返回 {len(trades)} 条交易记录")
        return jsonify(trades)
    except Exception as e:
        print(f"[ERROR] get_strategy_trades 异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/logs', methods=['GET'])
def get_strategy_logs(strategy_id):
    """获取策略的所有交易日志（包含 pending、success、failed）"""
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/logs")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM trade_logs
                WHERE strategy_id = %s
                ORDER BY intended_date DESC, created_at
            """, (strategy_id,))
            logs = cursor.fetchall()
        print(f"[DEBUG] 返回 {len(logs)} 条日志记录")
        return jsonify(logs)
    except Exception as e:
        print(f"[ERROR] get_strategy_logs 异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    """删除策略及其所有关联数据（包括交易记录和日志）"""
    print(f"[DEBUG] 收到 DELETE /api/strategies/{strategy_id}")
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                print(f"[WARN] 策略 {strategy_id} 不存在")
                return jsonify({'error': '策略不存在'}), 404

            cursor.execute("DELETE FROM trade_logs WHERE strategy_id = %s", (strategy_id,))
            cursor.execute("DELETE FROM strategies WHERE strategy_id = %s", (strategy_id,))
        conn.commit()
        print(f"[DEBUG] 策略 {strategy_id} 删除成功")
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] delete_strategy 异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/trades/<int:stock_id>', methods=['DELETE'])
def delete_pending_trade(strategy_id, stock_id):
    """撤销指定策略下指定 stock_id 的 pending 交易"""
    print(f"[DEBUG] 收到 DELETE /api/strategies/{strategy_id}/trades/{stock_id}")

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 检查策略是否存在
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                return jsonify({'error': '策略不存在'}), 404

            # 查询该交易是否存在且状态为 pending
            cursor.execute("""
                SELECT id, status FROM trade_logs
                WHERE strategy_id = %s AND stock_id = %s
            """, (strategy_id, stock_id))
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': f'交易 {stock_id} 在策略 {strategy_id} 中不存在'}), 404
            if row['status'] != 'pending':
                return jsonify({'error': f'交易状态为 {row["status"]}，无法撤销'}), 400

            # 删除该交易日志（实际业务中也可标记为取消，但需求是撤销，直接删除即可）
            cursor.execute("DELETE FROM trade_logs WHERE id = %s", (row['id'],))
        conn.commit()
        print(f"[DEBUG] 交易 {stock_id} 已撤销")
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] 撤销交易失败: {e}")
        import traceback
        traceback.print_exc()
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