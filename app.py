import sys
from collections import defaultdict
from datetime import datetime, timedelta, date, time
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts

# 配置日志输出到控制台
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ========== 配置 ==========
TUSHARE_TOKEN = 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41'
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'lianghui',
    'database': 'stock_trace',
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

def get_price_from_tushare(stock_code, trade_date, price_type='open'):
    """
    获取指定股票在指定日期的价格，若非交易日则自动取下一个交易日
    返回 (price, actual_date) 或 (None, None)
    """
    print(f"[DEBUG] 获取价格: {stock_code} {trade_date} {price_type}")
    if isinstance(trade_date, date):
        trade_date = trade_date.strftime('%Y%m%d')
    try:
        # 先尝试当天
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
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
    """计算当前现金和持仓（基于数据库已有交易）"""
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

# ========== 路由 ==========
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/strategies', methods=['POST'])
def add_strategies():
    """
    提交策略交易列表，逐笔检查失败条件
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

            # ----- 获取当前现金和持仓 -----
            if not exists:
                # 新策略：现金 = 初始资金，持仓为空
                current_cash = initial_capital
                current_positions = {}
                print(f"[DEBUG] 新策略，当前现金={current_cash}, 持仓为空")
            else:
                current_cash, current_positions = calculate_strategy_cash_and_positions(strategy_id)
                if current_cash is None:
                    print(f"[ERROR] 策略 {strategy_id} 数据异常")
                    return jsonify({'error': f'策略 {strategy_id} 数据异常'}), 500
                print(f"[DEBUG] 现有策略，当前现金={current_cash}, 当前持仓={current_positions}")

            # 逐笔处理新交易
            for item in stocks:
                stock_code = item.get('stock_code')
                action = item.get('action')
                quantity = item.get('quantity')
                if not all([stock_code, action, quantity]):
                    print("[ERROR] 股票交易信息不完整")
                    return jsonify({'error': '股票交易信息不完整'}), 400

                # ----- 确定意图交易日期 -----
                if 'date' in item:
                    try:
                        base_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                        intended_date = base_date
                        print(f"[DEBUG] 使用提供的日期: {intended_date}")
                    except:
                        print(f"[ERROR] 日期格式错误: {item['date']}")
                        return jsonify({'error': f'日期格式错误: {item["date"]}'}), 400
                else:
                    now = datetime.now()
                    today = now.date()
                    if now.time() < MARKET_OPEN_TIME and is_trading_day(today):
                        intended_date = today
                    else:
                        intended_date = get_next_trading_day(today, 'next')
                    print(f"[DEBUG] 自动确定日期: {intended_date}")

                # 获取实际交易日期和价格
                price, actual_date = get_price_from_tushare(stock_code, intended_date, 'open')
                if price is None:
                    fail_reason = '停牌或无数据'
                    print(f"[WARN] {stock_code} {intended_date} {fail_reason}")
                    # 记录失败日志
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs 
                            (strategy_id, stock_code, action, quantity, intended_date, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                        """, (strategy_id, stock_code, action, quantity, intended_date, fail_reason))
                    continue

                # 检查涨停/跌停
                up_limit, down_limit = get_limit_price(stock_code, actual_date)
                if action == 'buy' and up_limit is not None and abs(price - up_limit) < 0.001:
                    fail_reason = '涨停无法买入'
                    print(f"[WARN] {stock_code} {actual_date} {fail_reason}")
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs 
                            (strategy_id, stock_code, action, quantity, intended_date, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                        """, (strategy_id, stock_code, action, quantity, intended_date, fail_reason))
                    continue
                if action == 'sell' and down_limit is not None and abs(price - down_limit) < 0.001:
                    fail_reason = '跌停无法卖出'
                    print(f"[WARN] {stock_code} {actual_date} {fail_reason}")
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO trade_logs 
                            (strategy_id, stock_code, action, quantity, intended_date, status, fail_reason)
                            VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                        """, (strategy_id, stock_code, action, quantity, intended_date, fail_reason))
                    continue

                # 检查资金/持仓
                if action == 'buy':
                    cost = quantity * price
                    if current_cash < cost:
                        fail_reason = '资金不足'
                        print(f"[WARN] {fail_reason}: 需要{cost}, 当前现金{current_cash}")
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO trade_logs 
                                (strategy_id, stock_code, action, quantity, intended_date, status, fail_reason)
                                VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                            """, (strategy_id, stock_code, action, quantity, intended_date, fail_reason))
                        continue
                else:  # sell
                    current_qty = current_positions.get(stock_code, 0)
                    if current_qty < quantity:
                        fail_reason = '持仓不足'
                        print(f"[WARN] {fail_reason}: 需要{quantity}, 当前持仓{current_qty}")
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO trade_logs 
                                (strategy_id, stock_code, action, quantity, intended_date, status, fail_reason)
                                VALUES (%s, %s, %s, %s, %s, 'failed', %s)
                            """, (strategy_id, stock_code, action, quantity, intended_date, fail_reason))
                        continue

                # 所有检查通过，执行交易
                print(f"[INFO] 交易成功: {strategy_id} {stock_code} {action} {quantity}@{price} {actual_date}")
                # 插入 trades 表
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trades 
                        (strategy_id, stock_code, trade_date, action, quantity, price, price_type)
                        VALUES (%s, %s, %s, %s, %s, %s, 'open')
                    """, (strategy_id, stock_code, actual_date, action, quantity, price))

                # 插入成功日志
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trade_logs 
                        (strategy_id, stock_code, action, quantity, intended_date, actual_date, price, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'success')
                    """, (strategy_id, stock_code, action, quantity, intended_date, actual_date, price))

                # 更新当前现金和持仓（用于后续同批交易判断）
                if action == 'buy':
                    current_cash -= cost
                    current_positions[stock_code] = current_positions.get(stock_code, 0) + quantity
                else:
                    current_cash += quantity * price
                    current_positions[stock_code] = current_positions.get(stock_code, 0) - quantity
                    if current_positions[stock_code] == 0:
                        del current_positions[stock_code]

        conn.commit()
        print("[DEBUG] 所有交易处理完成，提交成功")
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
    """获取策略历史净值（开盘价），从最早交易当天开始，非交易日沿用前值"""
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/nav 请求")
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date', date.today().strftime('%Y-%m-%d'))
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
                print(f"[WARN] 策略 {strategy_id} 无交易记录")
                return jsonify([])
            # 起始日设为第一个交易日当天
            start_date = first_date
            if start_date > end_date:
                print(f"[WARN] 起始日期 {start_date} 晚于结束日期 {end_date}")
                return jsonify([])
            print(f"[DEBUG] 自动计算起始日: {start_date}")
    except Exception as e:
        print(f"[ERROR] 日期解析错误: {e}")
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

    # 获取涉及的所有股票代码
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

    # 生成连续日期范围（每一天）
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    print(f"[DEBUG] 日期范围: {dates[0]} 到 {dates[-1]}, 共 {len(dates)} 天")

    # 批量获取交易日价格
    price_cache = {}  # (stock_code, date) -> price (仅交易日)
    for stock_code in stock_codes:
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        try:
            df = pro.daily(ts_code=stock_code, start_date=start_str, end_date=end_str)
            if not df.empty:
                for _, row in df.iterrows():
                    trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                    price = float(row['open'])
                    price_cache[(stock_code, trade_date)] = price
            print(f"[DEBUG] 股票 {stock_code} 获取到 {len(df)} 条价格记录")
        except Exception as e:
            print(f"[ERROR] 获取 {stock_code} 价格失败: {e}")

    # 获取所有交易并按日期排序
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

    # 预处理：计算 start_date 开盘前的现金和持仓
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

    last_price = {}  # 用于非交易日价格回退
    result = []
    for d in dates:
        # 计算当日开盘市值
        market_value_open = 0.0
        for stock_code, qty in positions.items():
            price = price_cache.get((stock_code, d))
            if price is None:
                price = last_price.get(stock_code)
                if price is None:
                    continue
            else:
                last_price[stock_code] = price
            market_value_open += qty * price

        nav_open = cash + market_value_open
        nav_percent_open = (nav_open / initial_capital) * 100
        result.append({
            'date': d.strftime('%Y-%m-%d'),
            'nav': round(nav_open, 2),
            'nav_percent': round(nav_percent_open, 2)
        })

        # 处理当日交易
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

        # 为所有持仓股票更新 last_price（使用当天的价格，如果有）
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

    # 获取交易日数据
    df = pro.index_daily(ts_code='000300.SH',
                         start_date=start_date.strftime('%Y%m%d'),
                         end_date=end_date.strftime('%Y%m%d'))
    if df.empty:
        print("[ERROR] 沪深300无数据")
        return jsonify({'error': '无数据'}), 404

    df = df.sort_values('trade_date')
    # 构建交易日价格映射
    price_map = {}
    for _, row in df.iterrows():
        date_obj = datetime.strptime(row['trade_date'], '%Y%m%d').date()
        price_map[date_obj] = float(row[price_type])

    # 生成连续日期
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    # 填充价格，非交易日沿用前一个交易日价格
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
    """获取指定日期开盘前的持仓快照（含现金），非交易日返回最近交易日"""
    print(f"[DEBUG] 收到 GET /api/strategies/{strategy_id}/holdings")
    date_str = request.args.get('date')
    if not date_str:
        print("[ERROR] 缺少 date 参数")
        return jsonify({'error': '缺少 date 参数'}), 400
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        print("[ERROR] 日期格式错误")
        return jsonify({'error': '日期格式错误'}), 400

    # 如果 target_date 不是交易日，向前找到最近交易日
    if not is_trading_day(target_date):
        target_date = get_next_trading_day(target_date, 'prev')
        print(f"[DEBUG] 调整为最近交易日: {target_date}")

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                print(f"[ERROR] 策略 {strategy_id} 不存在")
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])

            # 获取 target_date 之前的交易（不含当天）
            cursor.execute("""
                SELECT stock_code, action, quantity, price
                FROM trades
                WHERE strategy_id = %s AND trade_date < %s
            """, (strategy_id, target_date))
            trades = cursor.fetchall()
    finally:
        conn.close()

    # 计算现金和持仓
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

    # 获取 target_date 的开盘价
    price_cache = {}
    for stock_code in positions.keys():
        price, _ = get_price_from_tushare(stock_code, target_date, 'open')
        if price:
            price_cache[stock_code] = price

    total_mv = 0.0
    holdings_list = []
    for stock_code, qty in positions.items():
        price = price_cache.get(stock_code)
        if price is None:
            continue
        mv = qty * price
        total_mv += mv
        holdings_list.append({
            'stock_code': stock_code,
            'quantity': qty,
            'price': price,
            'market_value': mv
        })

    nav = cash + total_mv
    nav_percent = (nav / initial_capital) * 100

    print(f"[DEBUG] 返回持仓快照: 日期={target_date}, 现金={cash}, 净值={nav}")
    return jsonify({
        'date': target_date.strftime('%Y-%m-%d'),
        'cash': round(cash, 2),
        'nav': round(nav, 2),
        'nav_percent': round(nav_percent, 2),
        'holdings': holdings_list
    })

@app.route('/api/strategies/<strategy_id>/trades', methods=['GET'])
def get_strategy_trades(strategy_id):
    """获取策略的所有交易记录（无日期筛选）"""
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
    """获取策略的所有交易日志（成功+失败）"""
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

            # 先删除日志（因为日志没有外键关联）
            cursor.execute("DELETE FROM trade_logs WHERE strategy_id = %s", (strategy_id,))
            # 再删除策略（级联删除 trades）
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

if __name__ == '__main__':
    app.run(debug=True)