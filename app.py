import os
from collections import defaultdict
from datetime import datetime, timedelta, date, time
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts

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

MARKET_OPEN_TIME = time(9, 30)  # 开盘时间

def get_db():
    return pymysql.connect(**DB_CONFIG)

# ========== 交易日工具 ==========
def is_trading_day(dt: date) -> bool:
    """判断是否为交易日（上交所）"""
    df = pro.trade_cal(exchange='SSE', start_date=dt.strftime('%Y%m%d'),
                       end_date=dt.strftime('%Y%m%d'))
    if df.empty:
        return False
    return df.iloc[0]['is_open'] == 1

def get_next_trading_day(dt: date, direction='next') -> date:
    """获取下一个/上一个交易日"""
    step = 1 if direction == 'next' else -1
    current = dt
    while True:
        if is_trading_day(current):
            return current
        current += timedelta(days=step)

def get_price_from_tushare(stock_code, trade_date, price_type='open'):
    """
    获取指定股票在指定日期的价格，若非交易日则自动取下一个交易日
    """
    if isinstance(trade_date, date):
        trade_date = trade_date.strftime('%Y%m%d')
    try:
        # 先尝试当天
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
            # 获取下一个交易日
            next_date = get_next_trading_day(datetime.strptime(trade_date, '%Y%m%d').date(), 'next')
            next_str = next_date.strftime('%Y%m%d')
            df = pro.daily(ts_code=stock_code, start_date=next_str, end_date=next_str)
            if df.empty:
                return None
        price = float(df.iloc[0][price_type])
        return price
    except Exception as e:
        print(f"tushare error: {e}")
        return None

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
    """计算当前现金和持仓"""
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
    提交策略交易列表
    请求体: JSON数组，每个元素:
    {
        "strategy_id": "strategy_001",
        "initial_capital": 1000000,   # 可选，默认100万
        "stocks": [ ... ]
    }
    """
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
            if not stocks:
                continue

            for item in stocks:
                stock_code = item.get('stock_code')
                action = item.get('action')
                quantity = item.get('quantity')
                if not all([stock_code, action, quantity]):
                    return jsonify({'error': '股票交易信息不完整'}), 400

                # ----- 确定交易日期 -----
                if 'date' in item:
                    try:
                        base_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                    except:
                        return jsonify({'error': f'日期格式错误: {item["date"]}'}), 400
                    trade_date = base_date if is_trading_day(base_date) else get_next_trading_day(base_date, 'next')
                else:
                    now = datetime.now()
                    today = now.date()
                    if now.time() < MARKET_OPEN_TIME and is_trading_day(today):
                        trade_date = today
                    else:
                        trade_date = get_next_trading_day(today, 'next')

                # 强制使用开盘价
                price_type = 'open'
                price = get_price_from_tushare(stock_code, trade_date, price_type)
                if price is None:
                    return jsonify({'error': f'无法获取{stock_code}在{trade_date}的开盘价'}), 400

                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO trades 
                        (strategy_id, stock_code, trade_date, action, quantity, price, price_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (strategy_id, stock_code, trade_date, action, quantity, price, price_type))

        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
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
    finally:
        conn.close()

@app.route('/api/strategies/<strategy_id>/nav', methods=['GET'])
def get_strategy_nav_history(strategy_id):
    """获取策略历史净值（开盘价），起始日为最早交易日的下一个交易日"""
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
                return jsonify([])
            # 起始日设为第一个交易日之后的下一个交易日
            start_date = get_next_trading_day(first_date, 'next')
            if start_date > end_date:
                return jsonify([])
    except Exception as e:
        return jsonify({'error': f'日期格式错误: {e}'}), 400

    # 获取初始资金
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

    # 获取涉及的所有股票代码
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT stock_code FROM trades WHERE strategy_id = %s", (strategy_id,))
            stock_codes = [r['stock_code'] for r in cursor.fetchall()]
    finally:
        conn.close()

    if not stock_codes:
        return jsonify([])

    # 批量获取价格
    price_cache = {}  # (stock_code, date) -> price
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
        except Exception as e:
            print(f"获取 {stock_code} 价格失败: {e}")

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

    # ---------- 预处理：计算 start_date 开盘前的现金和持仓 ----------
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
            break  # 已排序，后续交易日期 >= start_date

    # 生成日期范围
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

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

        # 处理当日交易，更新现金和持仓（用于下一天开盘）
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
    """沪深300，第一天为100%"""
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    price_type = request.args.get('price_type')

    if not all([start_date_str, end_date_str, price_type]):
        return jsonify({'error': '缺少必要参数'}), 400

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except:
        return jsonify({'error': '日期格式错误'}), 400

    df = pro.index_daily(ts_code='000300.SH',
                         start_date=start_date.strftime('%Y%m%d'),
                         end_date=end_date.strftime('%Y%m%d'))
    if df.empty:
        return jsonify({'error': '无数据'}), 404

    df = df.sort_values('trade_date')
    base_price = None
    result = []
    for _, row in df.iterrows():
        date_str = row['trade_date']
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        price = float(row[price_type])

        if base_price is None:
            base_price = price
            relative = 100.0
        else:
            relative = (price / base_price) * 100

        result.append({
            'date': date_obj.strftime('%Y-%m-%d'),
            'value': price,
            'percent_change': round(relative, 2)
        })

    return jsonify(result)

@app.route('/api/strategies/<strategy_id>/holdings', methods=['GET'])
def get_strategy_holdings_at_date(strategy_id):
    """获取指定日期开盘前的持仓快照"""
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': '缺少 date 参数'}), 400
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return jsonify({'error': '日期格式错误'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': '策略不存在'}), 404
            initial_capital = float(row['initial_capital'])

            cursor.execute("""
                SELECT stock_code, action, quantity, price
                FROM trades
                WHERE strategy_id = %s AND trade_date < %s
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

    # 获取目标日期开盘价
    price_cache = {}
    for stock_code in positions.keys():
        price = get_price_from_tushare(stock_code, target_date, 'open')
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

    return jsonify({
        'date': date_str,
        'nav': round(nav, 2),
        'nav_percent': round(nav_percent, 2),
        'holdings': holdings_list
    })

@app.route('/api/strategies/<strategy_id>/trades', methods=['GET'])
def get_strategy_trades(strategy_id):
    date_str = request.args.get('date')
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                except:
                    return jsonify({'error': '日期格式错误'}), 400
                cursor.execute("""
                    SELECT * FROM trades
                    WHERE strategy_id = %s AND trade_date = %s
                    ORDER BY id
                """, (strategy_id, date_obj))
            else:
                cursor.execute("""
                    SELECT * FROM trades
                    WHERE strategy_id = %s
                    ORDER BY trade_date, id
                """, (strategy_id,))
            trades = cursor.fetchall()
    finally:
        conn.close()
    return jsonify(trades)

@app.route('/api/strategies/<strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                return jsonify({'error': '策略不存在'}), 404
            cursor.execute("DELETE FROM strategies WHERE strategy_id = %s", (strategy_id,))
        conn.commit()
        return jsonify({'message': 'success'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)