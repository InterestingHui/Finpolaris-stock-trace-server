import os
from datetime import datetime, timedelta, date
import pymysql
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import tushare as ts

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ========== 配置 ==========
TUSHARE_TOKEN = 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41'  # 请替换
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'lianghui',  # 请替换
    'database': 'stock_trace',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db():
    return pymysql.connect(**DB_CONFIG)

# ========== 工具函数 ==========
def get_price_from_tushare(stock_code, trade_date, price_type='open'):
    """
    获取指定股票在指定日期的开盘价或收盘价
    :param stock_code: e.g. '000001.SZ'
    :param trade_date: 日期字符串 'YYYYMMDD' 或 date对象
    :param price_type: 'open' 或 'close'
    :return: float 价格，或 None
    """
    if isinstance(trade_date, date):
        trade_date = trade_date.strftime('%Y%m%d')
    try:
        df = pro.daily(ts_code=stock_code, start_date=trade_date, end_date=trade_date)
        if df.empty:
            # 尝试获取前一个交易日的数据
            prev_date = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d')
            df = pro.daily(ts_code=stock_code, start_date=prev_date, end_date=trade_date)
            if df.empty:
                return None
            df = df[df['trade_date'] <= trade_date].head(1)
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
    """
    计算策略当前现金余额和持仓（净数量）
    返回: (cash, positions_dict)
    positions_dict: {stock_code: net_quantity}
    """
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 获取初始资金
            cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
            row = cursor.fetchone()
            if not row:
                return None, None
            initial_capital = float(row['initial_capital'])  # <--- 转为 float

            # 获取所有交易记录，计算现金变动
            cursor.execute("""
                SELECT action, amount FROM trades 
                WHERE strategy_id = %s
            """, (strategy_id,))
            trades = cursor.fetchall()
            cash = initial_capital
            for t in trades:
                amount = float(t['amount'])  # <--- amount 也是 Decimal，转为 float
                if t['action'] == 'buy':
                    cash -= amount
                else:
                    cash += amount

            # 计算净持仓
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
    """
    获取策略当前总净值，或指定股票的当前净值
    如果stock_list为None，返回总净值(float)；否则返回列表，每个元素为 {stock_code, nav}
    """
    cash, positions = calculate_strategy_cash_and_positions(strategy_id)
    if cash is None:
        return None

    if stock_list is None:
        # 总净值 = 现金 + 所有持仓市值
        total_mv = 0.0
        for stock_code, qty in positions.items():
            qty = float(qty)
            price = get_latest_price(stock_code, price_type)
            if price:
                total_mv += qty * price
        return cash + total_mv
    else:
        # 返回指定股票的市值
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

            # 检查策略是否已存在
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
                exists = cursor.fetchone()

            # 如果策略已存在且请求中提供了 initial_capital，则拒绝
            if exists and 'initial_capital' in strategy:
                return jsonify({'error': f'策略 {strategy_id} 已存在，不能修改初始资金'}), 400

            # 处理 initial_capital：如果不存在则插入时用默认值，存在则使用原值（不需要更新）
            if not exists:
                initial_capital = strategy.get('initial_capital', 1000000.0)
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO strategies (strategy_id, initial_capital) 
                        VALUES (%s, %s)
                    """, (strategy_id, initial_capital))

            # 处理交易记录（无论策略新老都执行）
            stocks = strategy.get('stocks', [])
            if not stocks:
                continue

            for item in stocks:
                stock_code = item.get('stock_code')
                action = item.get('action')
                quantity = item.get('quantity')
                if not all([stock_code, action, quantity]):
                    return jsonify({'error': '股票交易信息不完整'}), 400

                # 日期处理：默认当天
                trade_date_str = item.get('date')
                if trade_date_str:
                    try:
                        trade_date = datetime.strptime(trade_date_str, '%Y-%m-%d').date()
                    except:
                        return jsonify({'error': f'日期格式错误: {trade_date_str}'}), 400
                else:
                    trade_date = date.today()

                price_type = item.get('price_type', 'open')

                # 获取价格
                price = get_price_from_tushare(stock_code, trade_date, price_type)
                if price is None:
                    return jsonify({'error': f'无法获取{stock_code}在{trade_date}的{price_type}价格'}), 400

                # 插入交易记录
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
    """获取所有策略ID"""
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
    """
    获取策略历史净值（百分比形式），计算每日开盘时的净值。
    参数:
        start_date: YYYY-MM-DD (可选)
        end_date: YYYY-MM-DD (可选，默认今天)
    """
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date', date.today().strftime('%Y-%m-%d'))
    # 强制使用开盘价
    price_type = 'open'

    # 解析日期
    try:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        else:
            # 取策略最早交易日期
            conn = get_db()
            with conn.cursor() as cursor:
                cursor.execute("SELECT MIN(trade_date) as first_date FROM trades WHERE strategy_id = %s", (strategy_id,))
                row = cursor.fetchone()
                first_date = row['first_date']
            if not first_date:
                return jsonify([])
            start_date = first_date
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

    # 生成日期范围
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    # ---------- 批量获取所有股票在日期范围内的开盘价 ----------
    price_cache = {}          # (stock_code, date) -> price
    last_price = {}           # 用于非交易日回退
    for stock_code in stock_codes:
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        try:
            df = pro.daily(ts_code=stock_code, start_date=start_str, end_date=end_str)
            if not df.empty:
                for _, row in df.iterrows():
                    trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').date()
                    price = float(row['open'])   # 确保取开盘价
                    price_cache[(stock_code, trade_date)] = price
        except Exception as e:
            print(f"获取 {stock_code} 价格失败: {e}")

    # ---------- 获取所有交易并按日期排序 ----------
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

    from collections import defaultdict
    trades_by_date = defaultdict(list)
    for t in trades:
        trades_by_date[t['trade_date']].append(t)

    # ---------- 逐日计算开盘净值 ----------
    cash = initial_capital
    positions = defaultdict(float)   # 当前持仓（交易后），初始为空

    result = []
    for d in dates:
        # 1. 计算当日开盘市值（基于前一交易日后持仓）
        market_value_open = 0.0
        for stock_code, qty in positions.items():
            price = price_cache.get((stock_code, d))
            if price is None:
                # 非交易日或无数据：沿用最近一次价格
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

        # 2. 处理当日交易，更新现金和持仓（用于下一天开盘）
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
    """
    获取策略当前总净值或指定股票的当前净值
    参数:
        stocks: 逗号分隔的股票代码，如 "000001.SZ,000002.SZ" (可选)
        price_type: open/close (默认close)
    """
    stocks_param = request.args.get('stocks')
    price_type = request.args.get('price_type', 'close')
    stock_list = stocks_param.split(',') if stocks_param else None

    result = get_current_nav(strategy_id, stock_list, price_type)
    if result is None:
        return jsonify({'error': '策略不存在'}), 404

    if stock_list is None:
        initial_capital = None
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT initial_capital FROM strategies WHERE strategy_id = %s", (strategy_id,))
                row = cursor.fetchone()
                if row:
                    initial_capital = float(row['initial_capital'])
        finally:
            conn.close()
        if initial_capital:
            percent = (result / initial_capital) * 100
            return jsonify({'total_nav': result, 'nav_percent': round(percent, 2)})
        else:
            return jsonify({'total_nav': result})
    else:
        return jsonify(result)

@app.route('/api/index/sh000300', methods=['GET'])
def get_index_sh000300():
    """
    获取沪深300指数历史波动（百分比形式）
    参数:
        start_date: YYYY-MM-DD (必填)
        end_date: YYYY-MM-DD (必填)
        price_type: open/close (必填)
    """
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

    # 获取沪深300指数数据
    # 指数代码 '000300.SH'
    df = pro.index_daily(ts_code='000300.SH', start_date=start_date.strftime('%Y%m%d'),
                         end_date=end_date.strftime('%Y%m%d'))
    if df.empty:
        return jsonify({'error': '无数据'}), 404

    df = df.sort_values('trade_date')
    base_price = None
    result = []
    for _, row in df.iterrows():
        date_str = row['trade_date']
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        if price_type == 'open':
            price = row['open']
        else:
            price = row['close']

        if base_price is None:
            base_price = price
            percent_change = 0.0
        else:
            percent_change = (price - base_price) / base_price * 100

        result.append({
            'date': date_obj.strftime('%Y-%m-%d'),
            'value': float(price),
            'percent_change': round(percent_change, 2)
        })

    return jsonify(result)

@app.route('/api/strategies/<strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    """删除策略及其所有交易记录（利用外键级联删除）"""
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 检查策略是否存在
            cursor.execute("SELECT 1 FROM strategies WHERE strategy_id = %s", (strategy_id,))
            if not cursor.fetchone():
                return jsonify({'error': '策略不存在'}), 404

            # 直接删除策略，由于外键设置了 ON DELETE CASCADE，关联的 trades 会自动删除
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