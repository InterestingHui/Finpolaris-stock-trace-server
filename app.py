# app.py
import os
import logging
import json
from datetime import datetime, timedelta
import pymysql
from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
import tushare as ts

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# ================== 配置 ==================
TUSHARE_TOKEN = 'de81b74f57902d498037a789ac0f31b5e485df1bff7f0bfe211e8a41'  # 请替换为真实token
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()
logging.basicConfig(level=logging.DEBUG)

# 数据库连接配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'lianghui',
    'database': 'trading_db',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 5,      # 新增：连接超时5秒
    'read_timeout': 10,        # 可选：读取超时
    'write_timeout': 10        # 可选：写入超时
}

def get_db_connection():
    try:
        logging.info(f"Connecting to MySQL: {DB_CONFIG['host']}")
        conn = pymysql.connect(**DB_CONFIG)
        logging.info("Connection successful")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise e

# ================== 工具函数 ==================
def get_price_from_tushare(stock_code, date, price_type):
    """
    获取指定股票在指定日期的价格
    :param stock_code: e.g. '000001.SZ'
    :param date: 字符串 'YYYYMMDD'
    :param price_type: 'open' 或 'close'
    :return: float 价格, 或 None 如果获取失败
    """
    try:
        df = pro.daily(ts_code=stock_code, start_date=date, end_date=date)
        if df.empty:
            # 尝试获取前一个交易日的数据
            df = pro.daily(ts_code=stock_code, start_date=(datetime.strptime(date, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d'), end_date=date)
            if df.empty:
                return None
            df = df[df['trade_date'] <= date].head(1)  # 取最近的一条
        price = float(df.iloc[0][price_type])
        return price
    except Exception as e:
        print(f"tushare error: {e}")
        return None

def get_latest_price(stock_code):
    """
    获取最新交易日收盘价
    """
    try:
        df = pro.daily(ts_code=stock_code, limit=1)
        if not df.empty:
            return float(df.iloc[0]['close'])
    except Exception as e:
        print(f"get_latest_price error: {e}")
    return None

def update_position_after_trade(stock_code, action, quantity, trade_price, signal_id=None):
    """
    根据交易更新positions表，并记录快照
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 查询当前持仓
            cursor.execute("SELECT * FROM positions WHERE stock_code = %s", (stock_code,))
            pos = cursor.fetchone()

            old_quantity = pos['quantity'] if pos else 0
            old_avg_cost = pos['avg_cost'] if pos else 0

            if action == 'buy':
                new_quantity = old_quantity + quantity
                # 计算新的加权平均成本
                new_avg_cost = (old_avg_cost * old_quantity + trade_price * quantity) / new_quantity if new_quantity > 0 else 0
                new_price = trade_price  # 买入后最新价暂时用成交价
            else:  # sell
                if old_quantity < quantity:
                    raise ValueError("卖出数量超过持仓")
                new_quantity = old_quantity - quantity
                new_avg_cost = old_avg_cost  # 卖出不改变平均成本
                new_price = trade_price  # 卖出价

            # 计算市值和收益
            market_value = new_quantity * new_price
            profit = market_value - new_quantity * new_avg_cost if new_quantity > 0 else 0

            if new_quantity == 0:
                # 清仓：删除持仓记录，但保留历史快照（最后一条将在后面插入）
                cursor.execute("DELETE FROM positions WHERE stock_code = %s", (stock_code,))
            else:
                # 更新或插入持仓
                if pos:
                    cursor.execute("""
                        UPDATE positions SET quantity=%s, avg_cost=%s, latest_price=%s,
                        market_value=%s, profit=%s WHERE stock_code=%s
                    """, (new_quantity, new_avg_cost, new_price, market_value, profit, stock_code))
                else:
                    cursor.execute("""
                        INSERT INTO positions (stock_code, quantity, avg_cost, latest_price, market_value, profit)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (stock_code, new_quantity, new_avg_cost, new_price, market_value, profit))

            # 记录快照到 position_history
            notes = f"{action} {quantity} shares at {trade_price:.2f}" + (f" (signal_id={signal_id})" if signal_id else "")
            cursor.execute("""
                INSERT INTO position_history (stock_code, snapshot_time, quantity, price, market_value, profit, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (stock_code, datetime.now(), new_quantity, new_price, market_value, profit, notes))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ================== 路由 ==================
@app.route('/')
def index():
    return send_from_directory('/home/admin/Finpolaris-stock-trace-web', 'index.html')

@app.route('/data_trace', methods=['POST', 'OPTIONS'])
def data_trace():
    """接收买卖决策，处理交易并记录"""
    if request.method == 'OPTIONS':
        return '', 200  # 预检请求直接返回成功
    data = request.get_json()
    if not data:
        return jsonify({'error': '无效的JSON'}), 400

    # 必填字段校验
    required = ['signal_id', 'timestamp', 'stock_code', 'action', 'quantity', 'price_type']
    for field in required:
        if field not in data:
            return jsonify({'error': f'缺少字段: {field}'}), 400

    signal_id = data['signal_id']
    timestamp_str = data['timestamp']
    stock_code = data['stock_code']
    action = data['action']
    quantity = data['quantity']
    price_type = data['price_type']
    confidence = data.get('confidence', 0.0)
    news_source = data.get('news_source', '')

    # 解析时间戳 (ISO格式: "2025-03-06T14:35")
    try:
        dt = datetime.fromisoformat(timestamp_str)
        date_str = dt.strftime('%Y%m%d')
    except:
        return jsonify({'error': 'timestamp格式错误，应为YYYY-MM-DDTHH:MM'}), 400

    # 根据price_type决定取哪一天的价格
    # open: 下一个交易日开盘价 (这里简化用当天，实际可根据需要调整)
    # close: 当天收盘价
    # 注意：如果日期是今天且未收盘，可能获取不到，这里直接尝试获取，若失败则返回错误
    if price_type == 'open':
        # 简单使用当天开盘价，实际可调用交易日历获取下一个交易日
        target_date = date_str
    else:  # close
        target_date = date_str

    trade_price = get_price_from_tushare(stock_code, target_date, price_type)
    if trade_price is None:
        return jsonify({'error': f'无法获取{stock_code}在{target_date}的{price_type}价格'}), 400

    # 插入signals记录
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO signals (signal_id, timestamp, stock_code, action, quantity, price_type, confidence, news_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (signal_id, dt, stock_code, action, quantity, price_type, confidence, news_source))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'插入signals失败: {str(e)}'}), 500
    finally:
        conn.close()

    # 更新持仓
    try:
        update_position_after_trade(stock_code, action, quantity, trade_price, signal_id)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'更新持仓失败: {str(e)}'}), 500

    return jsonify({'message': 'success', 'trade_price': trade_price}), 200

@app.route('/api/positions', methods=['GET', 'OPTIONS'])
def get_positions():
    """返回当前所有持仓"""
    if request.method == 'OPTIONS':
        return '', 200  # 预检请求直接返回成功
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM positions ORDER BY stock_code")
            rows = cursor.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route('/api/history', methods=['GET', 'OPTIONS'])
def get_history():
    """返回持仓历史快照，可按股票过滤"""
    if request.method == 'OPTIONS':
        return '', 200  # 预检请求直接返回成功
    stock_code = request.args.get('stock_code')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if stock_code:
                cursor.execute("SELECT * FROM position_history WHERE stock_code = %s ORDER BY snapshot_time", (stock_code,))
            else:
                cursor.execute("SELECT * FROM position_history ORDER BY stock_code, snapshot_time")
            rows = cursor.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route('/api/signals', methods=['GET', 'OPTIONS'])
def get_signals():
    """返回所有决策记录"""
    if request.method == 'OPTIONS':
        return '', 200  # 预检请求直接返回成功
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM signals ORDER BY timestamp DESC")
            rows = cursor.fetchall()
        return jsonify(rows)
    finally:
        conn.close()

@app.route('/api/refresh', methods=['POST', 'OPTIONS'])
def refresh_prices():
    """
    手动触发更新所有持仓股票的最新价格，并记录快照
    """
    if request.method == 'OPTIONS':
        return '', 200  # 预检请求直接返回成功
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM positions")
            positions = cursor.fetchall()

        for pos in positions:
            stock_code = pos['stock_code']
            new_price = get_latest_price(stock_code)
            if new_price is None:
                continue  # 跳过无法获取的股票

            # 更新持仓中的最新价格
            market_value = pos['quantity'] * new_price
            profit = market_value - pos['quantity'] * pos['avg_cost']
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE positions SET latest_price=%s, market_value=%s, profit=%s WHERE stock_code=%s
                """, (new_price, market_value, profit, stock_code))

                # 记录快照
                cursor.execute("""
                    INSERT INTO position_history (stock_code, snapshot_time, quantity, price, market_value, profit, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (stock_code, datetime.now(), pos['quantity'], new_price, market_value, profit, 'price refresh'))

        conn.commit()
        return jsonify({'message': 'refresh completed'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
