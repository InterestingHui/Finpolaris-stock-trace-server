"""回测验证脚本 — 用历史信号验证信号模型的效果

用法：
  python backtest_signal_model.py                    # 用 Assignment 的 10 条信号回测
  python backtest_signal_model.py --strategy S001     # 用 MySQL 历史交易数据回测
  python backtest_signal_model.py --date-range 2026-04-01 2026-04-30  # 指定日期范围回测
"""

import sys
import os
import argparse
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from technical_indicators import compute_technical_profile, _get_db
from signal_scorer import compute_composite_score

# Assignment 验证的 10 条 Top BUY 信号（2026-03-24~27）
ASSIGNMENT_SIGNALS = [
    {'stock_code': '002049.SZ', 'stock_name': '紫光国微', 'date': '2026-03-24', 'actual_return': 0.73, 'correct': True},
    {'stock_code': '002049.SZ', 'stock_name': '紫光国微', 'date': '2026-03-25', 'actual_return': 1.73, 'correct': True},
    {'stock_code': '300661.SZ', 'stock_name': '圣邦股份', 'date': '2026-03-24', 'actual_return': 3.59, 'correct': True},
    {'stock_code': '300661.SZ', 'stock_name': '圣邦股份', 'date': '2026-03-25', 'actual_return': 1.01, 'correct': True},
    {'stock_code': '300496.SZ', 'stock_name': '中科创达', 'date': '2026-03-24', 'actual_return': -1.20, 'correct': False},
    {'stock_code': '300496.SZ', 'stock_name': '中科创达', 'date': '2026-03-25', 'actual_return': -2.30, 'correct': False},
    {'stock_code': '300496.SZ', 'stock_name': '中科创达', 'date': '2026-03-26', 'actual_return': -0.80, 'correct': False},
    {'stock_code': '300496.SZ', 'stock_name': '中科创达', 'date': '2026-03-27', 'actual_return': -1.50, 'correct': False},
    {'stock_code': '300024.SZ', 'stock_name': '机器人', 'date': '2026-03-24', 'actual_return': -0.60, 'correct': False},
    {'stock_code': '002049.SZ', 'stock_name': '紫光国微', 'date': '2026-03-26', 'actual_return': -4.26, 'correct': False},
]


def _get_t1_return(stock_code: str, signal_date: date, holding_days: int = 1):
    """从 stock_daily_cache 获取信号日之后的实际收益"""
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT trade_date, close FROM stock_daily_cache
                WHERE stock_code = %s AND trade_date > %s
                ORDER BY trade_date LIMIT %s
            """, (stock_code, signal_date, holding_days + 1))
            rows = cursor.fetchall()
            if len(rows) < 2:
                return None, None

            entry_close = float(rows[0]['close'])
            exit_close = float(rows[holding_days]['close']) if len(rows) > holding_days else float(rows[-1]['close'])
            ret = (exit_close - entry_close) / entry_close * 100
            correct = ret > 0
            return round(ret, 4), correct
    except Exception:
        return None, None
    finally:
        conn.close()


def _print_results(results, title):
    print('=' * 100)
    print(f'回测：{title}')
    print('=' * 100)
    print()

    print(f"{'代码':<12} {'名称':<8} {'日期':<12} {'实际%':>7} {'RSI':>6} {'量比':>5} "
          f"{'MA趋势':<8} {'技术分':>6} {'复合分':>7} {'舆情趋势':<6} {'连续罚':>5} {'过滤':<6} {'对错'}")
    print('-' * 120)

    for r in results:
        if not r.get('data_available'):
            print(f"{r['stock_code']:<12} {r['stock_name']:<8} {r['date']:<12} "
                  f"{'N/A':>7} --- 数据不足 ---")
            continue
        filter_mark = '拒绝' if r['rejected'] else '通过'
        correct_mark = 'O' if r.get('correct') else 'X'
        actual = r.get('actual_return')
        actual_str = f'{actual:>+6.2f}%' if actual is not None else '  N/A '
        trend_str = r.get('sentiment_trend', '-')[:6]
        consec_str = f"{r.get('consecutive_penalty', 0):.0f}"
        print(f"{r['stock_code']:<12} {r['stock_name']:<8} {r['date']:<12} "
              f"{actual_str} {r['rsi']:>6.1f} {r['volume_ratio']:>5.2f} "
              f"{r['ma_trend']:<8} {r['technical_score']:>6.1f} {r['composite_score']:>7.1f} "
              f"{trend_str:<6} {consec_str:>5} {filter_mark:<6} {correct_mark}")

    available = [r for r in results if r.get('data_available')]
    rejected = [r for r in available if r['rejected']]
    passed = [r for r in available if not r['rejected']]

    print()
    print(f"统计：")
    print(f"  总信号数: {len(results)}")
    print(f"  数据可用: {len(available)}")
    print(f"  被拒绝:   {len(rejected)}")
    print(f"  通过:     {len(passed)}")

    if rejected:
        correct_rej = sum(1 for r in rejected if r.get('correct'))
        wrong_rej = sum(1 for r in rejected if not r.get('correct'))
        print(f"\n  被拒绝的信号:")
        for r in rejected:
            print(f"    {'O' if r.get('correct') else 'X'} {r.get('stock_name', r['stock_code'])} "
                  f"{r['date']}: {r['rejection_reason']}")
        print(f"  正确信号被误拒: {correct_rej}, 错误信号被正确拒绝: {wrong_rej}")

    if passed:
        correct_passed = [r for r in passed if r.get('correct')]
        wrong_passed = [r for r in passed if not r.get('correct')]
        returns = [r['actual_return'] for r in passed if r.get('actual_return') is not None]
        avg_ret = sum(returns) / len(returns) if returns else 0
        acc = len(correct_passed) / len(passed) * 100
        print(f"\n  通过信号准确率: {len(correct_passed)}/{len(passed)} = {acc:.0f}%")
        print(f"  平均收益: {avg_ret:+.2f}%")
        if returns:
            print(f"  最大收益: {max(returns):+.2f}%")
            print(f"  最大亏损: {min(returns):+.2f}%")


def backtest_assignment():
    backtest_signals(ASSIGNMENT_SIGNALS, 'Assignment 10 条 Top BUY 信号', raw_score=200.0)


def backtest_signals(signals, title, raw_score=100.0):
    results = []
    for sig in signals:
        trade_dt = datetime.strptime(sig['date'], '%Y-%m-%d').date() if isinstance(sig['date'], str) else sig['date']
        score = sig.get('raw_score', raw_score)

        profile = compute_technical_profile(sig['stock_code'], trade_dt)
        scoring = compute_composite_score(score, profile, stock_code=sig['stock_code'], trade_date=trade_dt)

        actual_ret, correct = _get_t1_return(sig['stock_code'], trade_dt)
        if actual_ret is None and 'actual_return' in sig:
            actual_ret = sig['actual_return']
            correct = sig.get('correct', actual_ret > 0)

        results.append({
            **sig,
            'rsi': profile.get('rsi', 50),
            'volume_ratio': profile.get('volume_ratio', 1.0),
            'ma_trend': profile.get('ma_trend', 'neutral'),
            'momentum': profile.get('momentum_5d_pct', 0),
            'technical_score': scoring['technical_score'],
            'composite_score': scoring['composite_score'],
            'sentiment_trend': scoring.get('sentiment_trend', '-'),
            'consecutive_penalty': scoring.get('consecutive_penalty', 0),
            'rejected': scoring['rejected'],
            'rejection_reason': scoring.get('rejection_reason', ''),
            'data_available': profile.get('data_available', False),
            'actual_return': actual_ret,
            'correct': correct,
        })

    _print_results(results, title)


def backtest_strategy(strategy_id):
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT stock_code, trade_date, action, price, quantity
                FROM trades
                WHERE strategy_id = %s AND action = 'buy'
                ORDER BY trade_date
            """, (strategy_id,))
            trades = cursor.fetchall()
    finally:
        conn.close()

    if not trades:
        print(f"策略 {strategy_id} 无历史交易记录")
        return

    signals = []
    for t in trades:
        trade_dt = t['trade_date']
        if isinstance(trade_dt, datetime):
            trade_dt = trade_dt.date()
        signals.append({
            'stock_code': t['stock_code'],
            'stock_name': t['stock_code'],
            'date': trade_dt,
            'raw_score': 100.0,
        })

    backtest_signals(signals, f'策略 {strategy_id} ({len(trades)} 条历史 BUY 交易)')


def backtest_date_range(start_date, end_date):
    """从 omni_strategy_trade_signal 加载指定日期范围的信号回测"""
    conn = _get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT strategy_id, stock_code, trade_date, score
                FROM omni_strategy_trade_signal
                WHERE trade_date BETWEEN %s AND %s
                  AND signal_type = 'buy'
                ORDER BY trade_date, score DESC
            """, (start_date, end_date))
            rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"日期范围 {start_date} ~ {end_date} 无 BUY 信号")
        return

    signals = []
    for r in rows:
        td = r['trade_date']
        if isinstance(td, datetime):
            td = td.date()
        signals.append({
            'stock_code': r['stock_code'],
            'stock_name': r['stock_code'],
            'date': td.strftime('%Y-%m-%d') if isinstance(td, date) else str(td),
            'raw_score': float(r.get('score', 100) or 100),
        })

    backtest_signals(signals, f'日期范围 {start_date} ~ {end_date} ({len(signals)} 条 BUY 信号)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Finpolaris 信号模型回测')
    parser.add_argument('--strategy', help='用 MySQL 历史交易回测的策略 ID')
    parser.add_argument('--date-range', nargs=2, metavar=('START', 'END'),
                        help='日期范围回测 (YYYY-MM-DD YYYY-MM-DD)')
    args = parser.parse_args()

    if args.strategy:
        backtest_strategy(args.strategy)
    elif args.date_range:
        backtest_date_range(args.date_range[0], args.date_range[1])
    else:
        backtest_assignment()
