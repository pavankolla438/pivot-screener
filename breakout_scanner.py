import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day
from history_store import get_all_histories, get_swing_points
from market_context import get_context

LOOKBACK_BARS = 34
SWING_WINDOW  = 3
MAX_BREAK_PCT = 20.0
GAP_PCT       = 2.0

# ─────────────────────────────────────────
# SWING LEVELS FROM PRECOMPUTED CACHE
# ─────────────────────────────────────────

def get_swing_resistance(df, sym, exchange, interval):
    _, high_idxs = get_swing_points(sym, exchange, interval)
    hist         = df.iloc[:-1]
    valid_idx    = [i for i in high_idxs if i < len(hist)]
    if not valid_idx:
        return float(hist['high'].max()) if not hist.empty else None
    return float(hist['high'].iloc[valid_idx[-1]])

def get_swing_support(df, sym, exchange, interval):
    _, low_idxs = get_swing_points(sym, exchange, interval)
    hist        = df.iloc[:-1]
    valid_idx   = [i for i in low_idxs if i < len(hist)]
    if not valid_idx:
        return float(hist['low'].min()) if not hist.empty else None
    return float(hist['low'].iloc[valid_idx[-1]])

# ─────────────────────────────────────────
# GAP DETECTION
# ─────────────────────────────────────────

def check_gap(df, daily_row):
    """
    Gap Up:   today open > yesterday close by GAP_PCT%+
    Gap Down: today open < yesterday close by GAP_PCT%+
    """
    if len(df) < 2:
        return None
    if 'open' not in daily_row.index:
        return None

    today_open  = float(daily_row['open'])
    today_close = float(daily_row['close'])
    prev_close  = float(df.iloc[-2]['close'])

    if prev_close == 0:
        return None

    gap_pct = (today_open - prev_close) / prev_close * 100

    if gap_pct >= GAP_PCT:
        continuation = today_close >= today_open * 0.99
        return {
            'type':         'Gap Up',
            'gap_pct':      round(gap_pct, 2),
            'continuation': '▲ Continuing' if continuation else '▼ Filling',
        }
    elif gap_pct <= -GAP_PCT:
        continuation = today_close <= today_open * 1.01
        return {
            'type':         'Gap Down',
            'gap_pct':      round(gap_pct, 2),
            'continuation': '▼ Continuing' if continuation else '▲ Filling',
        }
    return None

# ─────────────────────────────────────────
# SCAN ONE INTERVAL
# ─────────────────────────────────────────

def scan_interval(histories, daily_map, exchange, interval, interval_label, direction):
    hits = []
    for sym, df in histories.items():
        if df is None or len(df) < 7:
            continue
        today = daily_map.get(sym)
        if today is None:
            continue

        today_high  = today['high']
        today_low   = today['low']

        # ── LONG / BREAKOUT ──
        if direction in ('LONG', 'BOTH'):
            resistance = get_swing_resistance(df, sym, exchange, interval)
            if resistance and today_high > resistance:
                prev_high = float(df.iloc[-2]['high'])
                if prev_high <= resistance:
                    trigger = 'Fresh Breakout'
                elif np.any(df.iloc[-4:-1]['close'].values < resistance):
                    trigger = 'Breakout'
                else:
                    trigger = None
                if trigger:
                    pct = round((today_high - resistance) / resistance * 100, 2)
                    if pct <= MAX_BREAK_PCT:
                        hits.append({
                            'sym':      sym,
                            'side':     'LONG',
                            'label':    interval_label,
                            'level':    round(resistance, 2),
                            'move_pct': pct,
                            'trigger':  trigger,
                            'is_gap':   False,
                            'gap_pct':  '',
                        })

        # ── SHORT / BREAKDOWN ──
        if direction in ('SHORT', 'BOTH'):
            support = get_swing_support(df, sym, exchange, interval)
            if support and today_low < support:
                prev_low = float(df.iloc[-2]['low'])
                if prev_low >= support:
                    trigger = 'Fresh Breakdown'
                elif np.any(df.iloc[-4:-1]['close'].values > support):
                    trigger = 'Breakdown'
                else:
                    trigger = None
                if trigger:
                    pct = round((support - today_low) / support * 100, 2)
                    if pct <= MAX_BREAK_PCT:
                        hits.append({
                            'sym':      sym,
                            'side':     'SHORT',
                            'label':    interval_label,
                            'level':    round(support, 2),
                            'move_pct': pct,
                            'trigger':  trigger,
                            'is_gap':   False,
                            'gap_pct':  '',
                        })

        # ── GAP CHECK ──
        gap = check_gap(df, today)
        if gap:
            hits.append({
                'sym':      sym,
                'side':     'LONG' if gap['type'] == 'Gap Up' else 'SHORT',
                'label':    interval_label,
                'level':    round(float(df.iloc[-2]['close']), 2),
                'move_pct': abs(gap['gap_pct']),
                'trigger':  f"{gap['type']} {gap['continuation']}",
                'is_gap':   True,
                'gap_pct':  gap['gap_pct'],
            })

    return hits

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_breakout_scan(exchange='BOTH', direction='BOTH'):
    day = get_last_trading_day()
    print(f"\n=== Breakout/Breakdown Scan | {day} | Exchange: {exchange} | Direction: {direction} ===\n")

    results  = []
    contexts = get_context(exchange)

    for exch, ctx in contexts.items():
        if ctx is None or ctx.daily is None:
            continue

        print(f"--- {exch} ---")

        daily_map = {
            row['symbol']: row
            for _, row in ctx.daily.iterrows()
        }

        all_hits = {}  # { (sym, side): [hits] }

        for interval, label in [('1d', 'Daily'), ('1wk', 'Weekly')]:
            histories = get_all_histories(exch, interval)
            hits      = scan_interval(histories, daily_map, exch, interval, label, direction)
            for h in hits:
                key = (h['sym'], h['side'])
                all_hits.setdefault(key, []).append(h)

        flagged = 0
        for (sym, side), hit_list in all_hits.items():
            today    = daily_map[sym]
            best     = max(hit_list, key=lambda x: x['move_pct'])
            labels   = ' + '.join(dict.fromkeys(h['label']   for h in hit_list))
            triggers = ' + '.join(dict.fromkeys(h['trigger'] for h in hit_list))
            levels   = ' / '.join(f"₹{h['level']} ({h['label']})" for h in hit_list)
            gap_pct  = next((h['gap_pct'] for h in hit_list if h.get('is_gap')), '')
            results.append({
                'Symbol':    sym,
                'Exchange':  exch,
                'Direction': '🟢 Long' if side == 'LONG' else '🔴 Short',
                'Price':     round(float(today['close']), 2),
                'Trigger':   triggers,
                'Timeframe': labels,
                'Level':     levels,
                'Move %':    best['move_pct'],
                'Both TF':   '⭐ Yes' if len(hit_list) == 2 else 'No',
                'Gap %':     gap_pct,
            })
            flagged += 1

        print(f"[{exch}] {len(daily_map)} stocks — {flagged} flagged.\n")

    if not results:
        print("No setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df['_sort'] = df['Both TF'].apply(lambda x: 0 if '⭐' in x else 1)
    df = df.sort_values(['Direction', '_sort', 'Move %'],
                        ascending=[True, True, False]).drop(columns=['_sort'])
    return df.reset_index(drop=True)

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc
    from history_store import preload_histories
    day     = get_last_trading_day()
    symbols = get_nse_ohlc(day)['symbol'].tolist()
    preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=60)
    t0      = time.time()
    results = run_breakout_scan(exchange='NSE', direction='BOTH')
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(results[['Symbol','Exchange','Direction','Price',
                        'Trigger','Timeframe','Move %','Both TF','Gap %']].to_string(index=False))