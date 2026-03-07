import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day
from history_store import get_all_histories, get_swing_points
from market_context import get_context

PROXIMITY_PCT = 0.5
MIN_TOUCHES   = 3
MAX_SWING_PTS = 6

def check_bounce_trigger(df, level):
    if len(df) < 2:
        return False
    return (df.iloc[-2]['low']   <= level * (1 + PROXIMITY_PCT / 100) and
            df.iloc[-1]['close'] >  level)

def _price_near_lows(df, sym, exchange, interval, pct=3.0):
    """Uses precomputed swing lows — no rolling needed."""
    _, low_idxs = get_swing_points(sym, exchange, interval)
    if not low_idxs:
        return False
    lows    = df['low'].values
    current = float(df.iloc[-1]['close'])
    recent  = [lows[i] for i in low_idxs[-10:] if i < len(lows)]
    if not recent:
        return False
    return bool(np.any(np.abs(np.array(recent) - current) / current * 100 <= pct))

def find_horizontal_supports(df, sym, exchange, interval):
    _, low_idxs = get_swing_points(sym, exchange, interval)
    low_idxs    = [i for i in low_idxs if i < len(df)]
    if len(low_idxs) < MIN_TOUCHES:
        return []
    lows       = df['low'].values
    swing_lows = [(i, lows[i]) for i in low_idxs]
    supports   = []
    used       = set()
    for i, (idx_i, low_i) in enumerate(swing_lows):
        if i in used:
            continue
        group = [(idx_i, low_i)]
        for j, (idx_j, low_j) in enumerate(swing_lows):
            if j == i or j in used:
                continue
            if abs(low_i - low_j) / low_i * 100 <= PROXIMITY_PCT:
                group.append((idx_j, low_j))
                used.add(j)
        if len(group) >= MIN_TOUCHES:
            supports.append({
                'level':   round(sum(l for _, l in group) / len(group), 2),
                'touches': len(group),
            })
        used.add(i)
    return supports

def find_rising_trendlines(df, sym, exchange, interval):
    _, low_idxs = get_swing_points(sym, exchange, interval)
    low_idxs    = [i for i in low_idxs if i < len(df)]
    if len(low_idxs) < 3:
        return []
    low_idxs   = low_idxs[-MAX_SWING_PTS:]
    lows       = df['low'].values
    n          = len(df)
    trendlines = []
    for i in range(len(low_idxs)):
        for j in range(i+1, len(low_idxs)):
            for k in range(j+1, len(low_idxs)):
                idx1, idx2, idx3 = low_idxs[i], low_idxs[j], low_idxs[k]
                low1, low2, low3 = lows[idx1], lows[idx2], lows[idx3]
                if not (low3 > low2 > low1):
                    continue
                slope     = (low3 - low1) / (idx3 - idx1)
                intercept = low1 - slope * idx1
                mid_val   = slope * idx2 + intercept
                if abs(low2 - mid_val) / mid_val * 100 > PROXIMITY_PCT:
                    continue
                projected = slope * (n - 1) + intercept
                if projected <= 0:
                    continue
                check_range = np.arange(idx1, idx3 + 1)
                line_vals   = slope * check_range + intercept
                if np.any(lows[idx1:idx3+1] < line_vals * (1 - PROXIMITY_PCT / 100)):
                    continue
                trendlines.append({
                    'projected_value': round(projected, 2),
                    'slope':           round(slope, 4),
                    'strength':        k - i
                })
    trendlines.sort(key=lambda x: -x['strength'])
    return trendlines[:3]

def run_trendline_scan(exchange='BOTH'):
    day = get_last_trading_day()
    print(f"\n=== Trendline / Support Scan | {day} | Exchange: {exchange} ===\n")

    results  = []
    contexts = get_context(exchange)

    for exch, ctx in contexts.items():
        if ctx is None or ctx.daily is None:
            print(f"[{exch}] No context available, skipping.")
            continue

        print(f"--- {exch} ---")
        daily_map = {
            row['symbol']: row
            for _, row in ctx.daily.iterrows()
        }

        flagged = 0
        skipped = 0

        # Accumulate setups per symbol across both timeframes
        sym_setups = {}

        for interval, label in [('1d', 'Daily'), ('1wk', 'Weekly')]:
            histories = get_all_histories(exch, interval)

            for sym, df in histories.items():
                if df is None or len(df) < 10:
                    continue
                if daily_map.get(sym) is None:
                    continue

                if not _price_near_lows(df, sym, exch, interval):
                    skipped += 1
                    continue

                for sup in find_horizontal_supports(df, sym, exch, interval):
                    if check_bounce_trigger(df, sup['level']):
                        sym_setups.setdefault(sym, []).append({
                            'type':    f'Horizontal Support ({label})',
                            'level':   sup['level'],
                            'touches': sup['touches'],
                            'detail':  f"{sup['touches']} touches at ₹{sup['level']}"
                        })

                for tl in find_rising_trendlines(df, sym, exch, interval):
                    proj = tl['projected_value']
                    if check_bounce_trigger(df, proj):
                        sym_setups.setdefault(sym, []).append({
                            'type':    f'Rising Trendline ({label})',
                            'level':   proj,
                            'touches': 2,
                            'detail':  f"Projected ₹{proj}, slope {tl['slope']}"
                        })

        for sym, setups in sym_setups.items():
            today = daily_map[sym]
            best  = sorted(setups, key=lambda x: -x['touches'])[0]
            results.append({
                'Symbol':     sym,
                'Exchange':   exch,
                'Price':      round(float(today['close']), 2),
                'Direction':  '🟢 Long',
                'Setup':      best['type'],
                'Level':      best['level'],
                'Detail':     best['detail'],
                'All Setups': ' | '.join(s['type'] for s in setups)
            })
            flagged += 1

        print(f"[{exch}] {len(daily_map)} stocks — {flagged} flagged, "
              f"{skipped} skipped by pre-filter.\n")

    if not results:
        print("No trendline/support setups found.")
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values('Symbol').reset_index(drop=True)

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc
    from history_store import preload_histories
    day     = get_last_trading_day()
    symbols = get_nse_ohlc(day)['symbol'].tolist()
    preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=60)
    t0      = time.time()
    results = run_trendline_scan(exchange='NSE')
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(results[['Symbol','Exchange','Price','Setup','Level','Detail']].to_string(index=False))