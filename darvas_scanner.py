import pandas as pd
from datetime import datetime, timedelta
from data_fetcher import get_last_trading_day
from history_store import get_history
from market_context import get_context

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

MIN_BOX_WEEKS     = 3
MAX_BOX_AGE_WEEKS = 10
RETEST_PCT        = 1.0
LOOKBACK_WEEKS    = 52

# ─────────────────────────────────────────
# FETCH HISTORY
# ─────────────────────────────────────────

def fetch_weekly_history(symbol, exchange):
    df = get_history(symbol, exchange, interval='1wk')
    if df is None or df.empty:
        return None
    return df.iloc[-52:]  # slice only, no copy

def get_prev_close(symbol, exchange):
    df = get_history(symbol, exchange, interval='1d')
    if df is None or df.empty or len(df) < 2:
        return None
    val = df.iloc[-2]['close']
    # Guard against duplicate index producing a Series instead of scalar
    if hasattr(val, '__len__'):
        val = val.iloc[-1]
    return float(val)

# ─────────────────────────────────────────
# ACCUMULATION BOX DETECTION
# ─────────────────────────────────────────

def find_accumulation_boxes(weekly_df):
    if weekly_df is None or len(weekly_df) < MIN_BOX_WEEKS + 1:
        return []

    boxes  = []
    highs  = weekly_df['high'].values
    lows   = weekly_df['low'].values
    dates  = weekly_df.index.tolist()
    n      = len(highs)
    cutoff = pd.Timestamp.today(tz='UTC') - timedelta(weeks=MAX_BOX_AGE_WEEKS)

    i = 0
    while i < n - MIN_BOX_WEEKS:
        lookback_start = max(0, i - 10)
        if highs[i] <= max(highs[lookback_start:i], default=0):
            i += 1
            continue
        box_top      = highs[i]
        box_start    = dates[i]
        box_start_ts = pd.Timestamp(box_start)
        if box_start_ts.tz is None:
            box_start_ts = box_start_ts.tz_localize('UTC')
        if box_start_ts < cutoff:
            i += 1
            continue
        j = i + 1
        while j < n and highs[j] < box_top:
            j += 1
        consol_weeks = j - i - 1
        if consol_weeks >= MIN_BOX_WEEKS:
            boxes.append({
                'type':       'Accumulation',
                'box_top':    round(box_top, 2),
                'box_bottom': round(min(lows[i:j]), 2),
                'box_start':  box_start,
                'box_end':    dates[j - 1],
                'weeks':      consol_weeks
            })
            i = j
        else:
            i += 1
    return boxes

# ─────────────────────────────────────────
# DISTRIBUTION BOX DETECTION
# ─────────────────────────────────────────

def find_distribution_boxes(weekly_df):
    if weekly_df is None or len(weekly_df) < MIN_BOX_WEEKS + 1:
        return []

    boxes  = []
    highs  = weekly_df['high'].values
    lows   = weekly_df['low'].values
    dates  = weekly_df.index.tolist()
    n      = len(lows)
    cutoff = pd.Timestamp.today(tz='UTC') - timedelta(weeks=MAX_BOX_AGE_WEEKS)

    i = 0
    while i < n - MIN_BOX_WEEKS:
        lookback_start = max(0, i - 10)
        if lows[i] >= min(lows[lookback_start:i], default=float('inf')):
            i += 1
            continue
        box_bottom   = lows[i]
        box_start    = dates[i]
        box_start_ts = pd.Timestamp(box_start)
        if box_start_ts.tz is None:
            box_start_ts = box_start_ts.tz_localize('UTC')
        if box_start_ts < cutoff:
            i += 1
            continue
        j = i + 1
        while j < n and lows[j] > box_bottom:
            j += 1
        consol_weeks = j - i - 1
        if consol_weeks >= MIN_BOX_WEEKS:
            boxes.append({
                'type':       'Distribution',
                'box_top':    round(max(highs[i:j]), 2),
                'box_bottom': round(box_bottom, 2),
                'box_start':  box_start,
                'box_end':    dates[j - 1],
                'weeks':      consol_weeks
            })
            i = j
        else:
            i += 1
    return boxes

# ─────────────────────────────────────────
# TRIGGER CHECKS
# ─────────────────────────────────────────

def check_long_trigger(current_price, today_high, prev_close, box):
    box_top = box['box_top']
    if current_price > box_top:
        if prev_close is not None and prev_close <= box_top * 1.03:
            return 'Breakout'
        if abs(current_price - box_top) / box_top * 100 <= RETEST_PCT:
            return 'Retest'
    return None

def check_short_trigger(current_price, today_low, prev_close, box):
    box_bottom = box['box_bottom']
    if not (current_price < box_bottom or today_low < box_bottom):
        return None
    was_inside = prev_close >= box_bottom if prev_close is not None else True
    if not was_inside:
        return None
    return 'Distribution Breakdown' if box['type'] == 'Distribution' else 'Box Breakdown'

def check_distribution_retest(current_price, box):
    box_bottom = box['box_bottom']
    if current_price >= box_bottom:
        return None
    if abs(current_price - box_bottom) / box_bottom * 100 <= RETEST_PCT:
        return 'Breakdown Retest'
    return None

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_darvas_scan(exchange='ALL', direction='BOTH'):
    day = get_last_trading_day()
    print(f"\n=== Darvas Box Scan | {day} | Exchange: {exchange} | Direction: {direction} ===\n")

    results  = []
    contexts = get_context('ALL')

    for exch, ctx in contexts.items():
        if ctx is None or ctx.daily is None:
            print(f"[{exch}] No context available, skipping.")
            continue

        print(f"--- {exch} ---")
        total   = len(ctx.daily)
        flagged = 0

        for row_num, (idx, stock) in enumerate(ctx.daily.iterrows()):
            sym           = stock['symbol']
            current_price = stock['close']
            today_high    = stock['high']
            today_low     = stock['low']
            prev_close    = get_prev_close(sym, exch)

            weekly = fetch_weekly_history(sym, exch)
            if weekly is None or len(weekly) < MIN_BOX_WEEKS + 1:
                continue

            # ── LONG ──
            if direction in ('LONG', 'BOTH'):
                acc_boxes = find_accumulation_boxes(weekly)
                if acc_boxes:
                    box     = acc_boxes[-1]
                    trigger = check_long_trigger(current_price, today_high, prev_close, box)
                    if trigger:
                        results.append({
                            'Symbol':      sym,
                            'Exchange':    exch,
                            'Direction':   '🟢 Long',
                            'Box Type':    'Accumulation',
                            'Price':       round(current_price, 2),
                            'Trigger':     trigger,
                            'Box Top':     box['box_top'],
                            'Box Bottom':  box['box_bottom'],
                            'Box Width %': round((box['box_top'] - box['box_bottom']) / box['box_bottom'] * 100, 2),
                            'Box Weeks':   box['weeks'],
                            'Box Start':   str(box['box_start'])[:10],
                            'Box End':     str(box['box_end'])[:10],
                        })
                        flagged += 1

            # ── SHORT ──
            if direction in ('SHORT', 'BOTH'):
                acc_boxes = find_accumulation_boxes(weekly)
                if acc_boxes:
                    box     = acc_boxes[-1]
                    trigger = check_short_trigger(current_price, today_low, prev_close, box)
                    if trigger:
                        results.append({
                            'Symbol':      sym,
                            'Exchange':    exch,
                            'Direction':   '🔴 Short',
                            'Box Type':    'Accumulation Box ↓',
                            'Price':       round(current_price, 2),
                            'Trigger':     trigger,
                            'Box Top':     box['box_top'],
                            'Box Bottom':  box['box_bottom'],
                            'Box Width %': round((box['box_top'] - box['box_bottom']) / box['box_bottom'] * 100, 2),
                            'Box Weeks':   box['weeks'],
                            'Box Start':   str(box['box_start'])[:10],
                            'Box End':     str(box['box_end'])[:10],
                        })
                        flagged += 1

                dist_boxes = find_distribution_boxes(weekly)
                if dist_boxes:
                    box     = dist_boxes[-1]
                    trigger = check_short_trigger(current_price, today_low, prev_close, box)
                    if not trigger:
                        trigger = check_distribution_retest(current_price, box)
                    if trigger:
                        results.append({
                            'Symbol':      sym,
                            'Exchange':    exch,
                            'Direction':   '🔴 Short',
                            'Box Type':    'Distribution Box ↓',
                            'Price':       round(current_price, 2),
                            'Trigger':     trigger,
                            'Box Top':     box['box_top'],
                            'Box Bottom':  box['box_bottom'],
                            'Box Width %': round((box['box_top'] - box['box_bottom']) / box['box_bottom'] * 100, 2),
                            'Box Weeks':   box['weeks'],
                            'Box Start':   str(box['box_start'])[:10],
                            'Box End':     str(box['box_end'])[:10],
                        })
                        flagged += 1

            if (row_num + 1) % 200 == 0:
                print(f"  ... {row_num+1}/{total} scanned, {flagged} flagged so far")

        print(f"[{exch}] Scanned {total} stocks — {flagged} flagged.\n")

    if not results:
        print("No Darvas setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    order = {'Breakout': 0, 'Retest': 1, 'Box Breakdown': 2,
             'Distribution Breakdown': 3, 'Breakdown Retest': 4}
    df['_sort'] = df['Trigger'].map(order).fillna(5)
    df = df.sort_values(['Direction', '_sort', 'Box Width %'],
                        ascending=[True, True, True]).drop(columns=['_sort'])
    return df.reset_index(drop=True)

if __name__ == "__main__":
    results = run_darvas_scan(exchange='NSE', direction='BOTH')
    if not results.empty:
        print(f"Total Darvas setups: {len(results)}\n")
        print(results[['Symbol','Exchange','Direction','Box Type','Price',
                        'Trigger','Box Top','Box Bottom',
                        'Box Width %','Box Weeks']].to_string(index=False))
    else:
        print("No results.")