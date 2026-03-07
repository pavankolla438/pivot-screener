import pandas as pd
from data_fetcher import get_last_trading_day
from history_store import get_history
from market_context import get_context

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

DEFAULT_N = 2
MIN_N     = 2
MAX_N     = 5

# ─────────────────────────────────────────
# FETCH HISTORY
# ─────────────────────────────────────────

def fetch_history(symbol, exchange, interval='1d'):
    df = get_history(symbol, exchange, interval=interval)
    if df is None or df.empty:
        return None
    return df  # no copy needed — read only

# ─────────────────────────────────────────
# CORE PATTERN DETECTION
# ─────────────────────────────────────────

def find_inside_bar_setup(df, n=DEFAULT_N):
    """
    n = number of bars back for mother bar (2 to 5)

    Example n=2:
      df.iloc[-(n+1)] = mother bar
      df.iloc[-n:-1]  = inside bars (must all be inside mother)
      df.iloc[-1]     = today — one of 4 states

    4 trigger states:
      Breakout  — today closed ABOVE mother high
      Breakdown — today closed BELOW mother low
      Attempt   — today crossed high or low intraday but closed back inside
      Baby      — today fully inside mother (no violation at all)
    """
    if df is None or len(df) < n + 1:
        return None

    mother     = df.iloc[-(n + 1)]
    today      = df.iloc[-1]
    in_between = df.iloc[-n:-1]

    mother_high = mother['high']
    mother_low  = mother['low']
    mother_date = str(df.index[-(n + 1)])[:10]

    # All bars between mother and today must be strictly inside mother
    for _, bar in in_between.iterrows():
        if bar['high'] > mother_high or bar['low'] < mother_low:
            return None  # sequence broken

    today_high  = today['high']
    today_low   = today['low']
    today_close = today['close']

    # ── 4 states ──
    if today_close > mother_high:
        trigger   = 'Breakout'
        direction = 'Long'
    elif today_close < mother_low:
        trigger   = 'Breakdown'
        direction = 'Short'
    elif today_high > mother_high or today_low < mother_low:
        trigger   = 'Attempt'
        direction = 'Neutral'
    else:
        trigger   = 'Baby'
        direction = 'Neutral'

    return {
        'mother_date':  mother_date,
        'mother_high':  round(mother_high, 2),
        'mother_low':   round(mother_low, 2),
        'inside_count': len(in_between),
        'trigger':      trigger,
        'direction':    direction,
        'today_close':  round(today_close, 2),
        'today_high':   round(today_high, 2),
        'today_low':    round(today_low, 2),
    }

# ─────────────────────────────────────────
# CONFLUENCE DETECTION
# ─────────────────────────────────────────

def is_near(price, level, pct=1.0):
    if level is None or level == 0:
        return False
    return abs(price - level) / level * 100 <= pct

def check_confluence(symbol, exchange, mother_high, mother_low, current_price,
                     weekly_pivots=None, monthly_pivots=None):
    tags = []

    # ── 1. CPR Confluence ──
    # weekly_pivots / monthly_pivots are pre-indexed DataFrames (symbol → pivot row)
    # computed once in run_inside_bar_scan — zero I/O here, just dict lookup.
    for tf_label, pivots_df in [
        ('Weekly CPR',  weekly_pivots),
        ('Monthly CPR', monthly_pivots),
    ]:
        try:
            if pivots_df is None or symbol not in pivots_df.index:
                continue
            p_row = pivots_df.loc[symbol]
            for level_name in ['P', 'TC', 'BC']:
                level = p_row.get(level_name)
                if level and (is_near(mother_high, level) or
                              is_near(mother_low,  level) or
                              is_near(current_price, level)):
                    tags.append(tf_label)
                    break
        except Exception:
            pass

    # ── 2. Darvas Box Confluence ──
    try:
        from darvas_scanner import fetch_weekly_history, find_accumulation_boxes, find_distribution_boxes
        weekly = fetch_weekly_history(symbol, exchange)
        if weekly is not None:
            for box_fn in [find_accumulation_boxes, find_distribution_boxes]:
                boxes = box_fn(weekly)
                if boxes:
                    box = boxes[-1]
                    for lvl in [box['box_top'], box['box_bottom']]:
                        if (is_near(mother_high, lvl) or
                            is_near(mother_low,  lvl) or
                            is_near(current_price, lvl)):
                            tags.append('Darvas Level')
                            break
    except Exception:
        pass

    # ── 3. Horizontal Support/Resistance ──
    try:
        from trendline_scanner import fetch_history as tl_fetch, find_horizontal_supports
        daily_df = tl_fetch(symbol, exchange, interval='1d')
        if daily_df is not None:
            supports = find_horizontal_supports(daily_df)
            for sup in supports:
                level = sup['level']
                if (is_near(mother_high, level) or
                    is_near(mother_low,  level) or
                    is_near(current_price, level)):
                    tags.append('H. Support')
                    break
    except Exception:
        pass

    return list(dict.fromkeys(tags))

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_inside_bar_scan(exchange='ALL', direction='BOTH', n=DEFAULT_N):
    n   = max(MIN_N, min(MAX_N, int(n)))
    day = get_last_trading_day()
    print(f"\n=== Inside Bar Scan | {day} | N={n} | Exchange: {exchange} ===\n")

    results  = []
    contexts = get_context('ALL')

    # Pre-compute weekly + monthly pivot tables ONCE, indexed by symbol.
    # Avoids 2×N file reads inside check_confluence (was causing 23s → ~2s).
    _weekly_pivots  = None
    _monthly_pivots = None
    try:
        from data_fetcher import get_weekly_ohlc_nse, get_monthly_ohlc_nse
        from pivot_calculator import calculate_pivots
        w_ohlc = get_weekly_ohlc_nse()
        m_ohlc = get_monthly_ohlc_nse()
        if w_ohlc is not None and not w_ohlc.empty:
            _weekly_pivots  = calculate_pivots(w_ohlc).set_index('symbol')
        if m_ohlc is not None and not m_ohlc.empty:
            _monthly_pivots = calculate_pivots(m_ohlc).set_index('symbol')
    except Exception:
        pass  # CPR tags will simply be absent — non-fatal

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
            hits          = []

            for interval, label in [('1d', 'Daily'), ('1wk', 'Weekly')]:
                df = fetch_history(sym, exch, interval=interval)
                if df is None or len(df) < n + 1:
                    continue
                setup = find_inside_bar_setup(df, n=n)
                if setup is None:
                    continue
                hits.append({**setup, 'label': label})

            if not hits:
                continue

            priority = {'Breakout': 0, 'Breakdown': 1, 'Attempt': 2, 'Baby': 3}
            best     = min(hits, key=lambda x: priority.get(x['trigger'], 9))
            both_tf  = len(hits) == 2

            dir_label = {
                'Breakout':  '🟢 Long',
                'Breakdown': '🔴 Short',
                'Attempt':   '⚡ Attempt',
                'Baby':      '🟡 Baby',
            }.get(best['trigger'], '')

            cf_tags        = check_confluence(sym, exch,
                                              best['mother_high'],
                                              best['mother_low'],
                                              current_price,
                                              weekly_pivots=_weekly_pivots,
                                              monthly_pivots=_monthly_pivots)
            confluence_str = ' + '.join(cf_tags) if cf_tags else ''

            results.append({
                'Symbol':      sym,
                'Exchange':    exch,
                'Direction':   dir_label,
                'Price':       round(current_price, 2),
                'Timeframe':   ' + '.join(h['label'] for h in hits),
                'Trigger':     best['trigger'],
                'Mother Date': best['mother_date'],
                'Mother High': best['mother_high'],
                'Mother Low':  best['mother_low'],
                'Inside Bars': best['inside_count'],
                'Confluence':  confluence_str,
                'Both TF':     '⭐ Yes' if both_tf else 'No',
                'N':           n,
            })
            flagged += 1

            if (row_num + 1) % 200 == 0:
                print(f"  ... {row_num+1}/{total} scanned, {flagged} flagged so far")

        print(f"[{exch}] Scanned {total} stocks — {flagged} flagged.\n")

    if not results:
        print("No inside bar setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    trig_order = {'Breakout': 0, 'Breakdown': 1, 'Attempt': 2, 'Baby': 3}
    df['_trig'] = df['Trigger'].map(trig_order).fillna(4)
    df['_cf']   = df['Confluence'].apply(lambda x: 0 if x else 1)
    df['_both'] = df['Both TF'].apply(lambda x: 0 if '⭐' in x else 1)
    df = df.sort_values(['_trig', '_cf', '_both'], ascending=[True, True, True])
    df = df.drop(columns=['_trig', '_cf', '_both']).reset_index(drop=True)
    return df

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time
    for n in [2, 3, 4, 5]:
        t0      = time.time()
        results = run_inside_bar_scan(exchange='NSE', direction='BOTH', n=n)
        t1      = time.time()
        counts  = results['Trigger'].value_counts().to_dict() if not results.empty else {}
        print(f"N={n}: {len(results)} total in {round(t1-t0,1)}s — {counts}")