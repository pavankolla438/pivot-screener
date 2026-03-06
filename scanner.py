import pandas as pd
from data_fetcher import get_last_trading_day
from pivot_calculator import calculate_pivots
from market_context import get_context

PROXIMITY_PCT = 1.0

def is_near(price, level, pct=PROXIMITY_PCT):
    if level == 0:
        return False
    return abs(price - level) / level * 100 <= pct

def check_cpr_pivot_hits(price, pivot_row):
    hits = []
    tc = pivot_row['TC']
    bc = pivot_row['BC']
    p  = pivot_row['P']
    if bc <= price <= tc:
        hits.append('Inside CPR')
    else:
        if is_near(price, tc): hits.append('Near TC')
        if is_near(price, bc): hits.append('Near BC')
    if is_near(price, p):
        hits.append('Near Pivot')
    return hits

def classify_hit(hits):
    if not hits:
        return None
    if 'Inside CPR' in hits:
        return 'Inside CPR'
    labels = []
    if any('TC' in h or 'BC' in h for h in hits):
        labels.append('CPR')
    if any('Pivot' in h for h in hits):
        labels.append('Pivot')
    return ' + '.join(labels) if labels else None

def run_scan(exchange='ALL'):
    day = get_last_trading_day()
    print(f"\n=== Pivot Confluence Scan | {day} | Exchange: {exchange} ===\n")

    results  = []
    contexts = get_context('ALL')

    for exch, ctx in contexts.items():
        if ctx is None:
            print(f"[{exch}] No context available, skipping.")
            continue
        if ctx.daily is None or ctx.weekly_pivots is None or ctx.monthly_pivots is None:
            print(f"[{exch}] Missing data, skipping.")
            continue

        print(f"--- {exch} ---")
        total   = len(ctx.daily)
        flagged = 0

        for _, stock in ctx.daily.iterrows():
            sym   = stock['symbol']
            price = stock['close']

            if sym not in ctx.weekly_pivots.index or sym not in ctx.monthly_pivots.index:
                continue

            w_row = ctx.weekly_pivots.loc[sym]
            m_row = ctx.monthly_pivots.loc[sym]

            w_hits = check_cpr_pivot_hits(price, w_row)
            m_hits = check_cpr_pivot_hits(price, m_row)

            if not w_hits or not m_hits:
                continue

            narrow_cpr_w = bool(w_row['narrow_cpr'])
            narrow_cpr_m = bool(m_row['narrow_cpr'])

            setup_parts = []
            if 'Inside CPR' in w_hits or 'Inside CPR' in m_hits:
                setup_parts.append('Inside CPR')
            if any('Pivot' in h for h in w_hits + m_hits):
                setup_parts.append('Near Pivot')
            if any('TC' in h or 'BC' in h for h in w_hits + m_hits):
                setup_parts.append('Near CPR Edge')
            if narrow_cpr_w or narrow_cpr_m:
                setup_parts.append('🔥 Narrow CPR')

            # Direction: price above weekly pivot = Long bias, below = Short bias
            w_pivot   = float(w_row['P'])
            direction = '🟢 Long' if price >= w_pivot else '🔴 Short'

            results.append({
                'Symbol':         sym,
                'Exchange':       exch,
                'Direction':      direction,
                'Price':          round(price, 2),
                'Setup':          ' | '.join(setup_parts),
                'Weekly Hit':     ', '.join(w_hits),
                'Monthly Hit':    ', '.join(m_hits),
                'Narrow CPR (W)': '✅' if narrow_cpr_w else '',
                'Narrow CPR (M)': '✅' if narrow_cpr_m else '',
                'W_P':            round(w_row['P'],  2),
                'W_TC':           round(w_row['TC'], 2),
                'W_BC':           round(w_row['BC'], 2),
                'W_CPR_Width%':   round(w_row['cpr_width_pct'], 3),
                'M_P':            round(m_row['P'],  2),
                'M_TC':           round(m_row['TC'], 2),
                'M_BC':           round(m_row['BC'], 2),
                'M_CPR_Width%':   round(m_row['cpr_width_pct'], 3),
            })
            flagged += 1

        print(f"[{exch}] Scanned {total} stocks — {flagged} flagged.\n")

    if not results:
        print("No stocks matched.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df['_sort'] = df['Setup'].apply(lambda s: 0 if '🔥' in s else 1)
    df = df.sort_values(['_sort', 'Symbol']).drop(columns=['_sort'])
    return df.reset_index(drop=True)

if __name__ == "__main__":
    results = run_scan(exchange='NSE')
    if not results.empty:
        print(f"Total flagged: {len(results)}\n")
        print(results[['Symbol','Price','Setup','Weekly Hit','Monthly Hit',
                        'W_P','W_TC','W_BC','M_P','M_TC','M_BC']].to_string(index=False))
    else:
        print("No results.")