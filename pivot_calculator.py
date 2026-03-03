import pandas as pd

# ─────────────────────────────────────────
# STANDARD (CLASSIC) PIVOT CALCULATION
# ─────────────────────────────────────────

def calculate_pivots(ohlc_df):
    """
    Takes a DataFrame with columns: symbol, open, high, low, close, exchange
    Returns a DataFrame with all pivot levels added.
    
    Standard Pivot formulas:
      Pivot (P)  = (High + Low + Close) / 3
      BC         = (High + Low) / 2          <- Bottom of CPR
      TC         = (P - BC) + P              <- Top of CPR
      R1 = (2 * P) - Low
      R2 = P + (High - Low)
      R3 = High + 2 * (P - Low)
      S1 = (2 * P) - High
      S2 = P - (High - Low)
      S3 = Low - 2 * (High - P)
    """
    df = ohlc_df.copy()

    df['P']  = (df['high'] + df['low'] + df['close']) / 3
    df['BC'] = (df['high'] + df['low']) / 2
    df['TC'] = (df['P'] - df['BC']) + df['P']

    # Ensure TC is always the top of CPR
    tc = df[['TC', 'BC']].max(axis=1)
    bc = df[['TC', 'BC']].min(axis=1)
    df['TC'] = tc
    df['BC'] = bc

    df['R1'] = (2 * df['P']) - df['low']
    df['R2'] = df['P'] + (df['high'] - df['low'])
    df['R3'] = df['high'] + 2 * (df['P'] - df['low'])

    df['S1'] = (2 * df['P']) - df['high']
    df['S2'] = df['P'] - (df['high'] - df['low'])
    df['S3'] = df['low'] - 2 * (df['high'] - df['P'])

    # CPR width as % of price — narrow CPR flag (< 0.5%)
    df['cpr_width_pct'] = ((df['TC'] - df['BC']) / df['P']) * 100
    df['narrow_cpr']    = df['cpr_width_pct'] < 0.5

    return df

# ─────────────────────────────────────────
# PROXIMITY CHECK
# ─────────────────────────────────────────

PROXIMITY_PCT = 1.0  # within 1% of a level

def is_near(price, level, pct=PROXIMITY_PCT):
    """Returns True if price is within pct% of level."""
    if level == 0:
        return False
    return abs(price - level) / level * 100 <= pct

def check_proximity(price, pivot_row):
    """
    Given a current price and a row of pivot levels,
    returns a dict of which levels the price is near.
    """
    levels = {
        'CPR (between TC-BC)': None,  # handled separately
        'R1': pivot_row['R1'],
        'R2': pivot_row['R2'],
        'R3': pivot_row['R3'],
        'S1': pivot_row['S1'],
        'S2': pivot_row['S2'],
        'S3': pivot_row['S3'],
        'Pivot': pivot_row['P'],
    }

    hits = []

    # Special case: price inside CPR band
    if pivot_row['BC'] <= price <= pivot_row['TC']:
        hits.append('Inside CPR')
    elif is_near(price, pivot_row['TC']):
        hits.append('Near TC (top of CPR)')
    elif is_near(price, pivot_row['BC']):
        hits.append('Near BC (bottom of CPR)')

    for name, level in levels.items():
        if name == 'CPR (between TC-BC)':
            continue
        if is_near(price, level):
            hits.append(f'Near {name}')

    return hits

# ─────────────────────────────────────────
# CONFLUENCE CHECK
# ─────────────────────────────────────────

def check_confluence(weekly_hits, monthly_hits):
    """
    Returns True if there is overlap between weekly and monthly level hits.
    E.g. price is near Weekly S1 AND Monthly S1 — strong confluence.
    """
    if not weekly_hits or not monthly_hits:
        return False
    # Simplify hit names for comparison (strip 'Near ' prefix)
    w = set(h.replace('Near ', '') for h in weekly_hits)
    m = set(h.replace('Near ', '') for h in monthly_hits)
    return len(w & m) > 0

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    # Synthetic test case
    test = pd.DataFrame([{
        'symbol': 'RELIANCE',
        'open': 1200, 'high': 1250, 'low': 1180, 'close': 1230,
        'exchange': 'NSE'
    }])

    result = calculate_pivots(test)
    row = result.iloc[0]

    print("=== Pivot Levels for RELIANCE (test) ===")
    print(f"  P  : {row['P']:.2f}")
    print(f"  TC : {row['TC']:.2f}")
    print(f"  BC : {row['BC']:.2f}")
    print(f"  R1 : {row['R1']:.2f}  R2 : {row['R2']:.2f}  R3 : {row['R3']:.2f}")
    print(f"  S1 : {row['S1']:.2f}  S2 : {row['S2']:.2f}  S3 : {row['S3']:.2f}")
    print(f"  CPR Width: {row['cpr_width_pct']:.3f}%  Narrow: {row['narrow_cpr']}")

    # Test proximity
    test_price = 1215.0
    hits = check_proximity(test_price, row)
    print(f"\n  Price {test_price} is near: {hits if hits else 'nothing'}")