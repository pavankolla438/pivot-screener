"""
Check how many S3+S4 stocks pass the vol_building_strong gate.
Run: python debug_accum2.py
"""
from market_context import get_context
from history_store import preload_histories, get_all_histories
import numpy as np
import pandas as pd

VOL_BUILD_BARS = 5
LOW_52W_PCT    = 7.0
OBV_LOOKBACK   = 30

contexts = get_context('NSE')
ctx = contexts.get('NSE')
symbols = ctx.daily['symbol'].tolist()
preload_histories(symbols, 'NSE', intervals=('1d',), lookback_bars=252)
histories  = get_all_histories('NSE', '1d')
daily_map  = {row['symbol']: row for _, row in ctx.daily.iterrows()}

s3s4       = []
ratios     = []

for sym, df in histories.items():
    if daily_map.get(sym) is None or df is None or len(df) < OBV_LOOKBACK + VOL_BUILD_BARS + 20:
        continue
    if 'volume' not in df.columns:
        continue

    closes = pd.to_numeric(df['close'],  errors='coerce').values
    vols   = pd.to_numeric(df['volume'], errors='coerce').values
    lows   = pd.to_numeric(df['low'],    errors='coerce').values

    # S3
    obv = np.zeros(len(closes))
    for i in range(1, len(closes)):
        obv[i] = obv[i-1] + vols[i] if closes[i] > closes[i-1] else obv[i-1] - vols[i] if closes[i] < closes[i-1] else obv[i-1]
    slope = np.polyfit(np.arange(OBV_LOOKBACK), obv[-OBV_LOOKBACK:], 1)[0]
    avg_v = float(np.mean(vols[-OBV_LOOKBACK:]))
    s3 = slope > avg_v * 0.005

    # S4
    low_52w      = float(np.min(lows))
    pct_from_low = (float(lows[-1]) - low_52w) / low_52w * 100 if low_52w > 0 else 999
    recent_avg   = float(np.mean(vols[-VOL_BUILD_BARS:]))
    prior_avg    = float(np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS]))
    s4 = pct_from_low <= LOW_52W_PCT and recent_avg > prior_avg

    if s3 and s4:
        ratio = recent_avg / prior_avg if prior_avg > 0 else 0
        ratios.append(ratio)
        s3s4.append((sym, round(pct_from_low, 1), round(ratio, 2)))

print(f"S3+S4 stocks: {len(s3s4)}")
if ratios:
    print(f"Vol build ratio range: {min(ratios):.2f}x — {max(ratios):.2f}x")
    print(f"Stocks passing 2.0x gate: {sum(1 for r in ratios if r >= 2.0)}")
    print(f"Stocks passing 1.5x gate: {sum(1 for r in ratios if r >= 1.5)}")
    print(f"Stocks passing 1.2x gate: {sum(1 for r in ratios if r >= 1.2)}")
    print(f"\nAll S3+S4 stocks:")
    for sym, pct, ratio in sorted(s3s4, key=lambda x: -x[2]):
        gate = "✅ PASS" if ratio >= 2.0 else "❌ fail"
        print(f"  {sym:15s}  {pct:.1f}% from 52W low  vol_ratio={ratio:.2f}x  {gate}")
