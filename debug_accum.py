"""
Debug accumulation scanner — show signal counts even if score=0.
Run: python debug_accum.py
"""
from market_context import get_context
from history_store import preload_histories, get_all_histories
import pandas as pd
import numpy as np

VOL_AVG_PERIOD  = 21
TIGHT_RANGE_PCT = 2.0
FLAT_MOVE_PCT   = 0.5
VOL_SPIKE_RATIO = 2.0
VOL_BUILD_BARS  = 5
LOW_52W_PCT     = 7.0
OBV_LOOKBACK    = 30
CONSOL_BARS     = 5
VOL_RISE_DAYS   = 3

contexts = get_context('NSE')
ctx = contexts.get('NSE')
symbols = ctx.daily['symbol'].tolist()
preload_histories(symbols, 'NSE', intervals=('1d',), lookback_bars=252)

histories = get_all_histories('NSE', '1d')
daily_map = {row['symbol']: row for _, row in ctx.daily.iterrows()}

s1_count = s2_count = s3_count = s4_count = 0
no_vol   = 0
too_short = 0
total    = 0

for sym, df in histories.items():
    if daily_map.get(sym) is None:
        continue
    if df is None or len(df) < 30:
        too_short += 1
        continue
    if 'volume' not in df.columns:
        no_vol += 1
        continue

    total += 1

    # S1: tight range + vol rising
    if len(df) >= CONSOL_BARS + VOL_RISE_DAYS:
        recent    = df.iloc[-CONSOL_BARS:]
        hr        = float(recent['high'].max())
        lr        = float(recent['low'].min())
        range_pct = (hr - lr) / lr * 100 if lr > 0 else 999
        vols      = df['volume'].values
        vol_rising = all(vols[-i] > vols[-(i+1)] for i in range(1, VOL_RISE_DAYS+1))
        if range_pct <= TIGHT_RANGE_PCT and vol_rising:
            s1_count += 1

    # S2: vol spike + flat price
    if len(df) >= VOL_AVG_PERIOD + 2:
        vols      = pd.to_numeric(df['volume'], errors='coerce').values
        today_vol = float(vols[-1])
        avg_vol   = float(np.mean(vols[-VOL_AVG_PERIOD-1:-1]))
        if avg_vol > 0:
            ratio = today_vol / avg_vol
            if ratio >= VOL_SPIKE_RATIO:
                move = abs(float(df.iloc[-1]['close']) - float(df.iloc[-2]['close'])) / float(df.iloc[-2]['close']) * 100
                if move < FLAT_MOVE_PCT:
                    s2_count += 1

    # S3: OBV rising
    if len(df) >= OBV_LOOKBACK + 1:
        closes = pd.to_numeric(df['close'], errors='coerce').values
        vols   = pd.to_numeric(df['volume'], errors='coerce').values
        obv    = np.zeros(len(closes))
        for i in range(1, len(closes)):
            obv[i] = obv[i-1] + vols[i] if closes[i] > closes[i-1] else obv[i-1] - vols[i] if closes[i] < closes[i-1] else obv[i-1]
        slope  = np.polyfit(np.arange(OBV_LOOKBACK), obv[-OBV_LOOKBACK:], 1)[0]
        avg_v  = float(np.mean(vols[-OBV_LOOKBACK:]))
        if slope > avg_v * 0.005:
            s3_count += 1

    # S4: near 52w low + vol building
    if len(df) >= VOL_BUILD_BARS + 20:
        lows        = pd.to_numeric(df['low'], errors='coerce').values
        vols        = pd.to_numeric(df['volume'], errors='coerce').values
        low_52w     = float(np.min(lows))
        current_low = float(lows[-1])
        pct_from_low = (current_low - low_52w) / low_52w * 100 if low_52w > 0 else 999
        if pct_from_low <= LOW_52W_PCT:
            recent_avg = float(np.mean(vols[-VOL_BUILD_BARS:]))
            prior_avg  = float(np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS]))
            if recent_avg > prior_avg:
                s4_count += 1

print(f"\nTotal symbols checked: {total}")
print(f"Too short (<30 bars):  {too_short}")
print(f"No volume column:      {no_vol}")
print(f"\nSignal counts (independent):")
print(f"  S1 Tight Range + Vol Rising : {s1_count}")
print(f"  S2 Vol Spike + Price Flat   : {s2_count}")
print(f"  S3 OBV Rising               : {s3_count}")
print(f"  S4 Near 52W Low + Vol Build : {s4_count}")
print(f"\nNote: S1 or S2 required to qualify (or S3+S4+strong vol)")
