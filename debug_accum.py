"""
debug_accum.py — run from C:\\pivot_screener
Traces exactly why accumulation returns 0.
"""
from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, get_all_histories
import pandas as pd
import numpy as np

VOL_AVG_PERIOD  = 21
TIGHT_RANGE_PCT = 2.0
FLAT_MOVE_PCT   = 1.0
VOL_SPIKE_RATIO = 2.0
VOL_BUILD_BARS  = 5
LOW_52W_PCT     = 7.0
OBV_LOOKBACK    = 30
CONSOL_BARS     = 5
VOL_RISE_DAYS   = 3

day  = get_last_trading_day()
ohlc = get_nse_ohlc(day)
syms = ohlc['symbol'].tolist()
print(f"Loaded {len(syms)} symbols for {day}")

preload_histories(syms, 'NSE', intervals=('1d',), lookback_bars=252)
h = get_all_histories('NSE', '1d')
print(f"Histories: {len(h)} symbols\n")

s1_hits = s2_hits = s3_hits = s4_hits = 0
too_short = no_vol = checked = 0
sample_debug = []

for sym, df in h.items():
    if df is None or len(df) < 30:
        too_short += 1; continue
    if 'volume' not in df.columns:
        no_vol += 1; continue
    checked += 1

    vols   = pd.to_numeric(df['volume'], errors='coerce').values
    closes = pd.to_numeric(df['close'],  errors='coerce').values
    highs  = pd.to_numeric(df['high'],   errors='coerce').values
    lows   = pd.to_numeric(df['low'],    errors='coerce').values

    # s1
    recent     = df.iloc[-CONSOL_BARS:]
    rng        = (recent['high'].max() - recent['low'].min()) / recent['low'].min() * 100
    vol_consec = all(vols[-i] > vols[-(i+1)] for i in range(1, VOL_RISE_DAYS + 1))
    if rng <= TIGHT_RANGE_PCT and vol_consec: s1_hits += 1

    # s2
    avg_v = np.mean(vols[-VOL_AVG_PERIOD-1:-1]) if len(vols) > VOL_AVG_PERIOD else 0
    spike = (vols[-1] / avg_v >= VOL_SPIKE_RATIO) if avg_v > 0 else False
    move  = abs(closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] > 0 else 999
    if spike and move < FLAT_MOVE_PCT: s2_hits += 1

    # s3
    if len(closes) >= OBV_LOOKBACK + 1:
        obv = np.zeros(len(closes))
        for i in range(1, len(closes)):
            obv[i] = obv[i-1] + (vols[i] if closes[i] > closes[i-1]
                                  else -vols[i] if closes[i] < closes[i-1] else 0)
        slope = np.polyfit(np.arange(OBV_LOOKBACK), obv[-OBV_LOOKBACK:], 1)[0]
        if slope > np.mean(vols[-OBV_LOOKBACK:]) * 0.005: s3_hits += 1

    # s4
    if len(lows) >= VOL_BUILD_BARS + 20:
        low_52w      = lows.min()
        pct_from_low = (lows[-1] - low_52w) / low_52w * 100 if low_52w > 0 else 999
        rv_avg = np.mean(vols[-VOL_BUILD_BARS:])
        pv_avg = np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS])
        if pct_from_low <= LOW_52W_PCT and rv_avg > pv_avg: s4_hits += 1

    if len(sample_debug) < 5:
        sample_debug.append({
            'sym': sym, 'bars': len(df),
            'rng_5d%': round(rng, 2), 'vol_consec': vol_consec,
            'avg_vol': round(avg_v, 0), 'today_vol': round(float(vols[-1]), 0),
            'spike_2x': spike, 'move%': round(move, 2),
            's2_pass': spike and move < FLAT_MOVE_PCT,
        })

print(f"Checked:{checked}  too_short:{too_short}  no_vol:{no_vol}\n")
print(f"Signal hits (inline logic):")
print(f"  s1 tight+vol_rising : {s1_hits:4d}  ({s1_hits/max(checked,1)*100:.1f}%)")
print(f"  s2 spike+flat       : {s2_hits:4d}  ({s2_hits/max(checked,1)*100:.1f}%)")
print(f"  s3 OBV rising       : {s3_hits:4d}  ({s3_hits/max(checked,1)*100:.1f}%)")
print(f"  s4 52w low+vol build: {s4_hits:4d}  ({s4_hits/max(checked,1)*100:.1f}%)")

print(f"\nSample (first 5):")
for d in sample_debug:
    print(f"  {d['sym']:15s} bars={d['bars']:3d}  5d_rng={d['rng_5d%']:5.1f}%  "
          f"vol={d['today_vol']:8.0f}/avg={d['avg_vol']:8.0f}  "
          f"spike={d['spike_2x']}  move={d['move%']:.2f}%  s2={d['s2_pass']}")

print("\n--- Actual scanner ---")
from accumulation_scanner import run_accumulation_scan, score_symbol
results = run_accumulation_scan(min_score=1)
print(f"Scanner returned: {len(results)} rows")

# If still 0, test score_symbol directly on first 3 histories
if results.empty:
    print("\nDirect score_symbol test on first 3 histories:")
    for sym, df in list(h.items())[:3]:
        sc = score_symbol(df)
        print(f"  {sym}: score={sc['score']}  signals={sc['signals']}")
        print(f"         s1={sc['tight_vol_rising']} s2={sc['vol_spike_flat']} "
              f"s3={sc['obv_rising']} s4={sc['near_52w_low']}")
