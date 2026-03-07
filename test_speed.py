import time
import pandas as pd

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

_checkpoints = []

def tick(label):
    now = time.perf_counter()
    delta = (now - _checkpoints[-1][1]) if _checkpoints else 0.0
    _checkpoints.append((label, now, delta))
    print(f"  [{delta:6.2f}s]  {label}")

def summary():
    print("\n" + "=" * 58)
    print(f"  {'Stage':<42s}  {'Time':>6s}")
    print("=" * 58)
    total = 0.0
    for label, _, delta in _checkpoints[1:]:
        print(f"  {label:<42s}  {delta:5.2f}s")
        total += delta
    print("-" * 58)
    print(f"  {'TOTAL':<42s}  {total:5.2f}s")
    print("=" * 58)

# ─────────────────────────────────────────
# STAGE 1 — imports
# ─────────────────────────────────────────

print("\n=== Stage 1: Imports ===")
tick("start")

from data_fetcher import get_nse_ohlc, get_last_trading_day
tick("data_fetcher imported")

from history_store import preload_histories, store_stats, get_all_histories, get_swing_points
tick("history_store imported")

from scanner              import run_scan
from darvas_scanner       import run_darvas_scan
from trendline_scanner    import run_trendline_scan
from inside_bar_scanner   import run_inside_bar_scan
from accumulation_scanner import run_accumulation_scan
from momentum_scanner     import run_momentum_scan
tick("all 6 scanners imported")

# ─────────────────────────────────────────
# STAGE 2 — bhavcopy
# ─────────────────────────────────────────

print("\n=== Stage 2: Bhavcopy ===")
day  = get_last_trading_day()
tick(f"get_last_trading_day -> {day}")

ohlc = get_nse_ohlc(day)
syms = ohlc['symbol'].tolist()
tick(f"get_nse_ohlc -> {len(syms)} symbols")

# ─────────────────────────────────────────
# STAGE 3 — preload (split 1d vs 1wk)
# ─────────────────────────────────────────

print("\n=== Stage 3: Preload histories ===")

preload_histories(syms, 'NSE', intervals=('1d',), lookback_bars=252)
tick(f"preload 1d  ({len(syms)} syms)")

preload_histories(syms, 'NSE', intervals=('1wk',), lookback_bars=252)
tick(f"preload 1wk ({len(syms)} syms)")

store_stats()

# ─────────────────────────────────────────
# STAGE 4 — swing cache sanity check
# ─────────────────────────────────────────

print("\n=== Stage 4: Swing cache check ===")

hist_1d  = get_all_histories('NSE', '1d')
hist_1wk = get_all_histories('NSE', '1wk')
tick(f"get_all_histories -> {len(hist_1d)} daily / {len(hist_1wk)} weekly")

daily_with_swings = 0
daily_total_swings = 0
for sym in list(hist_1d.keys())[:500]:
    hi, lo = get_swing_points(sym, 'NSE', '1d')
    if hi or lo:
        daily_with_swings += 1
    daily_total_swings += len(hi) + len(lo)

print(f"  [sample 500] {daily_with_swings}/500 syms have swings, "
      f"{daily_total_swings} total pts, "
      f"avg {daily_total_swings/500:.1f}/sym")

if daily_with_swings < 100:
    print("  WARNING: Very few swing points -- min_prominence may still be too strict")
else:
    print("  OK Swing cache looks healthy")

# ─────────────────────────────────────────
# STAGE 5 — all 6 scanners
# ─────────────────────────────────────────

print("\n=== Stage 5: Scanner timings ===")

from market_context import get_context
get_context('ALL')
tick("market context built")

SCANNERS = [
    ("pivot",        lambda: run_scan()),
    ("darvas",       lambda: run_darvas_scan(direction='BOTH')),
    ("trendline",    lambda: run_trendline_scan()),
    ("inside_bar",   lambda: run_inside_bar_scan(direction='BOTH', n=2)),
    ("accumulation", lambda: run_accumulation_scan(min_score=1)),
    ("momentum",     lambda: run_momentum_scan(min_score=2)),
]

print(f"\n  {'Scanner':<16s}  {'Time':>6s}  {'Results':>8s}  Notes")
print("  " + "-" * 55)
for name, fn in SCANNERS:
    t0 = time.perf_counter()
    try:
        result = fn()
        n = len(result) if result is not None and not result.empty else 0
        note = '0 results -- check logic' if n == 0 else (f'{n} (high)' if n > 200 else '')
    except Exception as e:
        print(f"  ERROR {name:<14s}  {e}")
        _checkpoints.append((f"scanner: {name}", time.perf_counter(), time.perf_counter() - t0))
        continue
    elapsed = time.perf_counter() - t0
    print(f"  {name:<16s}  {elapsed:5.2f}s  {n:>8d}  {note}")
    _checkpoints.append((f"scanner: {name}", time.perf_counter(), elapsed))

# ─────────────────────────────────────────
# STAGE 6 — digest top 20
# ─────────────────────────────────────────

print("\n=== Stage 6: Digest / Top 20 ===")

from digest import pick_top_setups

# Build cache dict using the SAME key names pick_top_setups expects
# (mirrors exactly what app.py does in api_top10)
digest_cache = {
    'pivot_BOTH':          run_scan(),
    'darvas_BOTH':         run_darvas_scan(direction='BOTH'),
    'trendline_BOTH':      run_trendline_scan(),
    'insidebar_BOTH_2':    run_inside_bar_scan(direction='BOTH', n=2),
    'accumulation_BOTH_1': run_accumulation_scan(min_score=1),
    'momentum_BOTH_2':     run_momentum_scan(min_score=2),
}
tick("all scanners re-run for digest")

t0 = time.perf_counter()
top = pick_top_setups(digest_cache, top_n=20)
elapsed = time.perf_counter() - t0
tick(f"pick_top_setups -> {len(top)} results  ({elapsed:.3f}s)")

if not top.empty:
    cols = ['Symbol', 'Direction', 'Price', 'Setup', 'Score', '_n_scanners']
    show = [c for c in cols if c in top.columns]
    print("\n  Top setups:")
    print(top[show].head(10).to_string(index=False))

    missing_dir = top['Direction'].isna().sum() + (top['Direction'] == '').sum()
    if missing_dir:
        print(f"\n  WARNING: {missing_dir} rows missing Direction")
    else:
        print(f"\n  OK All {len(top)} rows have Direction")
else:
    print("  WARNING: No top setups returned -- check scanner results above")

# ─────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────

summary()
