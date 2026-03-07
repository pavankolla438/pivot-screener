"""
test_speed.py — mirrors production flow exactly.
Shows both cumulative time AND per-stage delta.
"""
import time
import pandas as pd

_t0   = time.perf_counter()
_prev = _t0

def _lap(label):
    global _prev
    now   = time.perf_counter()
    total = now - _t0
    delta = now - _prev
    _prev = now
    print(f"  {label:<48} {delta:>6.2f}s   ({total:.2f}s total)")

print("\n" + "=" * 70)
print(f"  {'Stage':<48} {'Delta':>6}   {'Cumul'}")
print("=" * 70)

# ── imports ──────────────────────────────────────────────
import data_fetcher;                         _lap("data_fetcher imported")
import history_store;                        _lap("history_store imported")

from scanner              import run_scan
from darvas_scanner       import run_darvas_scan
from trendline_scanner    import run_trendline_scan
from inside_bar_scanner   import run_inside_bar_scan
from accumulation_scanner import run_accumulation_scan
from momentum_scanner     import run_momentum_scan
from digest               import pick_top_setups
from volume_helper        import enrich_with_volume
_lap("all imports done")

# ── data ─────────────────────────────────────────────────
from data_fetcher  import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, get_all_histories
from market_context import get_context

day = get_last_trading_day();               _lap(f"get_last_trading_day -> {day}")
ohlc = get_nse_ohlc(day);                  _lap(f"get_nse_ohlc -> {len(ohlc)} symbols")
syms = ohlc['symbol'].tolist()

preload_histories(syms, 'NSE', intervals=('1d',),   lookback_bars=252)
_lap(f"preload 1d  ({len(syms)} syms)")

preload_histories(syms, 'NSE', intervals=('1wk',),  lookback_bars=252)
_lap(f"preload 1wk ({len(syms)} syms)")

d1  = get_all_histories('NSE', '1d')
d1w = get_all_histories('NSE', '1wk')
_lap(f"get_all_histories -> {len(d1)} daily / {len(d1w)} weekly")

get_context('ALL');                         _lap("market context built")

# ── scanners — run ONCE ──────────────────────────────────
def run(fn, **kwargs):
    df = fn(**kwargs)
    if not df.empty:
        df = enrich_with_volume(df)
    return df

r_pivot  = run(run_scan);                                _lap("scanner: pivot")
r_darvas = run(run_darvas_scan, direction='BOTH');       _lap("scanner: darvas")
r_tl     = run(run_trendline_scan);                      _lap("scanner: trendline")
r_ib     = run(run_inside_bar_scan, direction='BOTH', n=2); _lap("scanner: inside_bar")
r_accum  = run(run_accumulation_scan, min_score=1);      _lap("scanner: accumulation")
r_mom    = run(run_momentum_scan, min_score=2);          _lap("scanner: momentum")

# ── digest — no re-run ───────────────────────────────────
digest_cache = {
    'pivot_BOTH':          r_pivot,
    'darvas_BOTH':         r_darvas,
    'trendline_BOTH':      r_tl,
    'insidebar_BOTH_2':    r_ib,
    'accumulation_BOTH_1': r_accum,
    'momentum_BOTH_2':     r_mom,
}
top = pick_top_setups(digest_cache, top_n=20)
_lap(f"pick_top_setups -> {len(top) if top is not None else 0} results")

print("-" * 70)
_lap("TOTAL")
print("=" * 70)

# ── results ──────────────────────────────────────────────
if top is not None and not top.empty:
    n_dir = (top['Direction'].notna() & (top['Direction'] != '')).sum()
    print(f"\nAll {n_dir}/{len(top)} rows have Direction")
    pd.set_option('display.max_columns', 8)
    pd.set_option('display.width', 130)
    print(top[['Symbol', 'Direction', 'Score', 'Scanner', 'Setup', 'Price']].to_string(index=False))
else:
    print("\nNo results returned.")
