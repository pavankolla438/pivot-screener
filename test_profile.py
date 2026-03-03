import time

# ── Preload first ──
from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, get_all_histories, store_stats
from market_context import get_context

day     = get_last_trading_day()
daily   = get_nse_ohlc(day)
symbols = daily['symbol'].tolist()

print("Preloading...")
preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=60)
store_stats()

ctx       = get_context('NSE')['NSE']
daily_map = {row['symbol']: row for _, row in ctx.daily.iterrows()}

histories_1d  = get_all_histories('NSE', '1d')
histories_1wk = get_all_histories('NSE', '1wk')

print(f"1d symbols: {len(histories_1d)}, 1wk symbols: {len(histories_1wk)}")

# ── Profile trendline components ──
from trendline_scanner import (find_swing_lows, find_horizontal_supports,
                                find_rising_trendlines, check_bounce_trigger,
                                _price_near_lows)

t_prefilter = t_horiz = t_trendline = t_bounce = 0
passed = skipped = horiz_hits = tl_hits = 0

for sym, df in histories_1d.items():
    if df is None or len(df) < 10:
        continue

    t0   = time.perf_counter()
    near = _price_near_lows(df)
    t_prefilter += time.perf_counter() - t0

    if not near:
        skipped += 1
        continue
    passed += 1

    t0   = time.perf_counter()
    sups = find_horizontal_supports(df)
    t_horiz += time.perf_counter() - t0

    t0  = time.perf_counter()
    tls = find_rising_trendlines(df)
    t_trendline += time.perf_counter() - t0

    for sup in sups:
        t0 = time.perf_counter()
        check_bounce_trigger(df, sup['level'])
        t_bounce += time.perf_counter() - t0
        horiz_hits += 1

    tl_hits += len(tls)

print(f"\n=== Trendline Profile (1d) ===")
print(f"Passed pre-filter:   {passed}  |  Skipped: {skipped}")
print(f"Pre-filter:          {t_prefilter:.3f}s  ({t_prefilter/max(passed+skipped,1)*1000:.2f}ms/sym)")
print(f"Horizontal supports: {t_horiz:.3f}s  ({horiz_hits} levels checked)")
print(f"Rising trendlines:   {t_trendline:.3f}s  ({tl_hits} found)")
print(f"Bounce triggers:     {t_bounce:.3f}s")
print(f"TOTAL:               {t_prefilter+t_horiz+t_trendline+t_bounce:.3f}s")

# ── Profile breakout components ──
from breakout_scanner import get_swing_resistance, get_swing_support

t_resist = t_support = 0
n = 0

for sym, df in histories_1d.items():
    if df is None or len(df) < 8:
        continue

    t0 = time.perf_counter()
    get_swing_resistance(df)
    t_resist += time.perf_counter() - t0

    t0 = time.perf_counter()
    get_swing_support(df)
    t_support += time.perf_counter() - t0
    n += 1

print(f"\n=== Breakout Profile ({n} stocks) ===")
print(f"get_swing_resistance: {t_resist:.3f}s  ({t_resist/n*1000:.3f}ms/sym)")
print(f"get_swing_support:    {t_support:.3f}s  ({t_support/n*1000:.3f}ms/sym)")
print(f"TOTAL both:           {t_resist+t_support:.3f}s")
print(f"  → x2 for weekly:   {(t_resist+t_support)*2:.3f}s  (estimated full scan)")