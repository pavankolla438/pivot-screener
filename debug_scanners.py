"""
Debug trendline and accumulation scanner internals.
Run: python debug_scanners.py
"""
from market_context import get_context
from history_store import preload_histories, _store, _index, _swing_cache, get_all_histories, get_swing_points

# Build context
print("Building context...")
contexts = get_context('NSE')
ctx = contexts.get('NSE')
symbols = ctx.daily['symbol'].tolist()
print(f"Universe: {len(symbols)} symbols\n")

# Preload
preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=252)

# Check store keys
print("\n--- Store keys ---")
for exch in _store:
    for interval in _store[exch]:
        df = _store[exch][interval]
        n = df['_sym'].nunique() if df is not None and not df.empty else 0
        print(f"  _store['{exch}']['{interval}'] = {n} symbols")

print("\n--- Swing cache keys ---")
for k, v in _swing_cache.items():
    print(f"  _swing_cache['{k}'] = {len(v)} symbols")

# Check what exch value scanners see
print("\n--- Context keys ---")
for exch, ctx2 in contexts.items():
    print(f"  context key: '{exch}' → {len(ctx2.daily) if ctx2 and ctx2.daily is not None else 0} stocks")

# Simulate accumulation scanner
print("\n--- Accumulation simulation ---")
all_contexts = get_context('ALL')
for exch, ctx2 in all_contexts.items():
    print(f"  exch from contexts.items(): '{exch}'")
    h = get_all_histories(exch, '1d')
    print(f"  get_all_histories('{exch}', '1d') = {len(h)} symbols")

# Simulate trendline scanner  
print("\n--- Trendline simulation ---")
both_contexts = get_context('BOTH')
for exch, ctx2 in both_contexts.items():
    print(f"  exch from contexts.items(): '{exch}'")
    h1d = get_all_histories(exch, '1d')
    h1w = get_all_histories(exch, '1wk')
    print(f"  get_all_histories('{exch}', '1d')  = {len(h1d)} symbols")
    print(f"  get_all_histories('{exch}', '1wk') = {len(h1w)} symbols")
    # Check swing points for first symbol
    if ctx2 and ctx2.daily is not None:
        sym = ctx2.daily.iloc[0]['symbol']
        highs, lows = get_swing_points(sym, exch, '1d')
        print(f"  get_swing_points('{sym}', '{exch}', '1d') = {len(highs)} highs, {len(lows)} lows")
