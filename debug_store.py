"""
Debug script — shows what keys are in history store after preload.
Run: python debug_store.py
"""
from market_context import get_context
from history_store import preload_histories, _store, _index, store_stats

# Build context (triggers universe filter)
print("Building context...")
contexts = get_context('NSE')
ctx = contexts.get('NSE')

if ctx is None or ctx.daily is None:
    print("ERROR: No context built")
    exit()

symbols = ctx.daily['symbol'].tolist()
print(f"Universe: {len(symbols)} symbols")

# Preload
print("\nPreloading...")
preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=252)

# Show store keys
print("\n--- _store keys ---")
for exch, intervals in _store.items():
    for interval, df in intervals.items():
        n = df['_sym'].nunique() if df is not None and not df.empty else 0
        print(f"  _store['{exch}']['{interval}'] = {n} symbols")

print("\n--- _index keys ---")
for exch, intervals in _index.items():
    for interval, idx in intervals.items():
        print(f"  _index['{exch}']['{interval}'] = {len(idx)} symbols")

# Check what get_all_histories returns for different exchange values
print("\n--- get_all_histories check ---")
from history_store import get_all_histories
for exch in ['NSE', 'ALL', 'BOTH']:
    h = get_all_histories(exch, '1d')
    print(f"  get_all_histories('{exch}', '1d') = {len(h)} symbols")
