from history_store import get_all_histories
from inside_bar_scanner import find_inside_bar_setup

h = get_all_histories('NSE', '1d')
hits2 = []
for sym, df in h.items():
    r = find_inside_bar_setup(df, n=2)
    if r:
        hits2.append((sym, r['trigger']))

print(f"N=2 raw hits: {len(hits2)}")
if not hits2:
    # Show last 3 bars of first stock to check date alignment
    sym = list(h.keys())[0]
    df  = h[sym]
    print(df[['high','low','close']].tail(4))
    m, y = df.iloc[-3], df.iloc[-2]
    print(f"mother H={m['high']:.2f} L={m['low']:.2f}")
    print(f"inside H={y['high']:.2f} L={y['low']:.2f}")
    print(f"inside? {y['high'] <= m['high']} {y['low'] >= m['low']}")