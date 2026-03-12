"""
test_ib2.py — proper N=1/2/3 diagnostic with preload.
Run from C:\\pivot_screener:  python test_ib2.py
"""
from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, get_all_histories
from inside_bar_scanner import find_inside_bar_setup

day  = get_last_trading_day()
ohlc = get_nse_ohlc(day)
syms = ohlc['symbol'].tolist()
print(f"Universe: {len(syms)} symbols, last trading day: {day}")

preload_histories(syms, 'NSE', intervals=('1d',), lookback_bars=252)

h = get_all_histories('NSE', '1d')
print(f"Histories loaded: {len(h)} symbols")

# ── Count hits per N ──
for n in [1, 2, 3]:
    hits = [(sym, r['trigger']) for sym, df in h.items()
            if (r := find_inside_bar_setup(df, n=n)) is not None]
    print(f"\nN={n}: {len(hits)} hits")
    if hits:
        from collections import Counter
        print("  Trigger breakdown:", dict(Counter(t for _, t in hits)))
        print("  Sample:", hits[:3])

# ── Deep-dive N=2 to show raw bar data ──
print("\n── N=2 raw bar check (first 5 symbols) ──")
checked = 0
for sym, df in h.items():
    if checked >= 5:
        break
    if len(df) < 3:
        continue
    m = df.iloc[-3]   # mother
    y = df.iloc[-2]   # in_between (yesterday)
    t = df.iloc[-1]   # today
    h_ok = y['high'] <= m['high']
    l_ok = y['low']  >= m['low']
    print(f"\n{sym}:")
    print(f"  mother [{str(df.index[-3])[:10]}]: H={m['high']:.2f}  L={m['low']:.2f}")
    print(f"  inside [{str(df.index[-2])[:10]}]: H={y['high']:.2f}  L={y['low']:.2f}  "
          f"→ H_inside={h_ok}  L_inside={l_ok}  PASS={h_ok and l_ok}")
    print(f"  today  [{str(df.index[-1])[:10]}]: H={t['high']:.2f}  L={t['low']:.2f}  close={t['close']:.2f}")
    checked += 1
