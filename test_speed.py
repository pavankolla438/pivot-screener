import time
from history_store import clear_store
from market_context import clear_context

# Clear everything so we test cold start
clear_store()
clear_context()

print("=" * 50)
print("COLD START — first run (parquet → memory)")
print("=" * 50)

t0 = time.time()
from market_context import get_context
contexts = get_context('NSE')
t1 = time.time()
print(f"Context build: {round(t1-t0, 1)}s")

from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories
day    = get_last_trading_day()
daily  = get_nse_ohlc(day)
syms   = daily['symbol'].tolist()

t2 = time.time()
preload_histories(syms, 'NSE', intervals=('1d','1wk'), lookback_bars=60)
t3 = time.time()
print(f"History preload: {round(t3-t2, 1)}s")

print("\n" + "=" * 50)
print("SCAN TIMES (after preload)")
print("=" * 50)

from scanner import run_scan
t4 = time.time()
r  = run_scan(exchange='NSE')
t5 = time.time()
print(f"Pivot scan:     {round(t5-t4, 1)}s  ({len(r)} results)")

from darvas_scanner import run_darvas_scan
t6 = time.time()
r  = run_darvas_scan(exchange='NSE')
t7 = time.time()
print(f"Darvas scan:    {round(t7-t6, 1)}s  ({len(r)} results)")

from breakout_scanner import run_breakout_scan
t8 = time.time()
r  = run_breakout_scan(exchange='NSE')
t9 = time.time()
print(f"Breakout scan:  {round(t9-t8, 1)}s  ({len(r)} results)")

from inside_bar_scanner import run_inside_bar_scan
t10 = time.time()
r   = run_inside_bar_scan(exchange='NSE', n=2)
t11 = time.time()
print(f"Inside bar scan:{round(t11-t10, 1)}s  ({len(r)} results)")

from trendline_scanner import run_trendline_scan
t12 = time.time()
r   = run_trendline_scan(exchange='NSE')
t13 = time.time()
print(f"Trendline scan: {round(t13-t12, 1)}s  ({len(r)} results)")

print(f"\nTotal scan time: {round(t13-t4, 1)}s")
print(f"Total with preload: {round(t13-t0, 1)}s")