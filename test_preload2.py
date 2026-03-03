import time
from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, clear_store, store_stats

day     = get_last_trading_day()
daily   = get_nse_ohlc(day)
symbols = daily['symbol'].tolist()[:100]

# Clear memory store but keep disk cache
clear_store()

t0 = time.time()
preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=60)
t1 = time.time()

print(f'\nSecond run (disk cache): {round(t1-t0, 1)}s')
store_stats()