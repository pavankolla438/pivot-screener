from breakout_scanner import run_breakout_scan
from data_fetcher import get_nse_ohlc, get_last_trading_day
from history_store import preload_histories
day = get_last_trading_day()
syms = get_nse_ohlc(day)['symbol'].tolist()
preload_histories(syms, 'NSE', intervals=('1d','1wk'), lookback_bars=252)
df = run_breakout_scan(exchange='NSE', direction='BOTH')
print(df.columns.tolist())
print(df.head(3))