from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import preload_histories, get_all_histories, get_swing_points

day     = get_last_trading_day()
symbols = get_nse_ohlc(day)['symbol'].tolist()
preload_histories(symbols, 'NSE', intervals=('1d',), lookback_bars=60)

histories = get_all_histories('NSE', '1d')

# Check first 5 symbols
checked = 0
for sym, df in histories.items():
    if df is None or len(df) < 8:
        continue

    high_idxs, low_idxs = get_swing_points(sym, 'NSE', '1d')
    hist     = df.iloc[:-1]
    today    = df.iloc[-1]

    valid_high = [i for i in high_idxs if i < len(hist)]
    valid_low  = [i for i in low_idxs  if i < len(hist)]

    resistance = float(hist['high'].iloc[valid_high[-1]]) if valid_high else None
    support    = float(hist['low'].iloc[valid_low[-1]])   if valid_low  else None

    print(f"{sym}:")
    print(f"  df len={len(df)}, hist len={len(hist)}")
    print(f"  high_idxs={high_idxs[-3:]}  valid={valid_high[-3:]}")
    print(f"  low_idxs={low_idxs[-3:]}   valid={valid_low[-3:]}")
    print(f"  resistance={resistance}  today_high={today['high']}")
    print(f"  support={support}        today_low={today['low']}")
    print(f"  breakout? {today['high'] > resistance if resistance else 'N/A'}")
    print(f"  breakdown? {today['low'] < support if support else 'N/A'}")
    print()

    checked += 1
    if checked >= 5:
        break