"""
force_today.py — run once from C:\pivot_screener to force-download today's bhavcopy.
After this, get_last_trading_day() will return today and the app will use today's data.
"""
from datetime import date
from data_fetcher import download_nse_bhavcopy, is_trading_day, get_last_trading_day, DATA_DIR
import os

today = date.today()
print(f"Today: {today}")
print(f"DATA_DIR: {DATA_DIR}")
print(f"Is trading day: {is_trading_day(today)}")
print()

if not is_trading_day(today):
    print("Today is not a trading day — nothing to download.")
else:
    cache_path = os.path.join(DATA_DIR, f"nse_bhav_{today.strftime('%Y%m%d')}.csv")
    if os.path.exists(cache_path):
        print(f"Already downloaded: {cache_path}")
    else:
        print(f"Downloading bhavcopy for {today}...")
        df = download_nse_bhavcopy(today)
        if df is not None and not df.empty:
            print(f"Success: {len(df)} rows downloaded")
        else:
            print("FAILED — NSE may not have published yet, or network issue")

print()
print(f"get_last_trading_day() now returns: {get_last_trading_day()}")
