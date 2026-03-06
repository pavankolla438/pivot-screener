"""
Check what series codes the junk symbols have in bhavcopy.
Run: python debug_series.py
"""
from data_fetcher import get_last_trading_day, download_nse_bhavcopy
import pandas as pd

day = get_last_trading_day()
df  = download_nse_bhavcopy(day)
df.columns = df.columns.str.strip()

col_map = {
    'TckrSymb': 'symbol',
    'SctySrs':  'series',
    'ClsPric':  'close',
}
df = df.rename(columns=col_map)

check = ['BSLSENETFG', 'BANKNIFTY1', 'SMALL250', 'SENSEXBETA',
         'ICDSLTD', 'ORCHASP', 'PREMIERPOL', 'AVONMORE', 'STEELCITY',
         'NATCAPSUQ', 'RVTH', 'ROML', 'DEN', 'AARON', 'ELGIRUBCO']

for sym in check:
    rows = df[df['symbol'] == sym]
    if rows.empty:
        print(f"  {sym:15s} → NOT FOUND in bhavcopy")
    else:
        series = rows['series'].values[0].strip()
        close  = rows['close'].values[0] if 'close' in rows.columns else '?'
        print(f"  {sym:15s} → series={series!r:6s} close={close}")
