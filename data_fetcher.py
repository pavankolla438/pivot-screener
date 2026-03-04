import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import zipfile
import io

DATA_DIR = r"C:\pivot_screener\data"
os.makedirs(DATA_DIR, exist_ok=True)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}

# ─────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────

def get_last_trading_day():
    """
    Returns most recent trading day for which Bhavcopy is available.
    Falls back up to 5 days if today's file isn't published yet.
    """
    for days_back in range(0, 6):
        candidate = datetime.today() - timedelta(days=days_back)
        if candidate.weekday() >= 5:
            continue
        date_str   = candidate.strftime("%Y%m%d")
        cache_path = os.path.join(DATA_DIR, f"nse_bhav_{date_str}.csv")

        if os.path.exists(cache_path):
            return candidate.date()

        filename = f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
        url      = f"https://nsearchives.nseindia.com/content/cm/{filename}"
        try:
            r = requests.get(url, headers=NSE_HEADERS, timeout=15)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)
                df.to_csv(cache_path, index=False)
                print(f"[NSE] Downloaded Bhavcopy for {candidate.date()}")
                return candidate.date()
            else:
                print(f"[NSE] Bhavcopy not available for {candidate.date()} "
                      f"(HTTP {r.status_code}), trying previous day...")
        except Exception as e:
            print(f"[NSE] Error fetching {candidate.date()}: {e}")

    print("[NSE] Could not find Bhavcopy for last 5 trading days.")
    return None

def get_previous_week_range():
    today = datetime.today().date()
    start_of_this_week = today - timedelta(days=today.weekday())
    end_of_prev_week   = start_of_this_week - timedelta(days=1)
    start_of_prev_week = end_of_prev_week - timedelta(days=6)
    return start_of_prev_week, end_of_prev_week

def get_previous_month_range():
    today = datetime.today().date()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month  = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    return first_of_prev_month, last_of_prev_month

# ─────────────────────────────────────────
# NSE BHAVCOPY
# ─────────────────────────────────────────

def download_nse_bhavcopy(date):
    yyyy     = date.strftime("%Y")
    mm       = date.strftime("%m")
    dd       = date.strftime("%d")
    filename = f"BhavCopy_NSE_CM_0_0_0_{yyyy}{mm}{dd}_F_0000.csv"
    zip_url  = f"https://nsearchives.nseindia.com/content/cm/{filename}.zip"

    cache_path = os.path.join(DATA_DIR, f"nse_bhav_{yyyy}{mm}{dd}.csv")
    if os.path.exists(cache_path):
        print(f"[NSE] Loading cached file for {date}")
        return pd.read_csv(cache_path)

    print(f"[NSE] Downloading Bhavcopy for {date} ...")
    try:
        r = requests.get(zip_url, headers=NSE_HEADERS, timeout=20)
        if r.status_code == 200:
            z        = zipfile.ZipFile(io.BytesIO(r.content))
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)
            df.to_csv(cache_path, index=False)
            return df
        else:
            print(f"[NSE] Failed: HTTP {r.status_code} for {zip_url}")
            return None
    except Exception as e:
        print(f"[NSE] Error: {e}")
        return None

def get_nse_ohlc(date):
    df = download_nse_bhavcopy(date)
    if df is None:
        return None
    df.columns = df.columns.str.strip()
    col_map = {
        'TradDt':   'date',
        'TckrSymb': 'symbol',
        'OpnPric':  'open',
        'HghPric':  'high',
        'LwPric':   'low',
        'ClsPric':  'close',
        'SctySrs':  'series',
        'TtlTradgVol': 'volume',
    }
    df = df.rename(columns=col_map)
    if 'series' in df.columns:
        df = df[df['series'].str.strip() == 'EQ']
    cols = [c for c in ['symbol','open','high','low','close','volume'] if c in df.columns]
    df = df[cols].copy()
    df['exchange'] = 'NSE'
    for c in ['open','high','low','close','volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df.dropna(subset=['open','high','low','close'])

# ─────────────────────────────────────────
# BSE — BATCH yf.download()
# ─────────────────────────────────────────

BSE_BATCH_SIZE = 100  # symbols per batch to avoid yfinance timeouts

def get_bse_stock_list():
    """Derive BSE universe from NSE symbols — most stocks are dual-listed."""
    day    = get_last_trading_day()
    nse_df = get_nse_ohlc(day)
    if nse_df is None:
        return []
    return nse_df['symbol'].tolist()

def get_bse_ohlc(date):
    """
    Returns BSE OHLC for a given date using yf.download() batch mode.
    Much faster than one-by-one Ticker.history() calls.
    """
    cache_path = os.path.join(DATA_DIR, f"bse_bhav_{date.strftime('%Y%m%d')}.csv")
    if os.path.exists(cache_path):
        print(f"[BSE] Loading cached file for {date}")
        df = pd.read_csv(cache_path)
        df['exchange'] = 'BSE'
        return df

    symbols = get_bse_stock_list()
    if not symbols:
        print("[BSE] Could not get stock list.")
        return None

    start_str = date.strftime("%Y-%m-%d")
    end_str   = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    tickers   = [s + ".BO" for s in symbols]

    print(f"[BSE] Batch downloading {len(tickers)} stocks for {date}...")

    records = []
    total_batches = (len(tickers) + BSE_BATCH_SIZE - 1) // BSE_BATCH_SIZE

    for batch_num in range(total_batches):
        batch_tickers  = tickers[batch_num * BSE_BATCH_SIZE:(batch_num + 1) * BSE_BATCH_SIZE]
        batch_symbols  = symbols[batch_num * BSE_BATCH_SIZE:(batch_num + 1) * BSE_BATCH_SIZE]

        try:
            raw = yf.download(
                tickers   = batch_tickers,
                start     = start_str,
                end       = end_str,
                interval  = '1d',
                group_by  = 'ticker',
                auto_adjust = True,
                progress  = False,
                threads   = True,
            )

            if raw.empty:
                continue

            # Handle single vs multi ticker response
            if len(batch_tickers) == 1:
                ticker = batch_tickers[0]
                sym    = batch_symbols[0]
                if not raw.empty:
                    row = raw.iloc[0]
                    records.append({
                        'symbol': sym,
                        'open':   float(row['Open']),
                        'high':   float(row['High']),
                        'low':    float(row['Low']),
                        'close':  float(row['Close']),
                    })
            else:
                for sym, ticker in zip(batch_symbols, batch_tickers):
                    try:
                        stock_df = raw[ticker] if ticker in raw.columns.get_level_values(0) else None
                        if stock_df is None or stock_df.empty:
                            continue
                        stock_df = stock_df.dropna(subset=['Close'])
                        if stock_df.empty:
                            continue
                        row = stock_df.iloc[0]
                        records.append({
                            'symbol': sym,
                            'open':   float(row['Open']),
                            'high':   float(row['High']),
                            'low':    float(row['Low']),
                            'close':  float(row['Close']),
                        })
                    except Exception:
                        pass

        except Exception as e:
            print(f"[BSE] Batch {batch_num+1} error: {e}")
            continue

        print(f"[BSE] Batch {batch_num+1}/{total_batches} done — {len(records)} stocks so far")

    if not records:
        print("[BSE] No data retrieved.")
        return None

    result = pd.DataFrame(records)
    result.to_csv(cache_path, index=False)
    result['exchange'] = 'BSE'
    print(f"[BSE] Done. Got {len(result)} stocks.")
    return result

def get_all_ohlc(date):
    """
    Returns unified OHLC for all stocks.
    NSE preferred for duplicates. BSE-only stocks added after.
    Returns single DataFrame with 'exchange' column showing primary exchange.
    """
    nse_df = get_nse_ohlc(date)
    bse_df = get_bse_ohlc(date)

    if nse_df is None and bse_df is None:
        return None
    if nse_df is None:
        bse_df['exchange'] = 'BSE'
        return bse_df
    if bse_df is None:
        nse_df['exchange'] = 'NSE'
        return nse_df

    # NSE symbols take priority
    nse_syms = set(nse_df['symbol'].str.upper())

    # keep only BSE stocks not already in NSE
    bse_only = bse_df[~bse_df['symbol'].str.upper().isin(nse_syms)].copy()
    bse_only['exchange'] = 'BSE'
    nse_df['exchange']   = 'NSE'

    combined = pd.concat([nse_df, bse_only], ignore_index=True)
    print(f"[All] Unified: {len(nse_df)} NSE + {len(bse_only)} BSE-only = {len(combined)} total")
    return combined

# ─────────────────────────────────────────
# WEEKLY & MONTHLY OHLC AGGREGATION
# ─────────────────────────────────────────

def aggregate_ohlc(frames):
    all_data = pd.concat(frames)
    return all_data.groupby(['symbol', 'exchange']).agg(
        open=('open',   'first'),
        high=('high',   'max'),
        low=('low',     'min'),
        close=('close', 'last')
    ).reset_index()

def get_weekly_ohlc_nse():
    start, end = get_previous_week_range()
    frames  = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            df = get_nse_ohlc(current)
            if df is not None:
                frames.append(df)
        current += timedelta(days=1)
    if not frames:
        print("[NSE Weekly] No data found.")
        return None
    return aggregate_ohlc(frames)

def get_monthly_ohlc_nse():
    start, end = get_previous_month_range()
    frames  = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            df = get_nse_ohlc(current)
            if df is not None:
                frames.append(df)
        current += timedelta(days=1)
    if not frames:
        print("[NSE Monthly] No data found.")
        return None
    return aggregate_ohlc(frames)

def get_weekly_ohlc_bse():
    start, end = get_previous_week_range()
    frames  = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            df = get_bse_ohlc(current)
            if df is not None:
                frames.append(df)
        current += timedelta(days=1)
    if not frames:
        print("[BSE Weekly] No data found.")
        return None
    return aggregate_ohlc(frames)

def get_monthly_ohlc_bse():
    start, end = get_previous_month_range()
    frames  = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            df = get_bse_ohlc(current)
            if df is not None:
                frames.append(df)
        current += timedelta(days=1)
    if not frames:
        print("[BSE Monthly] No data found.")
        return None
    return aggregate_ohlc(frames)

# ─────────────────────────────────────────
# F&O STOCK LIST
# ─────────────────────────────────────────

def get_fo_symbols():
    cache_path = os.path.join(DATA_DIR, "fo_symbols.csv")
    if os.path.exists(cache_path):
        age = datetime.today().timestamp() - os.path.getmtime(cache_path)
        if age < 86400:
            df = pd.read_csv(cache_path)
            print(f"[F&O] Loaded {len(df)} symbols from cache.")
            return set(df['symbol'].tolist())

    url     = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
    try:
        print("[F&O] Downloading F&O stock list from NSE...")
        r = requests.get(url, headers=NSE_HEADERS, timeout=15)
        if r.status_code == 200:
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))
            df.columns = df.columns.str.strip()
            sym_col = None
            for col in df.columns:
                if 'symbol' in col.lower() or 'scrip' in col.lower():
                    sym_col = col
                    break
            if sym_col is None:
                print(f"[F&O] Could not find symbol column. Columns: {df.columns.tolist()}")
                return set()
            symbols = df[sym_col].str.strip().dropna().tolist()
            symbols = [s for s in symbols if s and not s.startswith('Underlying')]
            out = pd.DataFrame({'symbol': symbols})
            out.to_csv(cache_path, index=False)
            print(f"[F&O] Got {len(symbols)} F&O symbols.")
            return set(symbols)
        else:
            print(f"[F&O] Failed: HTTP {r.status_code}")
            return set()
    except Exception as e:
        print(f"[F&O] Error: {e}")
        return set()

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time

    day = get_last_trading_day()
    print(f"Last trading day: {day}")

    print("\n=== Testing NSE Daily ===")
    df_nse = get_nse_ohlc(day)
    if df_nse is not None:
        print(df_nse.head())
        print(f"Total NSE EQ stocks: {len(df_nse)}")

    print("\n=== Testing BSE Daily (batch mode) ===")
    t0     = time.time()
    df_bse = get_bse_ohlc(day)
    t1     = time.time()
    if df_bse is not None:
        print(df_bse.head())
        print(f"Total BSE stocks: {len(df_bse)}")
        print(f"Time taken: {round(t1-t0, 1)}s")