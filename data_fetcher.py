import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
import zipfile
import io

DATA_DIR = os.environ.get("DATA_ROOT", r"C:\pivot_screener\data")
os.makedirs(DATA_DIR, exist_ok=True)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com"
}

# ─────────────────────────────────────────
# NSE HOLIDAYS
# ─────────────────────────────────────────

NSE_HOLIDAYS = {
    date(2024, 1, 22),
    date(2024, 1, 26),
    date(2024, 3, 25),
    date(2024, 3, 29),
    date(2024, 4, 14),
    date(2024, 4, 17),
    date(2024, 4, 21),
    date(2024, 5, 23),
    date(2024, 6, 17),
    date(2024, 7, 17),
    date(2024, 8, 15),
    date(2024, 10, 2),
    date(2024, 10, 14),
    date(2024, 11, 1),
    date(2024, 11, 15),
    date(2024, 12, 25),

    date(2025, 2, 26),
    date(2025, 3, 14),
    date(2025, 3, 31),
    date(2025, 4, 10),
    date(2025, 4, 14),
    date(2025, 4, 18),
    date(2025, 5, 1),
    date(2025, 8, 15),
    date(2025, 8, 27),
    date(2025, 10, 2),
    date(2025, 10, 21),
    date(2025, 10, 22),
    date(2025, 11, 5),
    date(2025, 12, 25),

    date(2026, 1, 26),
    date(2026, 2, 17),
    date(2026, 3, 20),
    date(2026, 4, 2),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 9, 16),
    date(2026, 10, 2),
    date(2026, 10, 29),
    date(2026, 11, 24),
    date(2026, 12, 25),
}

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NSE_HOLIDAYS


# ─────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────

_last_trading_day_cache = {}

def get_last_trading_day():
    """
    Returns most recent NSE trading day for which bhavcopy data exists locally.
    Only caches a date if its bhavcopy file exists.
    """
    today     = datetime.today().date()
    today_str = str(today)

    if today_str in _last_trading_day_cache:
        return _last_trading_day_cache[today_str]

    candidate = today

    for _ in range(14):

        if not is_trading_day(candidate):
            candidate -= timedelta(days=1)
            continue

        cache_path = os.path.join(
            DATA_DIR,
            f"nse_bhav_{candidate.strftime('%Y%m%d')}.csv"
        )

        # Only accept and cache if bhavcopy file exists
        if os.path.exists(cache_path):
            _last_trading_day_cache[today_str] = candidate
            return candidate

        candidate -= timedelta(days=1)

    raise RuntimeError("Could not determine last trading day in the past 14 days.")


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
        'TradDt':      'date',
        'TckrSymb':    'symbol',
        'OpnPric':     'open',
        'HghPric':     'high',
        'LwPric':      'low',
        'ClsPric':     'close',
        'SctySrs':     'series',
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


def get_all_ohlc(date):
    df = get_nse_ohlc(date)
    if df is not None:
        print(f"[NSE] {len(df)} stocks loaded.")
    return df


# ─────────────────────────────────────────
# WEEKLY & MONTHLY OHLC
# ─────────────────────────────────────────

def aggregate_ohlc(frames):
    all_data = pd.concat(frames)

    return all_data.groupby(['symbol', 'exchange']).agg(
        open=('open','first'),
        high=('high','max'),
        low=('low','min'),
        close=('close','last')
    ).reset_index()


def get_weekly_ohlc_nse():

    start, end = get_previous_week_range()

    frames  = []
    current = start

    while current <= end:

        if is_trading_day(current):
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

        if is_trading_day(current):
            df = get_nse_ohlc(current)
            if df is not None:
                frames.append(df)

        current += timedelta(days=1)

    if not frames:
        print("[NSE Monthly] No data found.")
        return None

    return aggregate_ohlc(frames)


# ─────────────────────────────────────────
# F&O SYMBOLS
# ─────────────────────────────────────────

def get_fo_symbols():

    cache_path = os.path.join(DATA_DIR, "fo_symbols.csv")

    if os.path.exists(cache_path):

        age = datetime.today().timestamp() - os.path.getmtime(cache_path)

        if age < 86400:
            df = pd.read_csv(cache_path)
            print(f"[F&O] Loaded {len(df)} symbols from cache.")
            return set(df['symbol'].tolist())

    url = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"

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
                print(f"[F&O] Could not find symbol column.")
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