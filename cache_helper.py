import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

CACHE_DIR = r"C:\pivot_screener\data\yf_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(symbol, exchange, interval):
    today = datetime.today().strftime("%Y%m%d")
    return os.path.join(CACHE_DIR, f"{symbol}_{exchange}_{interval}_{today}.csv")

def fetch_history_cached(symbol, exchange, interval='1d', lookback_bars=60):
    cache_file = _cache_path(symbol, exchange, interval)

    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df
        except Exception:
            pass

    suffix = ".NS" if exchange == "NSE" else ".BO"
    ticker = symbol + suffix

    if interval == '1wk':
        start = datetime.today() - timedelta(weeks=max(lookback_bars * 2, 52))
    else:
        start = datetime.today() - timedelta(days=lookback_bars * 2)

    try:
        # Use yf.download for single symbol — faster than Ticker.history
        raw = yf.download(
            tickers     = ticker,
            start       = start.strftime("%Y-%m-%d"),
            interval    = interval,
            auto_adjust = True,
            progress    = False,
            threads     = False,
        )
        if raw.empty:
            pd.DataFrame().to_csv(cache_file)
            return None

        # yf.download returns MultiIndex columns for single ticker too
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.droplevel(1)

        cols = [c for c in ['Open','High','Low','Close','Volume'] if c in raw.columns]
        df   = raw[cols].copy()
        df.columns = [c.lower() for c in df.columns]
        df   = df.dropna(subset=['open','high','low','close'])
        df.to_csv(cache_file)
        return df

    except Exception:
        return None

def fetch_histories_batch(symbols, exchange, interval='1d', lookback_bars=60):
    """
    Batch fetch histories for multiple symbols using yf.download().
    Returns dict: { symbol: df }
    Only fetches symbols not already cached.
    """
    results       = {}
    to_fetch      = []
    to_fetch_syms = []

    suffix = ".NS" if exchange == "NSE" else ".BO"

    # Check cache first
    for sym in symbols:
        cache_file = _cache_path(sym, exchange, interval)
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if not df.empty:
                    df.columns = [c.lower() for c in df.columns]
                    results[sym] = df
                    continue
            except Exception:
                pass
        to_fetch.append(sym + suffix)
        to_fetch_syms.append(sym)

    if not to_fetch:
        return results

    if interval == '1wk':
        start = datetime.today() - timedelta(weeks=max(lookback_bars * 2, 52))
    else:
        start = datetime.today() - timedelta(days=lookback_bars * 2)

    print(f"[BatchFetch] {exchange} {interval} — fetching {len(to_fetch)} symbols from yfinance...")

    BATCH = 200
    for i in range(0, len(to_fetch), BATCH):
        batch_tickers = to_fetch[i:i + BATCH]
        batch_symbols = to_fetch_syms[i:i + BATCH]

        try:
            raw = yf.download(
                tickers     = batch_tickers,
                start       = start.strftime("%Y-%m-%d"),
                interval    = interval,
                group_by    = 'ticker',
                auto_adjust = True,
                progress    = False,
                threads     = True,
            )

            if raw.empty:
                continue

            for sym, ticker in zip(batch_symbols, batch_tickers):
                try:
                    if len(batch_tickers) == 1:
                        df = raw.copy()
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.droplevel(1)
                    else:
                        if ticker not in raw.columns.get_level_values(0):
                            continue
                        df = raw[ticker].copy()

                    cols = [c for c in ['Open','High','Low','Close','Volume'] if c in df.columns]
                    df   = df[cols].copy()
                    df.columns = [c.lower() for c in df.columns]
                    df   = df.dropna(subset=['open','high','low','close'])

                    if df.empty:
                        continue

                    cache_file = _cache_path(sym, exchange, interval)
                    df.to_csv(cache_file)
                    results[sym] = df

                except Exception:
                    pass

        except Exception as e:
            print(f"[BatchFetch] Batch error: {e}")
            continue

        print(f"[BatchFetch] {i + len(batch_tickers)}/{len(to_fetch)} done")

    return results

def clear_old_cache(days_to_keep=2):
    cutoff = datetime.today() - timedelta(days=days_to_keep)
    deleted = 0
    for fname in os.listdir(CACHE_DIR):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if mtime < cutoff:
                os.remove(fpath)
                deleted += 1
        except Exception:
            pass
    if deleted:
        print(f"[Cache] Cleaned up {deleted} old cache files.")

# ─────────────────────────────────────────
# BULK CACHE — single file per interval
# ─────────────────────────────────────────

BULK_CACHE_DIR = r"C:\pivot_screener\data\bulk_cache"
os.makedirs(BULK_CACHE_DIR, exist_ok=True)

def _bulk_cache_path(exchange, interval):
    today = datetime.today().strftime("%Y%m%d")
    return os.path.join(BULK_CACHE_DIR, f"{exchange}_{interval}_{today}.parquet")

def save_bulk_cache(data_dict, exchange, interval):
    """
    Saves all symbol histories into a single parquet file.
    data_dict: { symbol: df }
    """
    frames = []
    for sym, df in data_dict.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        df['_sym'] = sym
        frames.append(df)
    if not frames:
        return
    combined = pd.concat(frames)
    path = _bulk_cache_path(exchange, interval)
    combined.to_parquet(path)
    print(f"[BulkCache] Saved {len(data_dict)} symbols → {os.path.basename(path)}")

def load_bulk_cache(exchange, interval):
    """
    Loads combined parquet file.
    Returns raw combined DataFrame with '_sym' column.
    """
    path = _bulk_cache_path(exchange, interval)
    if not os.path.exists(path):
        return None
    try:
        combined = pd.read_parquet(path)
        syms = combined['_sym'].nunique()
        print(f"[BulkCache] Loaded {syms} symbols from {os.path.basename(path)}")
        return combined
    except Exception as e:
        print(f"[BulkCache] Load error: {e}")
        return None

def clear_old_bulk_cache(days_to_keep=2):
    cutoff = datetime.today() - timedelta(days=days_to_keep)
    for fname in os.listdir(BULK_CACHE_DIR):
        fpath = os.path.join(BULK_CACHE_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if mtime < cutoff:
                os.remove(fpath)
        except Exception:
            pass