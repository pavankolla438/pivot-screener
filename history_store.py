import os
import pickle
import pandas as pd
from cache_helper import fetch_histories_batch, load_bulk_cache

# ─────────────────────────────────────────
# IN-MEMORY HISTORY STORE
# ─────────────────────────────────────────
# All exchange keys normalized to 'NSE' — single source of truth.
# _store       : { 'NSE': { interval: combined_df with '_sym' col } }
# _index       : { 'NSE': { interval: { symbol: df } } }  — O(1) lookup
# _swing_cache : { 'NSE_1d': { sym: { swing_high_idxs, swing_low_idxs } } }

_store       = {}
_index       = {}
_swing_cache = {}

_NSE = 'NSE'   # single canonical key — never 'ALL' or 'BOTH'

def _norm(exchange):
    """Normalize any exchange variant to canonical 'NSE'."""
    return _NSE


# ─────────────────────────────────────────
# BUILD INDEX
# ─────────────────────────────────────────

def _build_index(exchange, interval):
    global _index
    exch = _norm(exchange)
    if exch not in _index:
        _index[exch] = {}

    combined = _store.get(exch, {}).get(interval)
    if combined is None or combined.empty:
        _index[exch][interval] = {}
        return

    print(f"[Index] Building {exch} {interval} symbol index...")
    idx = {}
    for sym, grp in combined.groupby('_sym', sort=False):
        df = grp.drop(columns=['_sym'])
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        # Drop duplicate dates — yfinance occasionally produces them for splits/adjustments
        df = df[~df.index.duplicated(keep='last')]
        idx[sym] = df
    _index[exch][interval] = idx
    print(f"[Index] {exch} {interval}: {len(idx)} symbols indexed")


# ─────────────────────────────────────────
# SWING CACHE — compute + disk persistence
# Saved as pkl alongside bulk parquet so prominence
# filter (~8s) is skipped on warm starts.
# ─────────────────────────────────────────

def _swing_cache_path(exchange, interval):
    from cache_helper import BULK_CACHE_DIR
    from data_fetcher import get_last_trading_day
    exch = _norm(exchange)
    day  = get_last_trading_day().strftime("%Y%m%d")
    return os.path.join(BULK_CACHE_DIR, f"{exch}_{interval}_{day}_swings.pkl")


def _save_swing_cache(exchange, interval):
    exch = _norm(exchange)
    key  = f"{exch}_{interval}"
    data = _swing_cache.get(key)
    if not data:
        return
    path = _swing_cache_path(exch, interval)
    try:
        with open(path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"[SwingCache] Saved {len(data)} symbols -> {os.path.basename(path)}")
    except Exception as e:
        print(f"[SwingCache] Save failed: {e}")


def _load_swing_cache(exchange, interval):
    path = _swing_cache_path(exchange, interval)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
        print(f"[SwingCache] Loaded {len(data)} symbols from {os.path.basename(path)}")
        return data
    except Exception as e:
        print(f"[SwingCache] Load failed ({e}), will recompute")
        return None


def _build_swing_cache(exchange, interval, window=5, min_prominence_pct=0.5):
    global _swing_cache
    exch     = _norm(exchange)
    key      = f"{exch}_{interval}"
    combined = _store.get(exch, {}).get(interval)

    if combined is None or combined.empty:
        _swing_cache[key] = {}
        return

    print(f"[SwingCache] Computing swings for {exch} {interval}...")

    win   = 2 * window + 1
    cache = {}

    for sym, grp in combined.groupby('_sym', sort=False):
        g     = grp.drop(columns=['_sym']).reset_index(drop=True)
        highs = pd.to_numeric(g['high'], errors='coerce')
        lows  = pd.to_numeric(g['low'],  errors='coerce')

        roll_max = highs.rolling(win, center=True, min_periods=win).max()
        roll_min = lows.rolling(win,  center=True, min_periods=win).min()

        raw_high_idxs = list(highs.index[highs == roll_max])
        raw_low_idxs  = list(lows.index[lows   == roll_min])

        min_prom = min_prominence_pct / 100.0

        filtered_highs = []
        for i in raw_high_idxs:
            left     = max(0, i - window)
            right    = min(len(highs), i + window + 1)
            surround = [highs.iloc[j] for j in range(left, right) if j != i]
            if not surround:
                continue
            avg = sum(surround) / len(surround)
            if avg > 0 and (highs.iloc[i] - avg) / avg >= min_prom:
                filtered_highs.append(i)

        filtered_lows = []
        for i in raw_low_idxs:
            left     = max(0, i - window)
            right    = min(len(lows), i + window + 1)
            surround = [lows.iloc[j] for j in range(left, right) if j != i]
            if not surround:
                continue
            avg = sum(surround) / len(surround)
            if avg > 0 and (avg - lows.iloc[i]) / avg >= min_prom:
                filtered_lows.append(i)

        cache[sym] = {
            'swing_high_idxs': filtered_highs,
            'swing_low_idxs':  filtered_lows,
        }

    _swing_cache[key] = cache
    print(f"[SwingCache] {exch} {interval}: {len(cache)} symbols cached")
    _save_swing_cache(exch, interval)


def _ensure_swing_cache(exch, interval):
    """Load swing cache from disk if available, else compute and save."""
    key = f"{exch}_{interval}"
    if key in _swing_cache:
        return
    cached = _load_swing_cache(exch, interval)
    if cached is not None:
        _swing_cache[key] = cached
        return
    # Not on disk — compute and save
    if interval == '1wk':
        _build_swing_cache(exch, interval, window=7, min_prominence_pct=2.0)
    else:
        _build_swing_cache(exch, interval, window=5, min_prominence_pct=0.5)


# ─────────────────────────────────────────
# PRELOAD
# ─────────────────────────────────────────

def preload_histories(symbols, exchange, intervals=('1d', '1wk'), lookback_bars=252):
    global _store
    exch = _norm(exchange)

    if exch not in _store:
        _store[exch] = {}

    for interval in intervals:
        # Already in memory and sufficiently complete?
        if interval in _store[exch] and _store[exch][interval] is not None:
            loaded = _store[exch][interval]['_sym'].nunique()
            if loaded >= len(symbols) * 0.9:
                print(f"[Store] {exch} {interval}: already in memory ({loaded} symbols)")
                if exch not in _index or interval not in _index.get(exch, {}):
                    _build_index(exch, interval)
                _ensure_swing_cache(exch, interval)
                continue

        # Try bulk parquet cache
        combined = load_bulk_cache(exch, interval)

        if combined is not None:
            loaded_syms = set(combined['_sym'].unique())
            missing     = [s for s in symbols if s not in loaded_syms]
            if missing:
                print(f"[Store] {exch} {interval}: {len(missing)} missing, fetching...")
                fetched = fetch_histories_batch(
                    missing, exch,
                    interval=interval,
                    lookback_bars=lookback_bars
                )
                new_frames = []
                for sym, df in fetched.items():
                    if df is not None and not df.empty:
                        df = df.tail(lookback_bars).copy()
                        df['_sym'] = sym
                        new_frames.append(df)
                if new_frames:
                    combined = pd.concat([combined] + new_frames)
                    _save_bulk(combined, exch, interval)
            _store[exch][interval] = combined
        else:
            print(f"[Store] {exch} {interval}: no bulk cache, fetching all...")
            fetched = fetch_histories_batch(
                symbols, exch,
                interval=interval,
                lookback_bars=lookback_bars
            )
            frames = []
            for sym, df in fetched.items():
                if df is not None and not df.empty:
                    df = df.tail(lookback_bars).copy()
                    df['_sym'] = sym
                    frames.append(df)
            if frames:
                combined = pd.concat(frames)
                _save_bulk(combined, exch, interval)
                _store[exch][interval] = combined
            else:
                _store[exch][interval] = None

        _build_index(exch, interval)
        _ensure_swing_cache(exch, interval)

        if _store[exch][interval] is not None:
            valid = _store[exch][interval]['_sym'].nunique()
            print(f"[Store] {exch} {interval}: {valid}/{len(symbols)} symbols loaded")


def _save_bulk(combined_df, exchange, interval):
    from cache_helper import _bulk_cache_path
    exch = _norm(exchange)
    path = _bulk_cache_path(exch, interval)
    combined_df.to_parquet(path)
    syms = combined_df['_sym'].nunique()
    print(f"[BulkCache] Saved {syms} symbols -> {os.path.basename(path)}")


# ─────────────────────────────────────────
# GET HISTORY — O(1) lookup
# ─────────────────────────────────────────

def get_history(symbol, exchange, interval='1d'):
    exch = _norm(exchange)
    try:
        df = _index[exch][interval].get(symbol)
        if df is not None:
            return df
    except KeyError:
        pass
    from cache_helper import fetch_history_cached
    return fetch_history_cached(symbol, _NSE, interval=interval, lookback_bars=252)


# ─────────────────────────────────────────
# GET ALL HISTORIES — full dict for scanners
# ─────────────────────────────────────────

def get_all_histories(exchange, interval='1d'):
    exch = _norm(exchange)
    try:
        return _index[exch][interval]
    except KeyError:
        return {}


# ─────────────────────────────────────────
# GET SWING POINTS — O(1) lookup
# ─────────────────────────────────────────

def get_swing_points(symbol, exchange, interval='1d'):
    exch  = _norm(exchange)
    key   = f"{exch}_{interval}"
    entry = _swing_cache.get(key, {}).get(symbol, {})
    return (
        entry.get('swing_high_idxs', []),
        entry.get('swing_low_idxs',  []),
    )


# ─────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────

def clear_store(exchange=None):
    global _store, _index, _swing_cache
    if exchange:
        exch = _norm(exchange)
        _store.pop(exch, None)
        _index.pop(exch, None)
        for k in [k for k in _swing_cache if k.startswith(exch)]:
            _swing_cache.pop(k, None)
    else:
        _store       = {}
        _index       = {}
        _swing_cache = {}
    # Delete swing pkl files so they're rebuilt fresh on next preload
    try:
        from cache_helper import BULK_CACHE_DIR
        for fname in os.listdir(BULK_CACHE_DIR):
            if fname.endswith('_swings.pkl'):
                if exchange is None or fname.startswith(_norm(exchange)):
                    os.remove(os.path.join(BULK_CACHE_DIR, fname))
    except Exception:
        pass
    print(f"[Store] Cleared {'all' if not exchange else exchange}")


def store_stats():
    for exch, intervals in _index.items():
        for interval, idx in intervals.items():
            key        = f"{exch}_{interval}"
            swing_syms = len(_swing_cache.get(key, {}))
            print(f"  {exch} {interval}: {len(idx)} indexed, {swing_syms} swing-cached")
