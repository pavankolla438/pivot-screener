import os
import pickle
import pandas as pd
from cache_helper import fetch_histories_batch, load_bulk_cache

# ─────────────────────────────────────────
# IN-MEMORY HISTORY STORE
# ─────────────────────────────────────────
# _store       : { exchange: { interval: combined_df with '_sym' col } }
# _index       : { exchange: { interval: { symbol: df } } }  — O(1) lookup
# _swing_cache : { 'NSE_1d': { sym: { swing_high_idxs, swing_low_idxs } } }

_store       = {}
_index       = {}
_swing_cache = {}

# ─────────────────────────────────────────
# BUILD INDEX
# ─────────────────────────────────────────

def _build_index(exchange, interval):
    global _index
    if exchange not in _index:
        _index[exchange] = {}

    combined = _store.get(exchange, {}).get(interval)
    if combined is None or combined.empty:
        _index[exchange][interval] = {}
        return

    print(f"[Index] Building {exchange} {interval} symbol index...")
    idx = {}
    for sym, grp in combined.groupby('_sym', sort=False):
        df = grp.drop(columns=['_sym'])
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df[~df.index.duplicated(keep='last')]
        idx[sym] = df
    _index[exchange][interval] = idx
    print(f"[Index] {exchange} {interval}: {len(idx)} symbols indexed")

# ─────────────────────────────────────────
# BUILD SWING CACHE
# Per-symbol rolling so positions match
# the reset-index DataFrames in _index.
# ─────────────────────────────────────────

def _build_swing_cache(exchange, interval, window=5, min_prominence_pct=0.5):
    global _swing_cache
    key      = f"{exchange}_{interval}"
    combined = _store.get(exchange, {}).get(interval)

    if combined is None or combined.empty:
        _swing_cache[key] = {}
        return

    print(f"[SwingCache] Computing swings for {exchange} {interval}...")

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

        # prominence filter — swing high must be min_prominence_pct% above
        # the average of surrounding bars
        min_prom = min_prominence_pct / 100.0

        filtered_highs = []
        for i in raw_high_idxs:
            left  = max(0, i - window)
            right = min(len(highs), i + window + 1)
            surround = [highs.iloc[j] for j in range(left, right) if j != i]
            if not surround:
                continue
            avg = sum(surround) / len(surround)
            if avg > 0 and (highs.iloc[i] - avg) / avg >= min_prom:
                filtered_highs.append(i)

        filtered_lows = []
        for i in raw_low_idxs:
            left  = max(0, i - window)
            right = min(len(lows), i + window + 1)
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
    print(f"[SwingCache] {exchange} {interval}: {len(cache)} symbols cached")
    _save_swing_cache(exchange, interval)


# ─────────────────────────────────────────
# SWING CACHE DISK PERSISTENCE
# Saved alongside the bulk parquet, keyed by last trading day.
# Avoids recomputing ~8s prominence filter on every process start.
# ─────────────────────────────────────────

def _swing_cache_path(exchange, interval):
    from cache_helper import BULK_CACHE_DIR
    from data_fetcher import get_last_trading_day
    exch = 'NSE' if exchange in ('ALL', 'BOTH') else exchange
    day  = get_last_trading_day().strftime("%Y%m%d")
    return os.path.join(BULK_CACHE_DIR, f"{exch}_{interval}_{day}_swings.pkl")

def _save_swing_cache(exchange, interval):
    key  = f"{exchange}_{interval}"
    data = _swing_cache.get(key)
    if not data:
        return
    path = _swing_cache_path(exchange, interval)
    try:
        with open(path, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"[SwingCache] Saved {len(data)} symbols → {os.path.basename(path)}")
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

# ─────────────────────────────────────────
# PRELOAD
# ─────────────────────────────────────────

def preload_histories(symbols, exchange, intervals=('1d', '1wk'), lookback_bars=252):
    global _store

    # normalize exchange key — everything is NSE data
    exch = 'NSE' if exchange in ('BOTH', 'ALL', 'NSE') else exchange

    if exch not in _store:
        _store[exch] = {}

    for interval in intervals:
        if interval in _store[exch] and _store[exch][interval] is not None:
            loaded = _store[exch][interval]['_sym'].nunique()
            if loaded >= len(symbols) * 0.9:
                print(f"[Store] {exch} {interval}: already in memory ({loaded} symbols)")
                if exch not in _index or interval not in _index.get(exch, {}):
                    _build_index(exch, interval)
                key = f"{exch}_{interval}"
                if key not in _swing_cache:
                    cached = _load_swing_cache(exch, interval)
                    if cached is not None:
                        _swing_cache[key] = cached
                    elif interval == '1wk':
                        _build_swing_cache(exch, interval, window=7, min_prominence_pct=2.0)
                    else:
                        _build_swing_cache(exch, interval, window=5, min_prominence_pct=0.5)
                continue

        combined = load_bulk_cache(exch, interval)

        if combined is not None:
            loaded_syms = set(combined['_sym'].unique())
            missing     = [s for s in symbols if s not in loaded_syms]
            if missing:
                print(f"[Store] {exch} {interval}: {len(missing)} missing, fetching...")
                # for ALL exchange, fetch missing from NSE first then BSE
                fetched = fetch_histories_batch(
                    missing, 'NSE',
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
        cached = _load_swing_cache(exch, interval)
        if cached is not None:
            _swing_cache[f"{exch}_{interval}"] = cached
        elif interval == '1wk':
            _build_swing_cache(exch, interval, window=7, min_prominence_pct=2.0)
        else:
            _build_swing_cache(exch, interval, window=5, min_prominence_pct=0.5)

        if _store[exch][interval] is not None:
            valid = _store[exch][interval]['_sym'].nunique()
            print(f"[Store] {exch} {interval}: {valid}/{len(symbols)} symbols loaded")

def _save_bulk(combined_df, exchange, interval):
    from cache_helper import _bulk_cache_path
    path = _bulk_cache_path(exchange, interval)
    combined_df.to_parquet(path)
    syms = combined_df['_sym'].nunique()
    print(f"[BulkCache] Saved {syms} symbols → {os.path.basename(path)}")

# ─────────────────────────────────────────
# GET HISTORY — O(1) lookup
# ─────────────────────────────────────────

def get_history(symbol, exchange, interval='1d'):
    exch = 'NSE' if exchange in ('BOTH', 'ALL', 'NSE') else exchange
    try:
        df = _index[exch][interval].get(symbol)
        if df is not None:
            return df
    except KeyError:
        pass
    # Always fall back to NSE — BSE removed
    from cache_helper import fetch_history_cached
    return fetch_history_cached(symbol, 'NSE', interval=interval, lookback_bars=252)


# ─────────────────────────────────────────
# GET ALL HISTORIES — full dict for vectorized scanners
# ─────────────────────────────────────────

def get_all_histories(exchange, interval='1d'):
    exch = 'NSE' if exchange in ('BOTH', 'ALL', 'NSE') else exchange
    try:
        return _index[exch][interval]
    except KeyError:
        return {}

# ─────────────────────────────────────────
# GET SWING POINTS — O(1) lookup
# ─────────────────────────────────────────

def get_swing_points(symbol, exchange, interval='1d'):
    exch  = 'NSE' if exchange in ('BOTH', 'ALL', 'NSE') else exchange
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
        _store.pop(exchange, None)
        _index.pop(exchange, None)
        for k in [k for k in _swing_cache if k.startswith(exchange)]:
            _swing_cache.pop(k, None)
    else:
        _store       = {}
        _index       = {}
        _swing_cache = {}
    # Also delete swing pkl files so they're rebuilt fresh on next preload
    try:
        from cache_helper import BULK_CACHE_DIR
        for fname in os.listdir(BULK_CACHE_DIR):
            if fname.endswith('_swings.pkl'):
                if exchange is None or fname.startswith(exchange):
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