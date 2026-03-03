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
        idx[sym] = df
    _index[exchange][interval] = idx
    print(f"[Index] {exchange} {interval}: {len(idx)} symbols indexed")

# ─────────────────────────────────────────
# BUILD SWING CACHE
# Per-symbol rolling so positions match
# the reset-index DataFrames in _index.
# ─────────────────────────────────────────

def _build_swing_cache(exchange, interval, window=3):
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
        # reset_index so positions match the per-symbol df in _index
        g     = grp.drop(columns=['_sym']).reset_index(drop=True)
        highs = pd.to_numeric(g['high'], errors='coerce')
        lows  = pd.to_numeric(g['low'],  errors='coerce')

        roll_max = highs.rolling(win, center=True, min_periods=win).max()
        roll_min = lows.rolling(win,  center=True, min_periods=win).min()

        cache[sym] = {
            'swing_high_idxs': list(highs.index[highs == roll_max]),
            'swing_low_idxs':  list(lows.index[lows   == roll_min]),
        }

    _swing_cache[key] = cache
    print(f"[SwingCache] {exchange} {interval}: {len(cache)} symbols cached")

# ─────────────────────────────────────────
# PRELOAD
# ─────────────────────────────────────────

def preload_histories(symbols, exchange, intervals=('1d', '1wk'), lookback_bars=60):
    global _store

    if exchange not in _store:
        _store[exchange] = {}

    for interval in intervals:
        # Already fully loaded
        if interval in _store[exchange] and _store[exchange][interval] is not None:
            loaded = _store[exchange][interval]['_sym'].nunique()
            if loaded >= len(symbols) * 0.9:
                print(f"[Store] {exchange} {interval}: already in memory ({loaded} symbols)")
                if exchange not in _index or interval not in _index.get(exchange, {}):
                    _build_index(exchange, interval)
                key = f"{exchange}_{interval}"
                if key not in _swing_cache:
                    _build_swing_cache(exchange, interval)
                continue

        # Try bulk parquet
        combined = load_bulk_cache(exchange, interval)

        if combined is not None:
            loaded_syms = set(combined['_sym'].unique())
            missing     = [s for s in symbols if s not in loaded_syms]

            if missing:
                print(f"[Store] {exchange} {interval}: "
                      f"{len(missing)} missing, fetching...")
                fetched = fetch_histories_batch(
                    missing, exchange,
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
                    _save_bulk(combined, exchange, interval)

            _store[exchange][interval] = combined

        else:
            print(f"[Store] {exchange} {interval}: no bulk cache, fetching all...")
            fetched = fetch_histories_batch(
                symbols, exchange,
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
                _save_bulk(combined, exchange, interval)
                _store[exchange][interval] = combined
            else:
                _store[exchange][interval] = None

        _build_index(exchange, interval)
        _build_swing_cache(exchange, interval)

        if _store[exchange][interval] is not None:
            valid = _store[exchange][interval]['_sym'].nunique()
            print(f"[Store] {exchange} {interval}: {valid}/{len(symbols)} symbols loaded")

def _save_bulk(combined_df, exchange, interval):
    from cache_helper import _bulk_cache_path
    path = _bulk_cache_path(exchange, interval)
    combined_df.to_parquet(path)
    syms = combined_df['_sym'].nunique()
    print(f"[BulkCache] Saved {syms} symbols → {path.split(chr(92))[-1]}")

# ─────────────────────────────────────────
# GET HISTORY — O(1) lookup
# ─────────────────────────────────────────

def get_history(symbol, exchange, interval='1d'):
    try:
        df = _index[exchange][interval].get(symbol)
        if df is not None:
            return df
    except KeyError:
        pass
    from cache_helper import fetch_history_cached
    return fetch_history_cached(symbol, exchange, interval=interval, lookback_bars=60)

# ─────────────────────────────────────────
# GET ALL HISTORIES — full dict for vectorized scanners
# ─────────────────────────────────────────

def get_all_histories(exchange, interval='1d'):
    try:
        return _index[exchange][interval]
    except KeyError:
        return {}

# ─────────────────────────────────────────
# GET SWING POINTS — O(1) lookup
# ─────────────────────────────────────────

def get_swing_points(symbol, exchange, interval='1d'):
    key   = f"{exchange}_{interval}"
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
    print(f"[Store] Cleared {'all' if not exchange else exchange}")

def store_stats():
    for exch, intervals in _index.items():
        for interval, idx in intervals.items():
            key        = f"{exch}_{interval}"
            swing_syms = len(_swing_cache.get(key, {}))
            print(f"  {exch} {interval}: {len(idx)} indexed, {swing_syms} swing-cached")