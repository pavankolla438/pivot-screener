import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day, get_nse_ohlc
from history_store import _store

VOL_AVG_PERIOD = 21

# ─────────────────────────────────────────
# VECTORIZED VOLUME STATS FROM BULK STORE
# ─────────────────────────────────────────

def compute_volume_stats_bulk(exchange):
    """
    Computes volume stats for ALL symbols at once using the bulk
    in-memory store. Returns dict: { symbol: { vol_ratio, vol_rising } }
    No disk reads, no per-symbol loops.
    """
    try:
        combined = _store[exchange]['1d']
    except KeyError:
        return {}

    if combined is None or combined.empty:
        return {}

    if 'volume' not in combined.columns:
        return {}

    results = {}

    # Work on a copy with numeric volume
    df = combined[['_sym', 'volume']].copy()
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df = df.dropna(subset=['volume'])

    # Group by symbol — vectorized within each group using tail
    for sym, grp in df.groupby('_sym', sort=False):
        vols = grp['volume'].values
        if len(vols) < 4:
            continue

        today_vol  = float(vols[-1])
        hist_vols  = vols[-VOL_AVG_PERIOD-1:-1] if len(vols) > VOL_AVG_PERIOD else vols[:-1]
        avg_vol    = float(np.mean(hist_vols)) if len(hist_vols) > 0 else 0
        vol_ratio  = round(today_vol / avg_vol, 2) if avg_vol > 0 else 0
        vol_rising = bool(vols[-1] > vols[-2] > vols[-3])

        results[sym] = {
            'vol_ratio':  vol_ratio,
            'vol_rising': vol_rising,
        }

    return results


# ─────────────────────────────────────────
# SINGLE SYMBOL STATS (FALLBACK)
# ─────────────────────────────────────────

def get_volume_stats(symbol, exchange):
    """Fallback for single symbol lookup."""
    from history_store import get_history
    df = get_history(symbol, exchange, interval='1d')
    if df is None or df.empty or 'volume' not in df.columns:
        return None
    try:
        df = df.tail(VOL_AVG_PERIOD + 3).copy()
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df = df.dropna(subset=['volume'])
        if len(df) < 4:
            return None
        vols      = df['volume'].values
        today_vol = float(vols[-1])
        hist_vols = vols[-VOL_AVG_PERIOD-1:-1] if len(vols) > VOL_AVG_PERIOD else vols[:-1]
        avg_vol   = float(np.mean(hist_vols)) if len(hist_vols) > 0 else 0
        vol_ratio  = round(today_vol / avg_vol, 2) if avg_vol > 0 else 0
        vol_rising = bool(len(vols) >= 3 and vols[-1] > vols[-2] > vols[-3])
        return {
            'today_vol':  int(today_vol),
            'avg_vol_21': round(avg_vol, 0),
            'vol_ratio':  vol_ratio,
            'vol_rising': vol_rising,
        }
    except Exception:
        return None


# ─────────────────────────────────────────
# ENRICH SCAN RESULTS
# ─────────────────────────────────────────

def enrich_with_volume(df, exchange_col='Exchange', symbol_col='Symbol'):
    """
    Enriches scan results DataFrame with volume columns.
    Uses bulk pre-computed stats — zero disk reads, no per-symbol loops.
    """
    if df.empty:
        return df

    # Pre-compute stats for all exchanges present in results
    bulk_stats = {}
    for exch in df[exchange_col].unique():
        bulk_stats[exch] = compute_volume_stats_bulk(exch)

    vol_ratios  = []
    vol_risings = []

    for _, row in df.iterrows():
        sym   = row[symbol_col]
        exch  = row[exchange_col]
        stats = bulk_stats.get(exch, {}).get(sym)

        if stats:
            vol_ratios.append(stats['vol_ratio'])
            vol_risings.append('✅' if stats['vol_rising'] else '')
        else:
            # Fallback to single lookup
            s = get_volume_stats(sym, exch)
            vol_ratios.append(s['vol_ratio']  if s else None)
            vol_risings.append('✅' if s and s['vol_rising'] else '')

    df = df.copy()
    df['Vol Ratio']  = vol_ratios
    df['Vol Rising'] = vol_risings
    return df