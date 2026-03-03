import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day
from history_store import get_all_histories
from market_context import get_context

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

VOL_AVG_PERIOD    = 21
TIGHT_RANGE_PCT   = 3.0    # max high-low range over last 5 bars
FLAT_MOVE_PCT     = 1.0    # max price move for "price flat"
VOL_SPIKE_RATIO   = 2.0    # min vol ratio for spike
VOL_BUILD_BARS    = 5      # bars to compare for volume building
LOW_52W_PCT       = 20.0   # within 20% of 52-week low
OBV_LOOKBACK      = 10     # bars for OBV trend
CONSOL_BARS       = 5      # bars for tight range check
VOL_RISE_DAYS     = 3      # consecutive days volume must rise

# ─────────────────────────────────────────
# SIGNAL DETECTORS
# ─────────────────────────────────────────

def signal_tight_range_vol_rising(df):
    """Tight price range (<3%) + volume rising 3+ consecutive days."""
    if len(df) < CONSOL_BARS + VOL_RISE_DAYS:
        return False
    recent     = df.iloc[-CONSOL_BARS:]
    high_range = float(recent['high'].max())
    low_range  = float(recent['low'].min())
    if low_range == 0:
        return False
    range_pct  = (high_range - low_range) / low_range * 100
    if range_pct > TIGHT_RANGE_PCT:
        return False
    # Volume rising consecutively
    vols = df['volume'].values
    for i in range(1, VOL_RISE_DAYS + 1):
        if vols[-i] <= vols[-(i + 1)]:
            return False
    return True

def signal_vol_spike_price_flat(df):
    """Volume spike (>2x avg) + price movement flat (<1%)."""
    if len(df) < VOL_AVG_PERIOD + 2:
        return False
    vols      = pd.to_numeric(df['volume'], errors='coerce').values
    today_vol = float(vols[-1])
    avg_vol   = float(np.mean(vols[-VOL_AVG_PERIOD-1:-1]))
    if avg_vol == 0:
        return False
    if today_vol / avg_vol < VOL_SPIKE_RATIO:
        return False
    # Price flat — today's move < 1%
    today_open  = float(df.iloc[-1]['open'])  if 'open'  in df.columns else None
    today_close = float(df.iloc[-1]['close'])
    prev_close  = float(df.iloc[-2]['close'])
    if prev_close == 0:
        return False
    move_pct = abs(today_close - prev_close) / prev_close * 100
    return move_pct < FLAT_MOVE_PCT

def signal_obv_rising(df):
    """OBV trending up over last OBV_LOOKBACK bars."""
    if len(df) < OBV_LOOKBACK + 1:
        return False
    closes = pd.to_numeric(df['close'],  errors='coerce').values
    vols   = pd.to_numeric(df['volume'], errors='coerce').values
    # Compute OBV
    obv    = np.zeros(len(closes))
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv[i] = obv[i-1] + vols[i]
        elif closes[i] < closes[i-1]:
            obv[i] = obv[i-1] - vols[i]
        else:
            obv[i] = obv[i-1]
    # Check OBV rising over last OBV_LOOKBACK bars using linear regression slope
    recent_obv = obv[-OBV_LOOKBACK:]
    x          = np.arange(len(recent_obv))
    slope      = np.polyfit(x, recent_obv, 1)[0]
    return slope > 0

def signal_near_52w_low_vol_building(df):
    """Price within 20% of 52-week low + volume building."""
    if len(df) < VOL_BUILD_BARS + 20:
        return False
    lows        = pd.to_numeric(df['low'],    errors='coerce').values
    vols        = pd.to_numeric(df['volume'], errors='coerce').values
    low_52w     = float(np.min(lows))
    current_low = float(lows[-1])
    if low_52w == 0:
        return False
    pct_from_low = (current_low - low_52w) / low_52w * 100
    if pct_from_low > LOW_52W_PCT:
        return False
    # Volume building — avg of last 5 days > avg of prior 20 days
    recent_vol_avg = float(np.mean(vols[-VOL_BUILD_BARS:]))
    prior_vol_avg  = float(np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS]))
    return recent_vol_avg > prior_vol_avg

# ─────────────────────────────────────────
# SCORE A SYMBOL
# ─────────────────────────────────────────

def score_symbol(df):
    """
    Returns dict of signals fired and total score.
    Score 1-4 based on how many signals are present.
    """
    s1 = signal_tight_range_vol_rising(df)
    s2 = signal_vol_spike_price_flat(df)
    s3 = signal_obv_rising(df)
    s4 = signal_near_52w_low_vol_building(df)

    score = sum([s1, s2, s3, s4])
    tags  = []
    if s1: tags.append('Tight+Vol↑')
    if s2: tags.append('Vol Spike')
    if s3: tags.append('OBV↑')
    if s4: tags.append('52W Low')

    return {
        'score':             score,
        'signals':           ' + '.join(tags) if tags else '',
        'tight_vol_rising':  s1,
        'vol_spike_flat':    s2,
        'obv_rising':        s3,
        'near_52w_low':      s4,
    }

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_accumulation_scan(exchange='BOTH', min_score=1):
    day = get_last_trading_day()
    print(f"\n=== Accumulation Scan | {day} | Exchange: {exchange} ===\n")

    results  = []
    contexts = get_context(exchange)

    for exch, ctx in contexts.items():
        if ctx is None or ctx.daily is None:
            print(f"[{exch}] No context available, skipping.")
            continue

        print(f"--- {exch} ---")
        daily_map = {
            row['symbol']: row
            for _, row in ctx.daily.iterrows()
        }

        histories = get_all_histories(exch, '1d')
        flagged   = 0

        for sym, df in histories.items():
            if df is None or len(df) < 30:
                continue
            today = daily_map.get(sym)
            if today is None:
                continue

            # Need volume column
            if 'volume' not in df.columns:
                continue

            scored = score_symbol(df)
            if scored['score'] < min_score:
                continue

            # Volume stats
            vols      = pd.to_numeric(df['volume'], errors='coerce').values
            today_vol = int(vols[-1]) if not np.isnan(vols[-1]) else 0
            avg_vol   = round(float(np.mean(vols[-VOL_AVG_PERIOD-1:-1])), 0)
            vol_ratio = round(today_vol / avg_vol, 2) if avg_vol > 0 else 0

            results.append({
                'Symbol':       sym,
                'Exchange':     exch,
                'Price':        round(float(today['close']), 2),
                'Score':        scored['score'],
                'Signals':      scored['signals'],
                'Tight+Vol↑':   '✅' if scored['tight_vol_rising'] else '',
                'Vol Spike':    '✅' if scored['vol_spike_flat']    else '',
                'OBV↑':         '✅' if scored['obv_rising']        else '',
                '52W Low':      '✅' if scored['near_52w_low']      else '',
                'Vol Ratio':    vol_ratio,
                'Vol Today':    today_vol,
                'Vol Avg(21)':  int(avg_vol),
            })
            flagged += 1

        print(f"[{exch}] Scanned {len(daily_map)} stocks — {flagged} flagged.\n")

    if not results:
        print("No accumulation setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values(['Score', 'Vol Ratio'],
                        ascending=[False, False]).reset_index(drop=True)
    return df

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc
    from history_store import preload_histories
    day     = get_last_trading_day()
    symbols = get_nse_ohlc(day)['symbol'].tolist()
    preload_histories(symbols, 'NSE', intervals=('1d',), lookback_bars=60)
    t0      = time.time()
    results = run_accumulation_scan(exchange='NSE', min_score=1)
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(results[['Symbol','Price','Score','Signals',
                        'Vol Ratio']].head(20).to_string(index=False))