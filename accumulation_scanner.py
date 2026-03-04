import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day
from history_store import get_all_histories
from market_context import get_context

VOL_AVG_PERIOD    = 21
TIGHT_RANGE_PCT   = 2.0
FLAT_MOVE_PCT     = 0.5
VOL_SPIKE_RATIO   = 2.0
VOL_BUILD_BARS    = 5
LOW_52W_PCT       = 7.0
OBV_LOOKBACK      = 30
CONSOL_BARS       = 5
VOL_RISE_DAYS     = 3

# ─────────────────────────────────────────
# SIGNAL DETECTORS
# ─────────────────────────────────────────

def signal_tight_range_vol_rising(df):
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
    vols = df['volume'].values
    for i in range(1, VOL_RISE_DAYS + 1):
        if vols[-i] <= vols[-(i + 1)]:
            return False
    return True


def signal_vol_spike_price_flat(df):
    if len(df) < VOL_AVG_PERIOD + 2:
        return False
    vols      = pd.to_numeric(df['volume'], errors='coerce').values
    today_vol = float(vols[-1])
    avg_vol   = float(np.mean(vols[-VOL_AVG_PERIOD-1:-1]))
    if avg_vol == 0:
        return False
    if today_vol / avg_vol < VOL_SPIKE_RATIO:
        return False
    today_close = float(df.iloc[-1]['close'])
    prev_close  = float(df.iloc[-2]['close'])
    if prev_close == 0:
        return False
    move_pct = abs(today_close - prev_close) / prev_close * 100
    return move_pct < FLAT_MOVE_PCT


def signal_obv_rising(df):
    if len(df) < OBV_LOOKBACK + 1:
        return False
    closes = pd.to_numeric(df['close'],  errors='coerce').values
    vols   = pd.to_numeric(df['volume'], errors='coerce').values
    obv    = np.zeros(len(closes))
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            obv[i] = obv[i-1] + vols[i]
        elif closes[i] < closes[i-1]:
            obv[i] = obv[i-1] - vols[i]
        else:
            obv[i] = obv[i-1]
    recent_obv = obv[-OBV_LOOKBACK:]
    x          = np.arange(len(recent_obv))
    slope      = np.polyfit(x, recent_obv, 1)[0]
    avg_vol    = float(np.mean(vols[-OBV_LOOKBACK:]))
    return slope > avg_vol * 0.005


def signal_near_52w_low_vol_building(df):
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
    recent_vol_avg = float(np.mean(vols[-VOL_BUILD_BARS:]))
    prior_vol_avg  = float(np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS]))
    return recent_vol_avg > prior_vol_avg


# ─────────────────────────────────────────
# SCORE A SYMBOL
# ─────────────────────────────────────────

def score_symbol(df):
    s1 = signal_tight_range_vol_rising(df)
    s2 = signal_vol_spike_price_flat(df)
    s3 = signal_obv_rising(df)
    s4 = signal_near_52w_low_vol_building(df)

    # check vol building strength for s3+s4 combo qualification
    vol_building_strong = False
    if len(df) >= VOL_BUILD_BARS + 20 and 'volume' in df.columns:
        vols = pd.to_numeric(df['volume'], errors='coerce').values
        recent_avg = float(np.mean(vols[-VOL_BUILD_BARS:]))
        prior_avg  = float(np.mean(vols[-VOL_BUILD_BARS-20:-VOL_BUILD_BARS]))
        if prior_avg > 0 and recent_avg / prior_avg >= 2.0:
            vol_building_strong = True

    # need at least one active signal OR strong OBV+52WLow combo
    has_active = s1 or s2
    has_strong_passive = s3 and s4 and vol_building_strong
    if not has_active and not has_strong_passive:
        return {
            'score': 0, 'signals': '',
            'tight_vol_rising': False, 'vol_spike_flat': False,
            'obv_rising': False, 'near_52w_low': False,
        }

    score = sum([s1, s2, s3, s4])
    tags  = []
    if s1: tags.append('Tight+Vol↑')
    if s2: tags.append('Vol Spike')
    if s3: tags.append('OBV↑')
    if s4: tags.append('52W Low')

    return {
        'score':            score,
        'signals':          ' + '.join(tags) if tags else '',
        'tight_vol_rising': s1,
        'vol_spike_flat':   s2,
        'obv_rising':       s3,
        'near_52w_low':     s4,
    }

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_accumulation_scan(exchange='BOTH', min_score=1, min_vol_ratio=0.0):
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
            if 'volume' not in df.columns:
                continue

            scored = score_symbol(df)
            if scored['score'] < min_score:
                continue

            vols      = pd.to_numeric(df['volume'], errors='coerce').values
            today_vol = int(vols[-1]) if not np.isnan(vols[-1]) else 0
            avg_vol   = round(float(np.mean(vols[-VOL_AVG_PERIOD-1:-1])), 0)
            vol_ratio = round(today_vol / avg_vol, 2) if avg_vol > 0 else 0

            results.append({
                'Symbol':      sym,
                'Exchange':    exch,
                'Price':       round(float(today['close']), 2),
                'Score':       scored['score'],
                'Signals':     scored['signals'],
                'Tight+Vol↑':  '✅' if scored['tight_vol_rising'] else '',
                'Vol Spike':   '✅' if scored['vol_spike_flat']    else '',
                'OBV↑':        '✅' if scored['obv_rising']        else '',
                '52W Low':     '✅' if scored['near_52w_low']      else '',
                'Vol Ratio':   vol_ratio,
                'Vol Today':   today_vol,
                'Vol Avg(21)': int(avg_vol),
            })
            flagged += 1

        print(f"[{exch}] Scanned {len(daily_map)} stocks — {flagged} flagged.\n")

    if not results:
        print("No accumulation setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values(['Score', 'Vol Ratio'],
                        ascending=[False, False]).reset_index(drop=True)

    if min_vol_ratio > 0:
        df = df[pd.to_numeric(df['Vol Ratio'], errors='coerce') >= min_vol_ratio]

    return df.reset_index(drop=True)


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc
    from history_store import preload_histories
    day     = get_last_trading_day()
    symbols = get_nse_ohlc(day)['symbol'].tolist()
    preload_histories(symbols, 'NSE', intervals=('1d',), lookback_bars=252)
    t0      = time.time()
    results = run_accumulation_scan(exchange='BOTH', min_score=1)
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(f"\nScore breakdown:")
        print(f"  Score 1: {len(results[results['Score']==1])}")
        print(f"  Score 2: {len(results[results['Score']==2])}")
        print(f"  Score 3: {len(results[results['Score']==3])}")
        print(f"  Score 4: {len(results[results['Score']==4])}")
        print(f"\nSignal counts:")
        print(f"  Tight+Vol↑: {results['Tight+Vol↑'].eq('✅').sum()}")
        print(f"  Vol Spike:  {results['Vol Spike'].eq('✅').sum()}")
        print(f"  OBV↑:       {results['OBV↑'].eq('✅').sum()}")
        print(f"  52W Low:    {results['52W Low'].eq('✅').sum()}")
        print(f"\nTop 20:")
        print(results[['Symbol', 'Price', 'Score', 'Signals',
                        'Vol Ratio']].head(20).to_string(index=False))

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc, get_bse_ohlc
    from history_store import preload_histories
    day      = get_last_trading_day()
    nse_syms = get_nse_ohlc(day)['symbol'].tolist()
    bse_syms = get_bse_ohlc(day)['symbol'].tolist()
    preload_histories(nse_syms, 'NSE', intervals=('1d',), lookback_bars=252)
    preload_histories(bse_syms, 'BSE', intervals=('1d',), lookback_bars=252)
    t0      = time.time()
    results = run_accumulation_scan(exchange='BOTH', min_score=1)
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(f"\nScore breakdown:")
        print(f"  Score 1: {len(results[results['Score']==1])}")
        print(f"  Score 2: {len(results[results['Score']==2])}")
        print(f"  Score 3: {len(results[results['Score']==3])}")
        print(f"  Score 4: {len(results[results['Score']==4])}")
        print(f"\nSignal counts:")
        print(f"  Tight+Vol↑: {results['Tight+Vol↑'].eq('✅').sum()}")
        print(f"  Vol Spike:  {results['Vol Spike'].eq('✅').sum()}")
        print(f"  OBV↑:       {results['OBV↑'].eq('✅').sum()}")
        print(f"  52W Low:    {results['52W Low'].eq('✅').sum()}")