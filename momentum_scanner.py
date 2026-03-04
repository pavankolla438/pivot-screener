import pandas as pd
import numpy as np
from data_fetcher import get_last_trading_day
from history_store import get_all_histories, get_swing_points
from market_context import get_context

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

RSI_PERIOD      = 14
MACD_FAST       = 12
MACD_SLOW       = 26
MACD_SIGNAL     = 9
MACD_LOOKBACK   = 1     # bars to look back for crossover
DIV_LOOKBACK    = 15    # bars to search for divergence swing points
MIN_DIV_DIFF    = 2.0   # minimum RSI difference to count as divergence
MIN_PRICE_DIFF  = 1.0   # minimum price % difference between swing points
SWING_WINDOW    = 3     # swing detection window
VOL_AVG_PERIOD  = 21
VOL_SPIKE_RATIO = 2.0
MIN_BARS = 50

# ─────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────

def compute_rsi(closes, period=RSI_PERIOD):
    delta  = np.diff(closes)
    gains  = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)
    avg_g  = np.convolve(gains,  np.ones(period)/period, mode='valid')[:1][0]
    avg_l  = np.convolve(losses, np.ones(period)/period, mode='valid')[:1][0]
    rsi    = np.zeros(len(closes))
    if avg_l == 0:
        rsi[period] = 100.0
    else:
        rs         = avg_g / avg_l
        rsi[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, len(closes)):
        g       = gains[i - 1]
        l       = losses[i - 1]
        avg_g   = (avg_g * (period - 1) + g) / period
        avg_l   = (avg_l * (period - 1) + l) / period
        rs      = avg_g / avg_l if avg_l != 0 else 100
        rsi[i]  = 100 - 100 / (1 + rs)
    return rsi

def compute_macd(closes):
    def ema(arr, span):
        k   = 2 / (span + 1)
        out = np.zeros(len(arr))
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = arr[i] * k + out[i-1] * (1 - k)
        return out
    ema_fast   = ema(closes, MACD_FAST)
    ema_slow   = ema(closes, MACD_SLOW)
    macd_line  = ema_fast - ema_slow
    signal     = ema(macd_line, MACD_SIGNAL)
    histogram  = macd_line - signal
    return macd_line, signal, histogram

# ─────────────────────────────────────────
# RSI DIVERGENCE DETECTION
# ─────────────────────────────────────────

def find_rsi_divergence(df, sym, exchange, interval):
    if len(df) < MIN_BARS:
        return []

    closes  = pd.to_numeric(df['close'], errors='coerce').values
    rsi     = compute_rsi(closes)
    n       = len(closes)

    high_idxs, low_idxs = get_swing_points(sym, exchange, interval)
    recent_high = [i for i in high_idxs if i >= n - DIV_LOOKBACK and i < n - 2]
    recent_low  = [i for i in low_idxs  if i >= n - DIV_LOOKBACK and i < n - 2]

    divergences = []
    curr_close  = closes[-1]
    curr_rsi    = rsi[-1]

    if curr_rsi <= 0:
        return []

    # ── Bullish: price lower low, RSI higher low (RSI must be below 55) ──
    if curr_rsi < 55:
        for i in recent_low:
            prev_close = closes[i]
            prev_rsi   = rsi[i]
            if prev_rsi <= 0:
                continue
            price_diff = (prev_close - curr_close) / prev_close * 100
            rsi_diff   = curr_rsi - prev_rsi
            if price_diff >= MIN_PRICE_DIFF and rsi_diff >= MIN_DIV_DIFF:
                divergences.append({
                    'type':      'Bullish Divergence',
                    'direction': 'Long',
                    'rsi_now':   round(curr_rsi, 1),
                    'rsi_prev':  round(prev_rsi, 1),
                    'bars_ago':  n - 1 - i,
                })
                break

    # ── Bearish: price higher high, RSI lower high (RSI must be above 45) ──
    if curr_rsi > 45:
        for i in reversed(recent_high):
            prev_close = closes[i]
            prev_rsi   = rsi[i]
            if prev_rsi <= 0:
                continue
            price_diff = (curr_close - prev_close) / prev_close * 100
            rsi_diff   = prev_rsi - curr_rsi
            if price_diff >= MIN_PRICE_DIFF and rsi_diff >= MIN_DIV_DIFF:
                divergences.append({
                    'type':      'Bearish Divergence',
                    'direction': 'Short',
                    'rsi_now':   round(curr_rsi, 1),
                    'rsi_prev':  round(prev_rsi, 1),
                    'bars_ago':  n - 1 - i,
                })
                break

    # ── Hidden Bullish: price higher low, RSI lower low (RSI below 50) ──
    if curr_rsi < 50:
        for i in recent_low:
            prev_close = closes[i]
            prev_rsi   = rsi[i]
            if prev_rsi <= 0:
                continue
            price_diff = (curr_close - prev_close) / prev_close * 100
            rsi_diff   = prev_rsi - curr_rsi
            if price_diff >= MIN_PRICE_DIFF and rsi_diff >= MIN_DIV_DIFF:
                divergences.append({
                    'type':      'Hidden Bullish',
                    'direction': 'Long',
                    'rsi_now':   round(curr_rsi, 1),
                    'rsi_prev':  round(prev_rsi, 1),
                    'bars_ago':  n - 1 - i,
                })
                break

    # ── Hidden Bearish: price lower high, RSI higher high (RSI above 50) ──
    if curr_rsi > 50:
        for i in reversed(recent_high):
            prev_close = closes[i]
            prev_rsi   = rsi[i]
            if prev_rsi <= 0:
                continue
            price_diff = (prev_close - curr_close) / prev_close * 100
            rsi_diff   = curr_rsi - prev_rsi
            if price_diff >= MIN_PRICE_DIFF and rsi_diff >= MIN_DIV_DIFF:
                divergences.append({
                    'type':      'Hidden Bearish',
                    'direction': 'Short',
                    'rsi_now':   round(curr_rsi, 1),
                    'rsi_prev':  round(prev_rsi, 1),
                    'bars_ago':  n - 1 - i,
                })
                break

    return divergences

# ─────────────────────────────────────────
# MACD CROSSOVER DETECTION
# ─────────────────────────────────────────

def find_macd_crossover(df):
    """
    Detects bullish/bearish MACD crossover within last MACD_LOOKBACK bars.
    Returns dict with type and direction or None.
    """
    if len(df) < MIN_BARS:
        return None

    closes     = pd.to_numeric(df['close'], errors='coerce').values
    macd, sig, hist = compute_macd(closes)

    # Check last MACD_LOOKBACK bars for a cross
    for i in range(1, MACD_LOOKBACK + 1):
        idx = -i
        if macd[idx] > sig[idx] and macd[idx-1] <= sig[idx-1]:
            return {
                'type':      'MACD Bull Cross',
                'direction': 'Long',
                'macd':      round(float(macd[-1]), 4),
                'signal':    round(float(sig[-1]),  4),
                'hist':      round(float(hist[-1]), 4),
                'bars_ago':  i - 1,
            }
        if macd[idx] < sig[idx] and macd[idx-1] >= sig[idx-1]:
            return {
                'type':      'MACD Bear Cross',
                'direction': 'Short',
                'macd':      round(float(macd[-1]), 4),
                'signal':    round(float(sig[-1]),  4),
                'hist':      round(float(hist[-1]), 4),
                'bars_ago':  i - 1,
            }
    return None

# ─────────────────────────────────────────
# VOLUME SPIKE CHECK
# ─────────────────────────────────────────

def has_vol_spike(df):
    if 'volume' not in df.columns or len(df) < VOL_AVG_PERIOD + 1:
        return False, 0.0
    vols      = pd.to_numeric(df['volume'], errors='coerce').values
    today_vol = float(vols[-1])
    avg_vol   = float(np.mean(vols[-VOL_AVG_PERIOD-1:-1]))
    if avg_vol == 0:
        return False, 0.0
    ratio = today_vol / avg_vol
    return ratio >= VOL_SPIKE_RATIO, round(ratio, 2)

# ─────────────────────────────────────────
# SCORE A SYMBOL
# ─────────────────────────────────────────

def score_symbol(divs, macd_cross, vol_spike):
    """
    Score 0-4:
    +1 RSI divergence present
    +1 MACD crossover present
    +1 Both signals agree on direction
    +1 Volume spike on signal bar
    """
    score = 0
    tags  = []

    has_div  = len(divs) > 0
    has_macd = macd_cross is not None

    if has_div:
        score += 1
        tags.append(divs[0]['type'])

    if has_macd:
        score += 1
        tags.append(macd_cross['type'])

    # Direction agreement
    agree = False
    if has_div and has_macd:
        agree = divs[0]['direction'] == macd_cross['direction']
        if agree:
            score += 1
            tags.append('✅ Agree')

    if vol_spike:
        score += 1
        tags.append('Vol Spike')

    return score, tags, agree

# ─────────────────────────────────────────
# DETERMINE OVERALL DIRECTION
# ─────────────────────────────────────────

def get_direction(divs, macd_cross):
    """
    If both agree → use that direction.
    If only one present → use that direction.
    If they disagree → Mixed.
    """
    div_dir  = divs[0]['direction']  if divs       else None
    macd_dir = macd_cross['direction'] if macd_cross else None

    if div_dir and macd_dir:
        return div_dir if div_dir == macd_dir else 'Mixed'
    return div_dir or macd_dir or 'Unknown'

# ─────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────

def run_momentum_scan(exchange='ALL', min_score=2):
    day = get_last_trading_day()
    print(f"\n=== Momentum Scan | {day} | Exchange: {exchange} ===\n")

    results  = []
    contexts = get_context('ALL')

    for exch, ctx in contexts.items():
        if ctx is None or ctx.daily is None:
            print(f"[{exch}] No context available, skipping.")
            continue

        print(f"--- {exch} ---")
        daily_map = {
            row['symbol']: row
            for _, row in ctx.daily.iterrows()
        }

        # Accumulate per-symbol hits across timeframes
        sym_hits = {}  # { sym: { '1d': {...}, '1wk': {...} } }

        for interval, label in [('1d', 'Daily'), ('1wk', 'Weekly')]:
            histories = get_all_histories(exch, interval)

            for sym, df in histories.items():
                if df is None or len(df) < MIN_BARS:
                    continue
                if daily_map.get(sym) is None:
                    continue

                divs       = find_rsi_divergence(df, sym, exch, interval)
                macd_cross = find_macd_crossover(df)

                # Skip if neither signal present
                if not divs and macd_cross is None:
                    continue

                spike, vol_ratio = has_vol_spike(df)
                score, tags, agree = score_symbol(divs, macd_cross, spike)

                if score < min_score:
                    continue

                direction = get_direction(divs, macd_cross)

                sym_hits.setdefault(sym, {})[label] = {
                    'divs':       divs,
                    'macd_cross': macd_cross,
                    'score':      score,
                    'tags':       tags,
                    'agree':      agree,
                    'direction':  direction,
                    'vol_ratio':  vol_ratio,
                    'vol_spike':  spike,
                }

        flagged = 0
        for sym, tf_hits in sym_hits.items():
            today = daily_map.get(sym)
            if today is None:
                continue

            both_tf = len(tf_hits) == 2

            # Use best scoring timeframe
            best_label, best = max(tf_hits.items(), key=lambda x: x[1]['score'])

            # Build RSI summary
            rsi_types = ' + '.join(d['type'] for d in best['divs']) if best['divs'] else '-'
            rsi_vals  = (f"RSI {best['divs'][0]['rsi_now']} vs {best['divs'][0]['rsi_prev']}"
                         if best['divs'] else '-')

            # Build MACD summary
            macd_type = best['macd_cross']['type'] if best['macd_cross'] else '-'
            macd_vals = (f"MACD {best['macd_cross']['macd']} / Sig {best['macd_cross']['signal']}"
                         if best['macd_cross'] else '-')

            dir_label = {
                'Long':  '🟢 Long',
                'Short': '🔴 Short',
                'Mixed': '⚡ Mixed',
            }.get(best['direction'], best['direction'])

            signals_str = ' + '.join(best['tags'])

            results.append({
                'Symbol':      sym,
                'Exchange':    exch,
                'Direction':   dir_label,
                'Price':       round(float(today['close']), 2),
                'Score':       best['score'],
                'Signals':     signals_str,
                'RSI Type':    rsi_types,
                'RSI Values':  rsi_vals,
                'MACD Signal': macd_type,
                'MACD Values': macd_vals,
                'Agree':       '✅' if best['agree'] else '',
                'Timeframe':   ' + '.join(tf_hits.keys()),
                'Both TF':     '⭐ Yes' if both_tf else 'No',
                'Vol Ratio':   best['vol_ratio'],
                'Vol Spike':   '✅' if best['vol_spike'] else '',
            })
            flagged += 1

        print(f"[{exch}] Scanned {len(daily_map)} stocks — {flagged} flagged.\n")

    if not results:
        print("No momentum setups found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df['_sort'] = df['Both TF'].apply(lambda x: 0 if '⭐' in x else 1)
    df = df.sort_values(['Score', '_sort'], ascending=[False, True])
    df = df.drop(columns=['_sort']).reset_index(drop=True)
    return df

# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == "__main__":
    import time
    from data_fetcher import get_nse_ohlc, get_last_trading_day
    from history_store import preload_histories

    day     = get_last_trading_day()
    symbols = get_nse_ohlc(day)['symbol'].tolist()
    preload_histories(symbols, 'NSE', intervals=('1d', '1wk'), lookback_bars=120)
    t0      = time.time()
    results = run_momentum_scan(exchange='NSE', min_score=2)
    t1      = time.time()
    print(f"\nTime: {round(t1-t0, 1)}s — {len(results)} results")
    if not results.empty:
        print(results[['Symbol','Direction','Price','Score','Signals',
                        'RSI Type','MACD Signal','Agree','Both TF',
                        'Vol Ratio']].head(30).to_string(index=False))