from functools import wraps
from flask import session, redirect, url_for
import os
from flask import Flask, render_template, jsonify, request
from accumulation_scanner import run_accumulation_scan
from scanner import run_scan
from momentum_scanner import run_momentum_scan
from darvas_scanner import run_darvas_scan
from trendline_scanner import run_trendline_scan
from inside_bar_scanner import run_inside_bar_scan
from data_fetcher import get_fo_symbols, get_last_trading_day
from cache_helper import clear_old_cache, clear_old_bulk_cache
from volume_helper import enrich_with_volume
from history_store import preload_histories, clear_store, store_stats
from ltp_fetcher import get_ltps_batch, is_market_open
from market_context import get_context, clear_context
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from digest import run_daily_digest
import pytz

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'change-this-locally')
APP_PASSWORD    = os.environ.get('APP_PASSWORD', 'changeme')

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

@app.route('/login', methods=['GET', 'POST'])
def login():
    from flask import request
    error = ''
    if request.method == 'POST':
        if request.form.get('password') == APP_PASSWORD:
            session['logged_in'] = True
            return redirect('/')
        error = 'Wrong password'
    return f'''
    <!DOCTYPE html>
    <html>
    <head>
      <title>Stock Screener — Login</title>
      <style>
        body {{ background:#0f1117; color:#e0e0e0;
                font-family:'Segoe UI',sans-serif;
                display:flex; align-items:center;
                justify-content:center; height:100vh; margin:0; }}
        .box {{ background:#1a1d2e; padding:40px; border-radius:12px;
                border:1px solid #2a2d3e; text-align:center; width:320px; }}
        h2   {{ color:#7c83fd; margin-bottom:24px; }}
        input {{ width:100%; padding:10px; background:#0f1117;
                 border:1px solid #2a2d3e; border-radius:6px;
                 color:#e0e0e0; font-size:1rem; margin-bottom:16px;
                 box-sizing:border-box; }}
        button {{ width:100%; padding:10px; background:#7c83fd;
                  border:none; border-radius:6px; color:#fff;
                  font-size:1rem; font-weight:600; cursor:pointer; }}
        .error {{ color:#ff4444; font-size:0.85rem; margin-top:12px; }}
      </style>
    </head>
    <body>
      <div class="box">
        <h2>📊 Stock Screener</h2>
        <form method="POST">
          <input type="password" name="password" placeholder="Enter password" autofocus>
          <button type="submit">Login</button>
        </form>
        <div class="error">{error}</div>
      </div>
    </body>
    </html>
    '''

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

_cache      = {}
_fo_symbols = None
_preloaded  = set()

def get_fo():
    global _fo_symbols
    if _fo_symbols is None:
        _fo_symbols = get_fo_symbols()
    return _fo_symbols

def filter_fo(df, fo_only):
    if not fo_only or df.empty:
        return df
    fo = get_fo()
    return df[df['Symbol'].isin(fo)].reset_index(drop=True)

def get_params():
    fo_only   = request.args.get('fo_only',   'false').lower() == 'true'
    direction = request.args.get('direction', 'BOTH').upper()
    return fo_only, direction

def dir_filter(df, direction):
    if direction == 'BOTH' or df.empty or 'Direction' not in df.columns:
        return df
    if direction == 'LONG':
        return df[df['Direction'] == '\U0001f7e2 Long'].reset_index(drop=True)
    if direction == 'SHORT':
        return df[df['Direction'] == '\U0001f534 Short'].reset_index(drop=True)
    if direction == 'ATTEMPT':
        return df[df['Direction'] == '\u26a1 Attempt'].reset_index(drop=True)
    if direction == 'BABY':
        return df[df['Direction'] == '\U0001f7e1 Baby'].reset_index(drop=True)
    return df

def to_json(df):
    if df.empty:
        return jsonify({'count': 0, 'data': []})
    return jsonify({'count': len(df), 'data': df.to_dict(orient='records')})

def ensure_preloaded():
    from market_context import UNIVERSE_MODE, MIN_VOL
    key = f"ALL_{get_last_trading_day()}_{UNIVERSE_MODE}"
    if key in _preloaded:
        return
    print(f"\n[Preload] Starting preload (mode: {UNIVERSE_MODE})...")
    contexts = get_context('ALL')
    ctx = contexts.get('ALL')
    if ctx and ctx.daily is not None:
        # ctx.daily is already universe-filtered (ETF, price, today_vol)
        symbols = ctx.daily['symbol'].tolist()
        preload_histories(symbols, 'NSE', intervals=('1d','1wk'), lookback_bars=252)
        store_stats()

        # ── Avg-vol filter: drop symbols where 21-day avg volume < MIN_VOL ──
        # Runs after history is loaded so bulk volume stats are available.
        if MIN_VOL > 0:
            from volume_helper import filter_low_volume_symbols
            keep = filter_low_volume_symbols('NSE', min_vol=MIN_VOL)
            if keep:
                before = len(ctx.daily)
                ctx.daily = ctx.daily[ctx.daily['symbol'].isin(keep)].reset_index(drop=True)
                removed   = before - len(ctx.daily)
                print(f"[Preload] Avg-vol filter (21d avg + today >= {MIN_VOL:,}): "                      f"{before} → {len(ctx.daily)} stocks (−{removed} illiquid)")

    _preloaded.add(key)
    print(f"[Preload] Complete — {len(ctx.daily) if ctx and ctx.daily is not None else 0} symbols.\n")

def run_and_enrich(scan_fn, **kwargs):
    df = scan_fn(**kwargs)
    if not df.empty:
        df = enrich_with_volume(df)
    return df

def ensure_all_scanners_cached():
    """Run all scanners into app cache if not already cached. Used by Top 10."""
    if 'pivot_ALL' not in _cache:
        print("[Top10] Running pivot scanner...")
        _cache['pivot_ALL'] = run_and_enrich(run_scan)
    if 'darvas_ALL' not in _cache:
        print("[Top10] Running darvas scanner...")
        _cache['darvas_ALL'] = run_and_enrich(run_darvas_scan, direction='BOTH')
    if 'trendline_ALL' not in _cache:
        print("[Top10] Running trendline scanner...")
        _cache['trendline_ALL'] = run_and_enrich(run_trendline_scan)
    if 'insidebar_ALL_2' not in _cache:
        print("[Top10] Running insidebar scanner...")
        _cache['insidebar_ALL_2'] = run_and_enrich(run_inside_bar_scan, direction='BOTH', n=2)
    if 'accumulation_ALL_1' not in _cache:
        print("[Top10] Running accumulation scanner...")
        _cache['accumulation_ALL_1'] = run_and_enrich(run_accumulation_scan, min_score=1)
    if 'momentum_ALL_2' not in _cache:
        print("[Top10] Running momentum scanner...")
        _cache['momentum_ALL_2'] = run_and_enrich(run_momentum_scan, min_score=2)

# ─────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/api/trigger_digest')
@login_required
def api_trigger_digest():
    import threading
    threading.Thread(target=lambda: run_daily_digest(_cache), daemon=True).start()
    return jsonify({'status': 'digest triggered in background'})

# -- PIVOT --

@app.route('/api/scan')
def api_scan():
    fo_only, _ = get_params()
    ensure_preloaded()
    if 'pivot_ALL' not in _cache:
        _cache['pivot_ALL'] = run_and_enrich(run_scan)
    return to_json(filter_fo(_cache['pivot_ALL'], fo_only))

@app.route('/api/refresh')
def api_refresh():
    fo_only, _ = get_params()
    ensure_preloaded()
    _cache.pop('pivot_ALL', None)
    _cache['pivot_ALL'] = run_and_enrich(run_scan)
    return to_json(filter_fo(_cache['pivot_ALL'], fo_only))

# -- DARVAS --

@app.route('/api/darvas')
def api_darvas():
    fo_only, direction = get_params()
    ensure_preloaded()
    if 'darvas_ALL' not in _cache:
        _cache['darvas_ALL'] = run_and_enrich(run_darvas_scan, direction='BOTH')
    return to_json(filter_fo(dir_filter(_cache['darvas_ALL'], direction), fo_only))

@app.route('/api/darvas/refresh')
def api_darvas_refresh():
    fo_only, direction = get_params()
    ensure_preloaded()
    _cache.pop('darvas_ALL', None)
    _cache['darvas_ALL'] = run_and_enrich(run_darvas_scan, direction='BOTH')
    return to_json(filter_fo(dir_filter(_cache['darvas_ALL'], direction), fo_only))

# -- TRENDLINE --

@app.route('/api/trendline')
def api_trendline():
    fo_only, _ = get_params()
    ensure_preloaded()
    if 'trendline_ALL' not in _cache:
        _cache['trendline_ALL'] = run_and_enrich(run_trendline_scan)
    return to_json(filter_fo(_cache['trendline_ALL'], fo_only))

@app.route('/api/trendline/refresh')
def api_trendline_refresh():
    fo_only, _ = get_params()
    ensure_preloaded()
    _cache.pop('trendline_ALL', None)
    _cache['trendline_ALL'] = run_and_enrich(run_trendline_scan)
    return to_json(filter_fo(_cache['trendline_ALL'], fo_only))

# -- INSIDE BAR --

@app.route('/api/insidebar')
def api_insidebar():
    fo_only, direction = get_params()
    ensure_preloaded()
    n         = int(request.args.get('n', 2))
    cache_key = f"insidebar_ALL_{n}"
    if cache_key not in _cache:
        _cache[cache_key] = run_and_enrich(run_inside_bar_scan, direction='BOTH', n=n)
    return to_json(filter_fo(dir_filter(_cache[cache_key], direction), fo_only))

@app.route('/api/insidebar/refresh')
def api_insidebar_refresh():
    fo_only, direction = get_params()
    ensure_preloaded()
    n         = int(request.args.get('n', 2))
    cache_key = f"insidebar_ALL_{n}"
    _cache.pop(cache_key, None)
    _cache[cache_key] = run_and_enrich(run_inside_bar_scan, direction='BOTH', n=n)
    return to_json(filter_fo(dir_filter(_cache[cache_key], direction), fo_only))

# -- ACCUMULATION --

@app.route('/api/accumulation')
def api_accumulation():
    fo_only, _ = get_params()
    ensure_preloaded()
    min_score = int(request.args.get('min_score', 1))
    cache_key = f"accumulation_ALL_{min_score}"
    if cache_key not in _cache:
        _cache[cache_key] = run_and_enrich(run_accumulation_scan, min_score=min_score)
    return to_json(filter_fo(_cache[cache_key], fo_only))

@app.route('/api/accumulation/refresh')
def api_accumulation_refresh():
    fo_only, _ = get_params()
    ensure_preloaded()
    min_score = int(request.args.get('min_score', 1))
    cache_key = f"accumulation_ALL_{min_score}"
    _cache.pop(cache_key, None)
    _cache[cache_key] = run_and_enrich(run_accumulation_scan, min_score=min_score)
    return to_json(filter_fo(_cache[cache_key], fo_only))

# -- MOMENTUM --

@app.route('/api/momentum')
def api_momentum():
    fo_only, direction = get_params()
    ensure_preloaded()
    min_score = int(request.args.get('min_score', 2))
    cache_key = f"momentum_ALL_{min_score}"
    if cache_key not in _cache:
        _cache[cache_key] = run_and_enrich(run_momentum_scan, min_score=min_score)
    return to_json(filter_fo(dir_filter(_cache[cache_key], direction), fo_only))

@app.route('/api/momentum/refresh')
def api_momentum_refresh():
    fo_only, direction = get_params()
    ensure_preloaded()
    min_score = int(request.args.get('min_score', 2))
    cache_key = f"momentum_ALL_{min_score}"
    _cache.pop(cache_key, None)
    _cache[cache_key] = run_and_enrich(run_momentum_scan, min_score=min_score)
    return to_json(filter_fo(dir_filter(_cache[cache_key], direction), fo_only))

# -- TOP 10 --

@app.route('/api/top10')
def api_top10():
    ensure_preloaded()
    if 'top10' not in _cache:
        from digest import pick_top_setups

        # Run any scanners not already cached — reuses existing results if available
        ensure_all_scanners_cached()

        # Feed app cache directly into digest scorer — no separate BSE/yfinance preload
        digest_cache = {
            'pivot_BOTH':          _cache.get('pivot_ALL',          pd.DataFrame()),
            'darvas_BOTH':         _cache.get('darvas_ALL',         pd.DataFrame()),
            'trendline_BOTH':      _cache.get('trendline_ALL',      pd.DataFrame()),
            'insidebar_BOTH_2':    _cache.get('insidebar_ALL_2',    pd.DataFrame()),
            'accumulation_BOTH_1': _cache.get('accumulation_ALL_1', pd.DataFrame()),
            'momentum_BOTH_2':     _cache.get('momentum_ALL_2',     pd.DataFrame()),
        }

        df = pick_top_setups(digest_cache, top_n=20)

        if df is None or df.empty:
            _cache['top10'] = {
                'longs': [], 'shorts': [], 'overall': [],
                'date': str(get_last_trading_day()),
            }
        else:
            KEEP = {'Symbol', 'Exchange', 'Price', 'Direction', 'Score',
                    'Vol Ratio', 'Scanner', 'Setup', 'Both TF', 'Signals'}

            def clean(row):
                return {k: (float(v) if hasattr(v, 'item') else v)
                        for k, v in row.items() if k in KEEP}

            def is_long(row):
                return 'Long' in str(row.get('Direction', ''))

            def is_short(row):
                return 'Short' in str(row.get('Direction', ''))

            records = [clean(r) for r in df.to_dict('records')]
            _cache['top10'] = {
                'longs':   [r for r in records if is_long(r)][:10],
                'shorts':  [r for r in records if is_short(r)][:10],
                'overall': records[:10],
                'date':    str(get_last_trading_day()),
            }

    return jsonify(_cache['top10'])

# -- LTP --

@app.route('/api/ltp', methods=['POST'])
def api_ltp():
    if not is_market_open():
        return jsonify({'status': 'closed', 'message': 'Market is closed'})
    data    = request.get_json()
    symbols = data.get('symbols', [])
    if not symbols:
        return jsonify({'status': 'ok', 'ltps': {}})
    pairs = [(s['symbol'], s['exchange']) for s in symbols]
    ltps  = get_ltps_batch(pairs)
    return jsonify({'status': 'ok', 'ltps': ltps})

@app.route('/api/market_status')
def api_market_status():
    return jsonify({'open': is_market_open()})

# -- UTILS --

@app.route('/api/clear_cache')
def api_clear_cache():
    _cache.clear()
    clear_store()
    clear_context()
    _preloaded.clear()
    return jsonify({'status': 'cache cleared'})

@app.route('/api/fo_count')
def api_fo_count():
    return jsonify({'count': len(get_fo())})

# -- SCHEDULER --

def _evening_refresh():
    """
    Runs at 6:30 PM IST Mon-Fri, after NSE publishes today's bhavcopy (~6-7pm).
    Clears all in-memory caches so the next API call triggers a fresh preload
    with today's data — without requiring a Railway restart.
    """
    from data_fetcher import _last_trading_day_cache
    from market_context import clear_context
    _last_trading_day_cache.clear()   # force re-detect today's trading day
    _cache.clear()                    # clear scanner result cache
    clear_store()                     # clear yfinance history store
    clear_context()                   # clear bhavcopy context cache
    _preloaded.clear()                # force re-preload on next request
    print("[Scheduler] Evening refresh complete — caches cleared, today's data will load on next request.")


def start_scheduler():
    IST = pytz.timezone('Asia/Kolkata')
    scheduler = BackgroundScheduler(timezone=IST)

    # 8:30 AM — morning digest (uses previous day's confirmed data)
    scheduler.add_job(
        func=lambda: run_daily_digest(_cache),
        trigger=CronTrigger(hour=8, minute=30,
                            day_of_week='mon-fri',
                            timezone=IST),
        id='daily_digest',
        name='Daily Digest + Preload',
        replace_existing=True,
    )

    # 6:30 PM — evening refresh (picks up today's bhavcopy after NSE publishes it)
    scheduler.add_job(
        func=_evening_refresh,
        trigger=CronTrigger(hour=18, minute=30,
                            day_of_week='mon-fri',
                            timezone=IST),
        id='evening_refresh',
        name='Evening Data Refresh',
        replace_existing=True,
    )

    scheduler.start()
    print("[Scheduler] Jobs scheduled:")
    print("            8:30 AM IST Mon-Fri — morning digest")
    print("            6:30 PM IST Mon-Fri — evening data refresh")
    return scheduler

_scheduler = start_scheduler()

if __name__ == '__main__':
    clear_old_cache()
    clear_old_bulk_cache()
    print("\n✅ Stock Screener running at http://127.0.0.1:5000\n")
    app.run(debug=False)
