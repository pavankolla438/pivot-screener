import os
import re
import pandas as pd
from datetime import datetime

_context_cache = {}

# ─────────────────────────────────────────
# UNIVERSE MODE
# Set UNIVERSE_MODE env var on Railway or locally.
# FNO_ONLY    : ~200 F&O stocks — fastest, cleanest for your style
# CASH_AND_FNO: all NSE EQ minus ETFs/indices — broader universe
# ─────────────────────────────────────────

UNIVERSE_MODE = os.environ.get('UNIVERSE_MODE', 'CASH_AND_FNO').upper()
MIN_PRICE     = float(os.environ.get('MIN_PRICE', '20'))

# ─────────────────────────────────────────
# NON-EQUITY FILTER — 7 layers
# Removes ETFs, index funds, SGBs, G-Secs, debt instruments, junk symbols.
# Each layer targets a distinct instrument class.
# ─────────────────────────────────────────

# 1. Explicit ETF/fund suffixes
_ETF_SUFFIX = re.compile(
    r'(ETF|BEES|REIT|INVIT|LIQUIDBEES|LIQUIDCASE|LIQUID|GILT|CASE|'
    r'JUNIOR|MANIA|SETF|GETF|IETF|MONQ|MAFANG|1D|DTB|N50|NN50|NN50ET)$',
    re.IGNORECASE
)

# 2. Index keyword anywhere in symbol (catches ICICIMIDCAP, HDFCNIFTY50, SETFNN50 etc.)
_INDEX_KEYWORD = re.compile(
    r'(NIFTY|SENSEX|BANKEX|MIDCAP|SMALLCAP|NEXT50|MIDSMALL|NN50)',
    re.IGNORECASE
)

# 3. Sovereign Gold Bonds
_SGB = re.compile(r'^SGB', re.IGNORECASE)

# 4. G-Secs / T-bills
_GSEC = re.compile(r'(\d+\.\d+GS\d{4}|GS\d{4})', re.IGNORECASE)

# 5. Unambiguous AMC-only prefixes (these entities list no operating company stocks)
_AMC_ONLY = re.compile(
    r'^(BSL[A-Z]+|MOTILALOFS[A-Z]*|GROWW[A-Z]{3,}|'
    r'MIRAE[A-Z]{3,}|ABSL[A-Z]{3,}|NIPPON[A-Z]{4,}|'
    r'KOTAKPSU|KOTAKSILVE|KOTAKGOLD)',
    re.IGNORECASE
)

# 6. Pure numeric symbols (debt instruments, T-bills)
_NUMERIC = re.compile(r'^\d+$')

# 7. Symbols >12 chars are almost always ETF/MF names, not operating company stocks
_TOO_LONG = re.compile(r'^.{13,}$')


def _is_non_equity(symbol: str) -> bool:
    return (
        bool(_ETF_SUFFIX.search(symbol))    or
        bool(_INDEX_KEYWORD.search(symbol)) or
        bool(_SGB.match(symbol))            or
        bool(_GSEC.search(symbol))          or
        bool(_AMC_ONLY.match(symbol))       or
        bool(_NUMERIC.match(symbol))        or
        bool(_TOO_LONG.match(symbol))
    )


def get_fo_symbol_set():
    from data_fetcher import get_fo_symbols
    return get_fo_symbols()


def apply_universe_filter(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter daily OHLC to configured universe.
    Called once per MarketContext build — all scanners see the filtered result.
    Steps: non-equity removal → price < MIN_PRICE → FNO_ONLY (if configured)
    """
    if daily_df is None or daily_df.empty:
        return daily_df

    original = len(daily_df)

    # Step 1 — remove non-equity instruments
    daily_df = daily_df[~daily_df['symbol'].apply(_is_non_equity)].reset_index(drop=True)
    after_ne = len(daily_df)

    # Step 2 — remove penny stocks below MIN_PRICE
    daily_df = daily_df[daily_df['close'] >= MIN_PRICE].reset_index(drop=True)
    after_price = len(daily_df)

    # Step 3 — FNO_ONLY subsetting (optional)
    if UNIVERSE_MODE == 'FNO_ONLY':
        fo_syms = get_fo_symbol_set()
        if fo_syms:
            daily_df = daily_df[daily_df['symbol'].isin(fo_syms)].reset_index(drop=True)
        else:
            print("[Universe] FNO_ONLY: could not load F&O list, using filtered universe")

    print(
        f"[Universe] {UNIVERSE_MODE}: {original} raw → "
        f"{after_ne} (−{original - after_ne} non-equity) → "
        f"{after_price} (−{after_ne - after_price} <₹{MIN_PRICE:.0f}) → "
        f"{len(daily_df)} stocks"
    )
    return daily_df


# ─────────────────────────────────────────
# MARKET CONTEXT
# ─────────────────────────────────────────

class MarketContext:
    def __init__(self, exchange, day):
        self.exchange       = exchange
        self.day            = day
        self.daily          = None
        self.weekly_ohlc    = None
        self.monthly_ohlc   = None
        self.weekly_pivots  = None
        self.monthly_pivots = None

    def build(self):
        from data_fetcher import (
            get_nse_ohlc, get_all_ohlc,
            get_weekly_ohlc_nse, get_monthly_ohlc_nse,
        )
        from pivot_calculator import calculate_pivots
        exch = self.exchange
        day  = self.day
        print(f"[Context] Building market context for {exch} {day}...")

        # Daily OHLC
        raw_daily = get_all_ohlc(day) if exch in ('ALL', 'BOTH') else get_nse_ohlc(day)
        if raw_daily is None:
            print(f"[Context] No daily data for {exch}.")
            return False

        # Apply universe filter — all scanners see only allowed symbols
        self.daily = apply_universe_filter(raw_daily)

        # Weekly OHLC + Pivots
        self.weekly_ohlc = get_weekly_ohlc_nse()
        if self.weekly_ohlc is not None:
            self.weekly_pivots = calculate_pivots(self.weekly_ohlc).set_index('symbol')

        # Monthly OHLC + Pivots
        self.monthly_ohlc = get_monthly_ohlc_nse()
        if self.monthly_ohlc is not None:
            self.monthly_pivots = calculate_pivots(self.monthly_ohlc).set_index('symbol')

        print(f"[Context] {exch} context ready — "
              f"{len(self.daily)} stocks "
              f"(mode: {UNIVERSE_MODE}), "
              f"weekly={'✅' if self.weekly_ohlc  is not None else '❌'}, "
              f"monthly={'✅' if self.monthly_ohlc is not None else '❌'}")
        return True


# ─────────────────────────────────────────
# GET CONTEXT
# ─────────────────────────────────────────

def get_context(exchange):
    today = str(datetime.today().date())
    exch  = 'NSE'
    key   = f"NSE_{today}_{UNIVERSE_MODE}"

    if key not in _context_cache:
        from data_fetcher import get_last_trading_day
        day = get_last_trading_day()
        ctx = MarketContext(exch, day)
        if ctx.build():
            _context_cache[key] = ctx
        else:
            return {'NSE': None}

    # Always return 'NSE' as key — scanners must iterate with exch='NSE'
    return {'NSE': _context_cache[key]}


def clear_context():
    global _context_cache
    _context_cache = {}
    print("[Context] Cleared all market contexts.")
