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

# Minimum price — stocks below this are excluded (saves scan time on illiquid trash)
MIN_PRICE = float(os.environ.get('MIN_PRICE', '20'))

# ── Filter 1: Suffix-based ETF/fund/bond patterns ──
# Catches symbols ending in known non-equity suffixes
_ETF_SUFFIX = re.compile(
    r'(ETF|BEES|LIQUID|GILT|SETF|GETF|IETF|SENETF|'
    r'BANKBEES|GOLDBEES|SILVERBEES|LIQUIDCASE|LIQUIDBEES|OVERNIGHT|'
    r'REIT|INVIT|\d+DTB|\d+TB|1D|0432|0423)$',
    re.IGNORECASE
)

# ── Filter 2: Index-tracker keywords anywhere in symbol ──
# SENSEXBETA, BANKNIFTY1, NIFTYBEES etc.
_INDEX_TRACKER = re.compile(
    r'NIFTY|SENSEX|BANKEX|MONQ|MAFANG|JUNIOR|MANIA',
    re.IGNORECASE
)

# ── Filter 3: Sovereign Gold Bonds (SGBOCT26, SGBJUN28 …) ──
_SGB = re.compile(r'^SGB[A-Z]{3}\d{2}$', re.IGNORECASE)

# ── Filter 4: G-Secs and T-bills (GS2034, GSEC2028, 7.26GS2029 …) ──
_GSEC = re.compile(r'^(GS\d{4}|GSEC\d{4}|\d+\.\d+GS\d{4})$', re.IGNORECASE)

# ── Filter 5: AMC-prefixed index fund names ──
# Catches HDFCNIFTY, ICICIMIDCAP, BSLSENETFG, NIPPONNIFTY etc.
# Uses {0,8} connector to allow for zero or short connectors between AMC and index word.
# Keeps HDFCBANK, SBICARD, ICICIPRU as real equities.
_AMC_INDEX = re.compile(
    r'^(BSL|SBI|HDFC|ICICI|NIPPON|AXIS|MIRAE|ABSL|GROWW|UTI|KOTAK|TATA|'
    r'EDELWEISS|MOTILAL|DSP|INVESCO)'
    r'.{0,8}'
    r'(NIFTY|SENSEX|MIDCAP|SMALLCAP|SMALL|INFRA|PHARMA|FMCG|METAL|REALTY|'
    r'ALPHA|QUALITY|VALUE|MOMENTUM|LOWVOL|EQUAL|INDEX|NEXT50|SENETF)',
    re.IGNORECASE
)

# ── Filter 6: Pure index-number symbols (SMALL250, NEXT50, MID150 …) ──
_INDEX_NUMERIC = re.compile(r'^(SMALL|MID|LARGE|NEXT|MICRO)\d+$', re.IGNORECASE)

# ── Filter 7: Purely numeric symbols (bond ISINs etc.) ──
_NUMERIC_SYMBOL = re.compile(r'^\d+$')


def _is_non_equity(symbol: str) -> bool:
    """Returns True if symbol is an ETF, index fund, bond, SGB, or other non-tradeable."""
    if _NUMERIC_SYMBOL.match(symbol):    return True
    if _ETF_SUFFIX.search(symbol):       return True
    if _INDEX_TRACKER.search(symbol):    return True
    if _SGB.match(symbol):               return True
    if _GSEC.match(symbol):              return True
    if _AMC_INDEX.match(symbol):         return True
    if _INDEX_NUMERIC.match(symbol):     return True
    return False


# Legacy alias — used by FNO_ONLY path
def _is_etf(symbol: str) -> bool:
    return _is_non_equity(symbol)


def get_fo_symbol_set():
    """Returns F&O symbol set, cached for process lifetime."""
    from data_fetcher import get_fo_symbols
    return get_fo_symbols()


def apply_universe_filter(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter daily OHLC DataFrame to configured universe.
    Called once per MarketContext build — all scanners inherit the result.

    Steps (applied in order regardless of mode):
      1. Remove ETFs, index funds, bonds, SGBs, REITs, junk symbols
      2. Remove stocks priced below MIN_PRICE (default ₹20)
      3. FNO_ONLY mode: further restrict to F&O stocks only
    """
    if daily_df is None or daily_df.empty:
        return daily_df

    original = len(daily_df)

    # ── Step 1: Remove non-tradeable instruments ──
    non_eq_mask  = daily_df['symbol'].apply(_is_non_equity)
    daily_df     = daily_df[~non_eq_mask].reset_index(drop=True)
    after_etf    = len(daily_df)
    removed_etf  = original - after_etf

    # ── Step 2: Remove stocks priced below MIN_PRICE ──
    if 'close' in daily_df.columns:
        price_mask    = pd.to_numeric(daily_df['close'], errors='coerce') >= MIN_PRICE
        daily_df      = daily_df[price_mask].reset_index(drop=True)
        after_price   = len(daily_df)
        removed_price = after_etf - after_price
    else:
        after_price   = after_etf
        removed_price = 0

    # ── Step 3: Universe mode ──
    if UNIVERSE_MODE == 'FNO_ONLY':
        fo_syms = get_fo_symbol_set()
        if fo_syms:
            daily_df = daily_df[daily_df['symbol'].isin(fo_syms)].reset_index(drop=True)
            print(f"[Universe] FNO_ONLY: {original} raw "
                  f"→ {after_etf} (−{removed_etf} non-equity) "
                  f"→ {after_price} (−{removed_price} <₹{MIN_PRICE:.0f}) "
                  f"→ {len(daily_df)} F&O stocks")
        else:
            print(f"[Universe] FNO_ONLY: F&O list unavailable — "
                  f"using {after_price} stocks after ETF+price filter")

    elif UNIVERSE_MODE == 'CASH_AND_FNO':
        print(f"[Universe] CASH_AND_FNO: {original} raw "
              f"→ {after_etf} (−{removed_etf} non-equity) "
              f"→ {after_price} (−{removed_price} <₹{MIN_PRICE:.0f}) stocks")

    else:
        print(f"[Universe] Unknown mode '{UNIVERSE_MODE}' — "
              f"{after_price} stocks after ETF+price filter")

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
