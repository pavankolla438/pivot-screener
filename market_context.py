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
MIN_VOL       = int(os.environ.get('MIN_VOL',   '5000'))  # min today's traded volume

# ─────────────────────────────────────────
# NON-EQUITY FILTER — 10 layers
# Removes ETFs, index funds, SGBs, G-Secs, debt instruments, junk symbols.
# ─────────────────────────────────────────

# 1. ETF substring anywhere (catches EBBETF0433, BBETF0432, mid-name ETFs)
_ETF_ANYWHERE = re.compile(r'ETF', re.IGNORECASE)

# 2. ETF/fund suffixes
_ETF_SUFFIX = re.compile(
    r'(BEES|REIT|INVIT|LIQUIDBEES|LIQUIDCASE|LIQUID|GILT|CASE|'
    r'JUNIOR|MANIA|SETF|GETF|IETF|MONQ|MAFANG|1D|DTB|'
    r'N50|NN50|NN50ET|ADD|GOLD|SILVER|BETA|TOP50|TOP100|MOM\d+)$',
    re.IGNORECASE
)

# 3. Index keyword anywhere
_INDEX_KEYWORD = re.compile(
    r'(NIFTY|SENSEX|BANKEX|MIDCAP|SMALLCAP|NEXT50|MIDSMALL|NN50|MAKEINDIA)',
    re.IGNORECASE
)

# 4. Sovereign Gold Bonds
_SGB = re.compile(r'^SGB', re.IGNORECASE)

# 5. G-Secs / T-bills
_GSEC = re.compile(r'(\d+\.\d+GS\d{4}|GS\d{4})', re.IGNORECASE)

# 6. Instruments ending in 3+ digits: year-coded bonds (0433), 360-series ETFs (GOLD360, SILVER360)
_BOND_YEAR     = re.compile(r'\d{3,}$')
_LIQUID_PREFIX = re.compile(r'^LIQUID',             re.IGNORECASE)  # LIQUIDSBI, LIQUIDSHRI…
_SETF_PREFIX   = re.compile(r'^SETF',               re.IGNORECASE)  # SETFNIF50, SETFGOLD…
_LIC_FUND      = re.compile(r'^LIC(NET|MFN|NFN|MF[A-Z])', re.IGNORECASE)  # LIC ETF/MF (not LICI/LICHSGFIN)
_INVESCO_ETF   = re.compile(r'^IVZIN',              re.IGNORECASE)  # IVZINNIFTY…

# 7. Bharat Bond ETF series ending in B+2digits (ICICIB22, ICICIB30 etc.)
_BHARAT_BOND = re.compile(r'B\d{2}$', re.IGNORECASE)

# 8. Unambiguous AMC-only prefixes (no operating company uses these)
_AMC_ONLY = re.compile(
    r'^(BSL[A-Z]+|MOTILALOFS[A-Z]*|GROWW[A-Z]{3,}|'
    r'MIRAE[A-Z]{3,}|ABSL[A-Z]{3,}|NIPPON[A-Z]{4,}|'
    r'KOTAKPSU|KOTAKSILVE|KOTAKGOLD)',
    re.IGNORECASE
)

# 11. AMC prefix + factor/strategy suffix = Smart Beta / Factor ETF
#     e.g. HDFCQUAL, HDFCMOMENT, ICICIQUAL, KOTAKQUAL, SBIPSU
_AMC_PREFIX_PAT  = re.compile(
    r'^(HDFC|ICICI|KOTAK|AXIS|SBI|UTI|DSP|FRANKLIN|NIPPON|MIRAE|ABSL|BSL|EDELWEISS|GROWW|MOTILAL|BARODA|MO)',
    re.IGNORECASE
)
_FACTOR_SUFF_PAT = re.compile(
    r'(QUAL|MOMENT|MOMENTM|VALUE|GROWTH|LOWVOL|LOWVOLAT|ALPHA|DIVYIELD|'
    r'MULTI|EQUALWT|MOMENTUM|QUALITY|PVTBANK|PSUBANK|CPSE|PSE|PSU|'
    r'SHARIAH|ESG|INFRA|METAL|REALTY|ENERGY|FMCG|CONSUMP|HEALTH|FSL|N500|NQ50|NQ100)$',
    re.IGNORECASE
)

# 12. Standalone strategy words that are full ETF names (e.g. MOMENTUM, QUALITY)
_STANDALONE_STRATEGY = re.compile(
    r'^(MOMENTUM|QUALITY|ALPHA|DIVIDEND|LOWVOL|CONSUMPTION)$',
    re.IGNORECASE
)

# 9. Pure numeric symbols
_NUMERIC = re.compile(r'^\d+$')

# 10. Symbols >12 chars
_TOO_LONG = re.compile(r'^.{13,}$')


def _is_non_equity(symbol: str) -> bool:
    return (
        bool(_ETF_ANYWHERE.search(symbol))  or
        bool(_ETF_SUFFIX.search(symbol))    or
        bool(_INDEX_KEYWORD.search(symbol)) or
        bool(_SGB.match(symbol))            or
        bool(_GSEC.search(symbol))          or
        bool(_BOND_YEAR.search(symbol))     or
        bool(_BHARAT_BOND.search(symbol))   or
        bool(_AMC_ONLY.match(symbol))       or
        bool(_AMC_PREFIX_PAT.match(symbol) and _FACTOR_SUFF_PAT.search(symbol)) or
        bool(_STANDALONE_STRATEGY.match(symbol)) or
        bool(_LIQUID_PREFIX.match(symbol))  or
        bool(_SETF_PREFIX.match(symbol))    or
        bool(_LIC_FUND.match(symbol))       or
        bool(_INVESCO_ETF.match(symbol))    or
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

    # Step 3 — remove illiquid stocks (today's volume < MIN_VOL)
    if 'volume' in daily_df.columns and MIN_VOL > 0:
        daily_df['volume'] = pd.to_numeric(daily_df['volume'], errors='coerce')
        daily_df = daily_df[daily_df['volume'].fillna(0) >= MIN_VOL].reset_index(drop=True)
    after_vol = len(daily_df)

    # Step 4 — FNO_ONLY subsetting (optional)
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
        f"{after_vol} (−{after_price - after_vol} vol<{MIN_VOL:,}) → "
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
