import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────
# SHARED MARKET CONTEXT
# One instance per exchange per day.
# Computed once, passed into all scanners.
# ─────────────────────────────────────────

_context_cache = {}

class MarketContext:
    def __init__(self, exchange, day):
        self.exchange      = exchange
        self.day           = day
        self.daily         = None   # today's OHLC DataFrame
        self.weekly_ohlc   = None   # prev week aggregated OHLC
        self.monthly_ohlc  = None   # prev month aggregated OHLC
        self.weekly_pivots = None   # pivot levels indexed by symbol
        self.monthly_pivots= None   # pivot levels indexed by symbol

    def build(self):
        from data_fetcher import (
            get_nse_ohlc, get_bse_ohlc,
            get_weekly_ohlc_nse, get_weekly_ohlc_bse,
            get_monthly_ohlc_nse, get_monthly_ohlc_bse,
        )
        from pivot_calculator import calculate_pivots

        exch = self.exchange
        day  = self.day

        print(f"[Context] Building market context for {exch} {day}...")

        # ── Daily OHLC ──
        self.daily = get_nse_ohlc(day) if exch == 'NSE' else get_bse_ohlc(day)
        if self.daily is None:
            print(f"[Context] No daily data for {exch}.")
            return False

        # ── Weekly OHLC + Pivots ──
        self.weekly_ohlc = get_weekly_ohlc_nse() if exch == 'NSE' else get_weekly_ohlc_bse()
        if self.weekly_ohlc is not None:
            self.weekly_pivots = calculate_pivots(self.weekly_ohlc).set_index('symbol')

        # ── Monthly OHLC + Pivots ──
        self.monthly_ohlc = get_monthly_ohlc_nse() if exch == 'NSE' else get_monthly_ohlc_bse()
        if self.monthly_ohlc is not None:
            self.monthly_pivots = calculate_pivots(self.monthly_ohlc).set_index('symbol')

        print(f"[Context] {exch} context ready — "
              f"{len(self.daily)} stocks, "
              f"weekly={'✅' if self.weekly_ohlc is not None else '❌'}, "
              f"monthly={'✅' if self.monthly_ohlc is not None else '❌'}")
        return True


def get_context(exchange):
    """
    Returns a cached MarketContext for the given exchange.
    Rebuilds if date has changed or not yet built.
    """
    today = str(datetime.today().date())

    exchanges = []
    if exchange in ('NSE', 'BOTH'):
        exchanges.append('NSE')
    if exchange in ('BSE', 'BOTH'):
        exchanges.append('BSE')

    contexts = {}
    for exch in exchanges:
        key = f"{exch}_{today}"
        if key not in _context_cache:
            from data_fetcher import get_last_trading_day
            day = get_last_trading_day()
            ctx = MarketContext(exch, day)
            if ctx.build():
                _context_cache[key] = ctx
            else:
                contexts[exch] = None
                continue
        contexts[exch] = _context_cache[key]

    return contexts

def clear_context():
    global _context_cache
    _context_cache = {}
    print("[Context] Cleared all market contexts.")