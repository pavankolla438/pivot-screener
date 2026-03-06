import pandas as pd
from datetime import datetime

_context_cache = {}

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

        # Daily OHLC — all exchanges now route to NSE
        self.daily = get_all_ohlc(day) if exch in ('ALL', 'BOTH') else get_nse_ohlc(day)

        if self.daily is None:
            print(f"[Context] No daily data for {exch}.")
            return False

        # Weekly OHLC + Pivots
        self.weekly_ohlc = get_weekly_ohlc_nse()
        if self.weekly_ohlc is not None:
            self.weekly_pivots = calculate_pivots(self.weekly_ohlc).set_index('symbol')

        # Monthly OHLC + Pivots
        self.monthly_ohlc = get_monthly_ohlc_nse()
        if self.monthly_ohlc is not None:
            self.monthly_pivots = calculate_pivots(self.monthly_ohlc).set_index('symbol')

        print(f"[Context] {exch} context ready — "
              f"{len(self.daily)} stocks, "
              f"weekly={'✅' if self.weekly_ohlc  is not None else '❌'}, "
              f"monthly={'✅' if self.monthly_ohlc is not None else '❌'}")
        return True


def get_context(exchange):
    today = str(datetime.today().date())

    # All exchange variants resolve to NSE
    exch = 'NSE'
    key  = f"NSE_{today}"

    if key not in _context_cache:
        from data_fetcher import get_last_trading_day
        day = get_last_trading_day()
        ctx = MarketContext(exch, day)
        if ctx.build():
            _context_cache[key] = ctx
        else:
            return {exchange: None}

    return {exchange: _context_cache[key]}


def clear_context():
    global _context_cache
    _context_cache = {}
    print("[Context] Cleared all market contexts.")
