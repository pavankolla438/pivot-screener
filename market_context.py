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
            get_nse_ohlc, get_bse_ohlc, get_all_ohlc,
            get_weekly_ohlc_nse, get_weekly_ohlc_bse,
            get_monthly_ohlc_nse, get_monthly_ohlc_bse,
        )
        from pivot_calculator import calculate_pivots
        exch = self.exchange
        day  = self.day
        print(f"[Context] Building market context for {exch} {day}...")

        # ── Daily OHLC ──
        if exch == 'NSE':
            self.daily = get_nse_ohlc(day)
        elif exch == 'BSE':
            self.daily = get_bse_ohlc(day)
        elif exch == 'ALL':
            self.daily = get_all_ohlc(day)

        if self.daily is None:
            print(f"[Context] No daily data for {exch}.")
            return False

        # ── Weekly OHLC + Pivots ──
        # For ALL: use NSE weekly (covers most stocks)
        if exch in ('NSE', 'ALL'):
            self.weekly_ohlc = get_weekly_ohlc_nse()
        else:
            self.weekly_ohlc = get_weekly_ohlc_bse()

        if self.weekly_ohlc is not None:
            self.weekly_pivots = calculate_pivots(self.weekly_ohlc).set_index('symbol')

        # ── Monthly OHLC + Pivots ──
        if exch in ('NSE', 'ALL'):
            self.monthly_ohlc = get_monthly_ohlc_nse()
        else:
            self.monthly_ohlc = get_monthly_ohlc_bse()

        if self.monthly_ohlc is not None:
            self.monthly_pivots = calculate_pivots(self.monthly_ohlc).set_index('symbol')

        print(f"[Context] {exch} context ready — "
              f"{len(self.daily)} stocks, "
              f"weekly={'✅' if self.weekly_ohlc  is not None else '❌'}, "
              f"monthly={'✅' if self.monthly_ohlc is not None else '❌'}")
        return True


def get_context(exchange):
    today = str(datetime.today().date())

    # normalize — ALL replaces BOTH
    if exchange in ('BOTH', 'ALL'):
        exch_list = ['ALL']
    elif exchange == 'NSE':
        exch_list = ['NSE']
    elif exchange == 'BSE':
        exch_list = ['BSE']
    else:
        exch_list = ['ALL']

    contexts = {}
    for exch in exch_list:
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