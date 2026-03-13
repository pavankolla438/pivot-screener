"""
regime.py — Market regime detection for NSE.

Architecture
────────────
Regime sits BEFORE scoring in the pipeline:

  preload → scanners → regime.get_regime() → pick_top_setups(regime=...)

Each signal is scored (+1 / -1 / 0) and summed into three orthogonal scores:
  TrendScore   — directional momentum
  RangeScore   — compression / balance
  VolScore     — volatility expansion (shock / breakout risk)

The final label is determined by which score dominates.
None of the three are binary — degree matters.

Index symbols fetched via yfinance
───────────────────────────────────
  ^NSEI   = Nifty 50
  ^NSEBANK= Nifty Bank

Only 60 daily bars needed — fast fetch, cached in memory per trading day.
"""

import os
import math
import numpy as np
import pandas as pd
from datetime import datetime

# ── In-memory cache ──────────────────────────────────────────────────────────
_regime_cache: dict = {}   # { 'YYYY-MM-DD': MarketRegime }


# ─────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────

_INDEX_SYMBOLS = {
    'NIFTY50':   '^NSEI',
    'NIFTYBANK': '^NSEBANK',
}

def _fetch_index(ticker: str, bars: int = 60) -> pd.DataFrame | None:
    """Fetch daily OHLC for an index symbol via yfinance. Returns None on failure."""
    try:
        import yfinance as yf
        raw = yf.download(
            tickers    = ticker,
            period     = f'{bars * 2}d',   # overshoot for weekends/holidays
            interval   = '1d',
            auto_adjust= True,
            progress   = False,
        )
        if raw is None or raw.empty:
            return None
        # Flatten MultiIndex if present
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [c[0].lower() for c in raw.columns]
        else:
            raw.columns = [c.lower() for c in raw.columns]
        raw = raw.dropna(subset=['close'])
        return raw.tail(bars)
    except Exception as e:
        print(f"[Regime] fetch {ticker} failed: {e}")
        return None


# ─────────────────────────────────────────
# INDICATOR HELPERS
# ─────────────────────────────────────────

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    h, l, c = df['high'], df['low'], df['close']
    prev_c  = c.shift(1)
    tr      = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _higher_highs(closes: pd.Series, lookback: int = 10) -> bool:
    """True if the last `lookback` closes form a series of higher swing highs."""
    if len(closes) < lookback:
        return False
    seg = closes.tail(lookback).values
    # Simple: last 3 local maxima are ascending
    highs = [seg[i] for i in range(1, len(seg) - 1) if seg[i] > seg[i-1] and seg[i] > seg[i+1]]
    return len(highs) >= 2 and highs[-1] > highs[-2]


def _lower_lows(closes: pd.Series, lookback: int = 10) -> bool:
    if len(closes) < lookback:
        return False
    seg = closes.tail(lookback).values
    lows = [seg[i] for i in range(1, len(seg) - 1) if seg[i] < seg[i-1] and seg[i] < seg[i+1]]
    return len(lows) >= 2 and lows[-1] < lows[-2]


def _pivot(df: pd.DataFrame) -> float:
    """Classic pivot point from prior day's bar."""
    if len(df) < 2:
        return float(df['close'].iloc[-1])
    prev = df.iloc[-2]
    return (float(prev['high']) + float(prev['low']) + float(prev['close'])) / 3.0


def _atr_contracting(atr_series: pd.Series, window: int = 5) -> bool:
    """True if ATR has been declining for the last `window` bars."""
    if len(atr_series) < window + 1:
        return False
    seg = atr_series.tail(window + 1).values
    return seg[-1] < seg[0]      # latest ATR < ATR `window` bars ago


def _overlapping_candles(df: pd.DataFrame, lookback: int = 5) -> bool:
    """True if recent bars overlap heavily — sign of balance / indecision."""
    if len(df) < lookback:
        return False
    seg  = df.tail(lookback)
    highs = seg['high'].values
    lows  = seg['low'].values
    # Each bar's range overlaps the prior bar
    overlaps = sum(
        1 for i in range(1, len(highs))
        if lows[i] < highs[i-1] and highs[i] > lows[i-1]
    )
    return overlaps >= lookback - 1


# ─────────────────────────────────────────
# CORE SCORING
# ─────────────────────────────────────────

def _score_index(df: pd.DataFrame, name: str) -> dict:
    """
    Score one index dataframe. Returns a dict of component scores.
    Each component is +1 / 0 / -1.
    """
    if df is None or len(df) < 20:
        return {}

    close  = df['close']
    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)
    atr    = _atr(df)
    pvt    = _pivot(df)

    c      = float(close.iloc[-1])
    e20    = float(ema20.iloc[-1])
    e50    = float(ema50.iloc[-1])
    atr_v  = float(atr.iloc[-1])
    atr_avg= float(atr.tail(20).mean())
    atr_pct= atr_v / c * 100    # ATR as % of price

    # ── Trend signals ──────────────────────────────
    t_above_20ema   = +1 if c > e20  else -1
    t_above_50ema   = +1 if c > e50  else -1
    t_ema_aligned   = +1 if e20 > e50 else -1          # bullish stack
    t_higher_highs  = +1 if _higher_highs(close) else (-1 if _lower_lows(close) else 0)
    t_above_pivot   = +1 if c > pvt  else -1

    trend_score = t_above_20ema + t_above_50ema + t_ema_aligned + t_higher_highs + t_above_pivot

    # ── Range / balance signals ─────────────────────
    r_atr_contract  = +1 if _atr_contracting(atr)      else 0
    r_near_pivot    = +1 if abs(c - pvt) / c < 0.005   else 0   # within 0.5%
    r_overlap       = +1 if _overlapping_candles(df)    else 0
    r_low_atr       = +1 if atr_pct < 0.8              else 0   # tight daily range

    range_score = r_atr_contract + r_near_pivot + r_overlap + r_low_atr

    # ── Volatility / shock signals ──────────────────
    atr_ratio  = atr_v / atr_avg if atr_avg > 0 else 1.0
    v_atr_spike= +1 if atr_ratio > 1.4 else 0             # ATR expanding fast
    v_big_day  = +1 if atr_pct > 1.5   else 0             # large intraday range
    v_gap      = 0
    if len(df) >= 2:
        prev_c = float(df['close'].iloc[-2])
        gap_pct = abs(c - prev_c) / prev_c * 100
        v_gap  = +1 if gap_pct > 1.0 else 0               # gap > 1% open

    vol_score = v_atr_spike + v_big_day + v_gap

    return {
        f'{name}_trend_score':   trend_score,
        f'{name}_range_score':   range_score,
        f'{name}_vol_score':     vol_score,
        f'{name}_close':         round(c,   2),
        f'{name}_ema20':         round(e20, 2),
        f'{name}_ema50':         round(e50, 2),
        f'{name}_pivot':         round(pvt, 2),
        f'{name}_atr_pct':       round(atr_pct, 3),
        f'{name}_atr_ratio':     round(atr_ratio, 2),
        # Human-readable component flags for debugging
        f'{name}_above_20ema':   c > e20,
        f'{name}_above_50ema':   c > e50,
        f'{name}_ema_aligned':   e20 > e50,
        f'{name}_higher_highs':  _higher_highs(close),
        f'{name}_atr_contract':  _atr_contracting(atr),
        f'{name}_overlapping':   _overlapping_candles(df),
    }


# ─────────────────────────────────────────
# REGIME RESULT
# ─────────────────────────────────────────

# Score thresholds
_TREND_STRONG  =  3     # |trend_score| ≥ 3 → strong trend
_TREND_WEAK    =  1     # |trend_score| ≥ 1 → mild trend
_VOL_SHOCK     =  2     # vol_score ≥ 2 → shock / expansion


class MarketRegime:
    """
    Immutable result object returned by get_regime().

    Attributes
    ──────────
    label        : str   — 'TRENDING_UP' | 'TRENDING_DOWN' | 'RANGING' | 'VOLATILE'
    trend_score  : int   — combined Nifty50 + NiftyBank trend score  (-10 .. +10)
    range_score  : int   — combined range score                       (0 .. +8)
    vol_score    : int   — combined vol/shock score                   (0 .. +6)
    direction    : str   — 'UP' | 'DOWN' | 'NEUTRAL'
    components   : dict  — all individual signal values (for debugging / API)
    scanner_bias : dict  — which scanner types are preferred in this regime
    """

    def __init__(self, n50: dict, nbank: dict):
        self.components = {**n50, **nbank}

        # Combine scores across both indices
        self.trend_score = (
            n50.get('NIFTY50_trend_score',   0) +
            nbank.get('NIFTYBANK_trend_score', 0)
        )
        self.range_score = (
            n50.get('NIFTY50_range_score',   0) +
            nbank.get('NIFTYBANK_range_score', 0)
        )
        self.vol_score = (
            n50.get('NIFTY50_vol_score',   0) +
            nbank.get('NIFTYBANK_vol_score', 0)
        )

        # Sector divergence flag
        n50_dir   = n50.get('NIFTY50_trend_score',   0)
        nbank_dir = nbank.get('NIFTYBANK_trend_score', 0)
        self.sector_divergence = (
            n50_dir != 0 and nbank_dir != 0 and
            (n50_dir > 0) != (nbank_dir > 0)
        )

        # ── Classify ─────────────────────────────────
        if self.vol_score >= _VOL_SHOCK:
            self.label = 'VOLATILE'
        elif abs(self.trend_score) >= _TREND_STRONG:
            self.label = 'TRENDING_UP' if self.trend_score > 0 else 'TRENDING_DOWN'
        elif abs(self.trend_score) >= _TREND_WEAK and self.range_score < 3:
            self.label = 'TRENDING_UP' if self.trend_score > 0 else 'TRENDING_DOWN'
        else:
            self.label = 'RANGING'

        self.direction = (
            'UP'      if self.trend_score > 0 else
            'DOWN'    if self.trend_score < 0 else
            'NEUTRAL'
        )

        # ── Scanner bias ─────────────────────────────
        # Which trigger types to BOOST vs MUTE in this regime
        self.scanner_bias = self._build_scanner_bias()

    def _build_scanner_bias(self) -> dict:
        """
        Returns multipliers (0.5 – 1.5) per scanner/trigger type.
        Applied as a multiplicative bonus in pick_top_setups.

        If volatile_overlay is True (strong trend + elevated vol), long setups
        are dampened in a downtrend and short setups dampened in an uptrend.
        """
        label   = self.label
        volatile = getattr(self, 'volatile_overlay', False)

        if label == 'TRENDING_UP':
            bias = {
                'Breakout':     1.4,
                'Breakdown':    0.6,
                'Attempt':      1.2,
                'Baby':         1.0,
                'Pivot':        0.8,
                'Darvas':       1.3,
                'Accumulation': 1.2,
                'Momentum':     1.3,
                'Trendline':    1.2,
                'Inside Bar':   1.1,
            }
        elif label == 'TRENDING_DOWN':
            bias = {
                'Breakout':     0.6,
                'Breakdown':    1.4,
                'Attempt':      1.2,
                'Baby':         1.0,
                'Pivot':        1.1,
                'Darvas':       1.2,
                'Accumulation': 0.9,
                'Momentum':     1.2,
                'Trendline':    1.1,
                'Inside Bar':   1.1,
            }
        elif label == 'RANGING':
            bias = {
                'Breakout':     0.8,
                'Breakdown':    0.8,
                'Attempt':      1.3,
                'Baby':         1.4,
                'Pivot':        1.5,
                'Darvas':       0.9,
                'Accumulation': 1.3,
                'Momentum':     0.8,
                'Trendline':    1.2,
                'Inside Bar':   1.4,
            }
        else:  # VOLATILE — no strong trend, direction unclear
            bias = {
                'Breakout':     0.7,
                'Breakdown':    0.7,
                'Attempt':      0.8,
                'Baby':         1.2,
                'Pivot':        1.0,
                'Darvas':       0.8,
                'Accumulation': 1.1,
                'Momentum':     0.7,
                'Trendline':    0.9,
                'Inside Bar':   1.2,
            }

        # Volatile overlay: strong trend with elevated vol.
        # Dampen counter-trend setups so the engine doesn't fight the trend.
        if volatile and label != 'VOLATILE':
            if self.direction == 'DOWN':
                # Volatile downtrend — dampen long-biased scanners
                for k in ('Breakout', 'Accumulation', 'Momentum', 'Darvas'):
                    bias[k] = round(bias[k] * 0.85, 3)
            elif self.direction == 'UP':
                # Volatile uptrend — dampen short-biased setups
                for k in ('Breakdown',):
                    bias[k] = round(bias[k] * 0.85, 3)

        return bias


    # ── Convenience properties ────────────────────────────────────────────────

    @property
    def emoji(self) -> str:
        return {
            'TRENDING_UP':   '📈',
            'TRENDING_DOWN': '📉',
            'RANGING':       '↔️',
            'VOLATILE':      '⚡',
        }.get(self.label, '❓')

    @property
    def summary(self) -> str:
        diverg = ' | ⚠️ Sector Divergence' if self.sector_divergence else ''
        return (
            f"{self.emoji} {self.label}  "
            f"[trend={self.trend_score:+d}  range={self.range_score}  vol={self.vol_score}]"
            f"{diverg}"
        )

    # Direction bias per regime — applied on top of scanner bias
    _DIRECTION_BIAS = {
        'TRENDING_DOWN': {'Long': 0.5,  'Short': 1.3, 'Mixed': 0.8, 'Neutral': 0.7},
        'TRENDING_UP':   {'Long': 1.3,  'Short': 0.5, 'Mixed': 0.8, 'Neutral': 0.8},
        'RANGING':       {'Long': 1.0,  'Short': 1.0, 'Mixed': 1.0, 'Neutral': 1.1},
        'VOLATILE':      {'Long': 0.8,  'Short': 0.8, 'Mixed': 0.9, 'Neutral': 1.0},
    }

    @staticmethod
    def _parse_direction(trigger: str) -> str:
        """Extract Long/Short/Mixed/Neutral from any trigger or direction string."""
        t = str(trigger)
        if any(x in t for x in ('Long', 'Breakout', 'Bull', '🟢')): return 'Long'
        if any(x in t for x in ('Short', 'Breakdown', 'Bear', '🔴')): return 'Short'
        if 'Mixed' in t or '⚡' in t: return 'Mixed'
        return 'Neutral'

    def get_bias(self, scanner: str, trigger: str = '') -> float:
        """
        Get the combined multiplier for a (scanner, trigger) pair.
        scanner : 'Pivot' | 'Darvas' | 'Trendline' | 'Inside Bar' | 'Accumulation' | 'Momentum'
        trigger : any Direction or Trigger string from scanner output
        Returns float — multiplied scanner × trigger × direction bias.
        """
        sb = self.scanner_bias

        # Scanner-level bias (e.g. Accumulation=0.9 in downtrend)
        scanner_mult = sb.get(scanner, 1.0)

        # Trigger-level bias for named triggers (Breakout/Breakdown/Baby/Attempt)
        trigger_mult = sb.get(trigger, 1.0) if trigger else 1.0

        # Direction bias — the most important: Long in downtrend gets 0.5×
        direction     = self._parse_direction(trigger)
        dir_bias      = self._DIRECTION_BIAS.get(self.label, {})
        direction_mult = dir_bias.get(direction, 1.0)

        return round(scanner_mult * trigger_mult * direction_mult, 3)

    def to_dict(self) -> dict:
        return {
            'label':             self.label,
            'volatile_overlay':  getattr(self, 'volatile_overlay', False),
            'direction':         self.direction,
            'trend_score':       self.trend_score,
            'range_score':       self.range_score,
            'vol_score':         self.vol_score,
            'sector_divergence': self.sector_divergence,
            'summary':           self.summary,
            'scanner_bias':      self.scanner_bias,
            **{k: (bool(v) if isinstance(v, (bool, np.bool_)) else
                   float(v) if isinstance(v, (float, np.floating)) else
                   int(v)   if isinstance(v, (int, np.integer)) else v)
               for k, v in self.components.items()},
        }


# ─────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────

def get_regime(force_refresh: bool = False) -> MarketRegime:
    """
    Returns today's MarketRegime, cached in memory.
    Re-fetches after market close or on force_refresh=True.
    """
    from data_fetcher import get_last_trading_day
    day = get_last_trading_day()

    if not force_refresh and day in _regime_cache:
        return _regime_cache[day]

    print(f"[Regime] Computing market regime for {day}...")

    n50_df    = _fetch_index(_INDEX_SYMBOLS['NIFTY50'],   bars=60)
    nbank_df  = _fetch_index(_INDEX_SYMBOLS['NIFTYBANK'], bars=60)

    n50_scores   = _score_index(n50_df,   'NIFTY50')
    nbank_scores = _score_index(nbank_df, 'NIFTYBANK')

    regime = MarketRegime(n50_scores, nbank_scores)
    _regime_cache[day] = regime

    print(f"[Regime] {regime.summary}")
    return regime


def clear_regime_cache():
    global _regime_cache
    _regime_cache = {}


# ─────────────────────────────────────────
# INTEGRATION HELPER — called by digest.py
# ─────────────────────────────────────────

def apply_regime_bias(score: float, scanner: str, trigger: str,
                      regime: MarketRegime | None) -> float:
    """
    Multiply a raw digest score by the regime bias for this (scanner, trigger).
    Safe to call with regime=None (returns score unchanged).
    """
    if regime is None:
        return score
    return round(score * regime.get_bias(scanner, trigger), 3)


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────

if __name__ == '__main__':
    import time
    t0 = time.perf_counter()
    r  = get_regime()
    t1 = time.perf_counter()

    print(f"\nRegime computed in {t1-t0:.2f}s")
    print(f"Label:     {r.label}")
    print(f"Direction: {r.direction}")
    print(f"Scores:    trend={r.trend_score:+d}  range={r.range_score}  vol={r.vol_score}")
    print(f"Divergence:{r.sector_divergence}")
    print(f"Summary:   {r.summary}")
    print()
    print("Scanner bias:")
    for k, v in sorted(r.scanner_bias.items()):
        print(f"  {k:<14} {v:.1f}x")
    print()
    print("NIFTY50 components:")
    for k, v in r.components.items():
        if 'NIFTY50' in k:
            print(f"  {k:<35} {v}")
    print()
    print("NIFTYBANK components:")
    for k, v in r.components.items():
        if 'NIFTYBANK' in k:
            print(f"  {k:<35} {v}")
