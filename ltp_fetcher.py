import requests
import yfinance as yf
from datetime import datetime

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept":     "application/json",
    "Referer":    "https://www.nseindia.com",
}

_nse_session = None

def get_nse_session():
    global _nse_session
    if _nse_session is None:
        _nse_session = requests.Session()
        _nse_session.headers.update(NSE_HEADERS)
        try:
            _nse_session.get("https://www.nseindia.com", timeout=10)
        except Exception:
            pass
    return _nse_session

def get_ltp_nse(symbol):
    """Fetch LTP for a single NSE symbol."""
    try:
        session = get_nse_session()
        url     = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        r       = session.get(url, timeout=8)
        if r.status_code == 200:
            data = r.json()
            ltp  = data.get('priceInfo', {}).get('lastPrice')
            if ltp:
                return round(float(ltp), 2)
    except Exception:
        pass
    return None

def get_ltp_bse(symbol):
    """Fetch LTP for a BSE symbol via yfinance."""
    try:
        ticker = yf.Ticker(symbol + ".BO")
        info   = ticker.fast_info
        ltp    = info.last_price
        if ltp:
            return round(float(ltp), 2)
    except Exception:
        pass
    return None

def get_ltps_batch(symbols_exchanges):
    """
    Fetch LTPs for a list of (symbol, exchange) tuples.
    Returns dict: {symbol: ltp}
    """
    results = {}
    for sym, exch in symbols_exchanges:
        if exch == 'NSE':
            ltp = get_ltp_nse(sym)
        else:
            ltp = get_ltp_bse(sym)
        if ltp:
            results[sym] = ltp
    return results

def is_market_open():
    """Returns True if current IST time is within market hours."""
    now_utc = datetime.utcnow()
    # IST = UTC + 5:30
    ist_hour   = (now_utc.hour + 5) % 24
    ist_minute = (now_utc.minute + 30) % 60
    if now_utc.minute + 30 >= 60:
        ist_hour = (ist_hour + 1) % 24
    ist_total = ist_hour * 60 + ist_minute

    market_open  = 9  * 60 + 15   # 9:15 AM
    market_close = 15 * 60 + 30   # 3:30 PM

    # Only on weekdays
    weekday = now_utc.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False

    return market_open <= ist_total <= market_close