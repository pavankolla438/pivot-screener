import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from data_fetcher import get_last_trading_day

GMAIL_USER     = os.environ.get('GMAIL_USER',     'pavan.kolla438@gmail.com')
GMAIL_PASSWORD = os.environ.get('GMAIL_PASSWORD', 'nryd rcki xlpr syam')
DIGEST_TO      = os.environ.get('DIGEST_TO',      'harish216gis@gmail.com')


def preload_all(cache_ref):
    from market_context import get_context
    from history_store import preload_histories, store_stats

    day = get_last_trading_day()
    print(f"\n[Digest] Preloading all data for {day}...")

    for exch in ['NSE', 'BSE']:
        contexts = get_context(exch)
        ctx = contexts.get(exch)
        if ctx and ctx.daily is not None:
            symbols = ctx.daily['symbol'].tolist()
            preload_histories(symbols, exch, intervals=('1d','1wk'), lookback_bars=120)
            store_stats()
        print(f"[Digest] {exch} preload complete.")

    from scanner import run_scan
    from darvas_scanner import run_darvas_scan
    from trendline_scanner import run_trendline_scan
    from breakout_scanner import run_breakout_scan
    from inside_bar_scanner import run_inside_bar_scan
    from accumulation_scanner import run_accumulation_scan
    from momentum_scanner import run_momentum_scan
    from volume_helper import enrich_with_volume

    print("[Digest] Warming scanner caches...")
    for scan_fn, kwargs, key in [
        (run_scan,              {'exchange': 'BOTH'},                       'pivot_BOTH'),
        (run_darvas_scan,       {'exchange': 'BOTH', 'direction': 'BOTH'},  'darvas_BOTH'),
        (run_trendline_scan,    {'exchange': 'BOTH'},                       'trendline_BOTH'),
        (run_breakout_scan,     {'exchange': 'BOTH', 'direction': 'BOTH'},  'breakout_BOTH'),
        (run_inside_bar_scan,   {'exchange': 'BOTH', 'direction': 'BOTH', 'n': 2}, 'insidebar_BOTH_2'),
        (run_accumulation_scan, {'exchange': 'BOTH', 'min_score': 1},       'accumulation_BOTH_1'),
        (run_momentum_scan,     {'exchange': 'BOTH', 'min_score': 2},       'momentum_BOTH_2'),
    ]:
        try:
            df = scan_fn(**kwargs)
            if not df.empty:
                df = enrich_with_volume(df)
            cache_ref[key] = df
            print(f"[Digest] {key}: {len(df)} results cached.")
        except Exception as e:
            print(f"[Digest] {key} failed: {e}")
            cache_ref[key] = pd.DataFrame()

    print("[Digest] All caches warmed.\n")


def pick_top_setups(cache_ref, top_n=10):
    combined = []

    def add(df, scanner_name):
        if df is None or df.empty:
            return
        rows = df.copy()
        rows['Scanner'] = scanner_name
        combined.append(rows)

    add(cache_ref.get('pivot_BOTH'),          'Pivot')
    add(cache_ref.get('darvas_BOTH'),         'Darvas')
    add(cache_ref.get('trendline_BOTH'),      'Trendline')
    add(cache_ref.get('breakout_BOTH'),       'Breakout')
    add(cache_ref.get('insidebar_BOTH_2'),    'Inside Bar')
    add(cache_ref.get('accumulation_BOTH_1'), 'Accumulation')
    add(cache_ref.get('momentum_BOTH_2'),     'Momentum')

    if not combined:
        return pd.DataFrame()

    all_df = pd.concat(combined, ignore_index=True, sort=False)

    def quality(r):
        q = 0
        if pd.notna(r.get('Score')) and r.get('Score', 0) >= 3:
            q += 2
        elif pd.notna(r.get('Score')) and r.get('Score', 0) >= 2:
            q += 1
        if str(r.get('Both TF', '')).startswith('⭐'):
            q += 2
        if r.get('Agree') == '✅':
            q += 2
        vr = r.get('Vol Ratio', 0) or 0
        if vr >= 3:
            q += 2
        elif vr >= 2:
            q += 1
        if 'Fresh' in str(r.get('Trigger', '')):
            q += 1
        return q

    all_df['_quality'] = all_df.apply(quality, axis=1)
    filtered = all_df[all_df['_quality'] >= 2].copy()
    if filtered.empty:
        filtered = all_df.copy()

    filtered['_vr'] = pd.to_numeric(filtered.get('Vol Ratio', 0), errors='coerce').fillna(0)
    filtered = filtered.sort_values(['_quality', '_vr'], ascending=[False, False])

    seen = set()
    top  = []
    for _, row in filtered.iterrows():
        sym = row.get('Symbol', '')
        if sym not in seen:
            seen.add(sym)
            top.append(row)
        if len(top) >= top_n:
            break

    return pd.DataFrame(top) if top else pd.DataFrame()


def build_email_html(top_df, day):
    if top_df.empty:
        return "<p>No high-quality setups found today.</p>"

    rows_html = ''
    for _, r in top_df.iterrows():
        direction = str(r.get('Direction', ''))
        color     = '#00cc66' if 'Long' in direction else '#ff4444' if 'Short' in direction else '#ffcc44'
        scanner   = r.get('Scanner', '')
        symbol    = r.get('Symbol', '')
        exchange  = r.get('Exchange', '')
        price     = r.get('Price', '')
        signals   = str(r.get('Signals', r.get('Trigger', r.get('Setup', '-'))))
        vol_ratio = r.get('Vol Ratio', '')
        vol_str   = f"{vol_ratio:.1f}x" if isinstance(vol_ratio, float) else str(vol_ratio or '-')
        both_tf   = '⭐' if str(r.get('Both TF', '')).startswith('⭐') else ''
        agree     = '✅' if r.get('Agree') == '✅' else ''
        score     = r.get('Score', '')
        score_str = f"{int(score)}/4" if pd.notna(score) and score != '' else ''

        rows_html += f"""
        <tr style="border-bottom:1px solid #1e2130;">
          <td style="padding:10px 14px; font-weight:700; color:#e0e0e0;">{symbol}</td>
          <td style="padding:10px 14px; color:#888; font-size:0.85rem;">{exchange}</td>
          <td style="padding:10px 14px;">
            <span style="background:#1a1d2e; color:#7c83fd; padding:2px 8px;
                         border-radius:4px; font-size:0.8rem; font-weight:600;">{scanner}</span>
          </td>
          <td style="padding:10px 14px;">
            <span style="color:{color}; font-weight:700;">{direction}</span>
          </td>
          <td style="padding:10px 14px; color:#e0e0e0;">₹{price}</td>
          <td style="padding:10px 14px; color:#ffdd44; font-size:0.82rem;">{signals[:60]}</td>
          <td style="padding:10px 14px; color:#ffaa44; font-weight:600;">{vol_str}</td>
          <td style="padding:10px 14px; text-align:center; color:#aaa;">{both_tf} {agree} {score_str}</td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0f1117;font-family:'Segoe UI',sans-serif;color:#e0e0e0;">
  <div style="max-width:900px;margin:0 auto;padding:24px;">
    <div style="background:#1a1d2e;border-radius:12px;padding:24px 32px;
                border:1px solid #2a2d3e;margin-bottom:24px;">
      <h1 style="color:#7c83fd;margin:0 0 6px;font-size:1.4rem;letter-spacing:1px;">
        📊 Stock Screener — Daily Digest
      </h1>
      <p style="color:#666;margin:0;font-size:0.9rem;">
        {day} &nbsp;|&nbsp; Top setups across all scanners &nbsp;|&nbsp; NSE + BSE
      </p>
    </div>
    <div style="background:#1a1d2e;border-radius:12px;border:1px solid #2a2d3e;overflow:hidden;">
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#13151f;border-bottom:2px solid #2a2d3e;">
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Symbol</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Exch</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Scanner</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Direction</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Price</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Signals</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Vol Ratio</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Quality</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    <p style="color:#444;font-size:0.78rem;margin-top:20px;text-align:center;">
      Generated by Stock Screener at 8:30 AM IST &nbsp;|&nbsp; Not financial advice.
    </p>
  </div>
</body>
</html>"""


def send_digest_email(top_df, day):
    if not GMAIL_USER or not GMAIL_PASSWORD or not DIGEST_TO:
        print("[Digest] Email not configured — skipping send.")
        return

    recipients = [r.strip() for r in DIGEST_TO.split(',') if r.strip()]
    if not recipients:
        print("[Digest] No recipients configured.")
        return

    subject = f"📊 Stock Screener Digest — {day} — Top {len(top_df)} Setups"
    html    = build_email_html(top_df, day)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = GMAIL_USER
    msg['To']      = ', '.join(recipients)
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, recipients, msg.as_string())
        print(f"[Digest] ✅ Email sent to {recipients}")
    except Exception as e:
        print(f"[Digest] ❌ Email failed: {e}")


def run_daily_digest(cache_ref):
    day = get_last_trading_day()
    print(f"\n[Digest] Starting daily digest for {day}...")
    try:
        preload_all(cache_ref)
        top_df = pick_top_setups(cache_ref, top_n=10)
        print(f"[Digest] Top setups: {len(top_df)}")
        if not top_df.empty:
            print(top_df[['Symbol', 'Exchange', 'Scanner', 'Direction', 'Price', '_quality']].to_string(index=False))
        send_digest_email(top_df, day)
    except Exception as e:
        print(f"[Digest] Error: {e}")
        import traceback
        traceback.print_exc()