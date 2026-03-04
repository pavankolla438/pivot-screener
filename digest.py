import os
import math
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from data_fetcher import get_last_trading_day

GMAIL_USER     = os.environ.get('GMAIL_USER',     '')
GMAIL_PASSWORD = os.environ.get('GMAIL_PASSWORD', '')
DIGEST_TO      = os.environ.get('DIGEST_TO',      '')


# ─────────────────────────────────────────
# PRELOAD ALL DATA
# ─────────────────────────────────────────

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
        (run_scan,              {'exchange': 'BOTH'},                            'pivot_BOTH'),
        (run_darvas_scan,       {'exchange': 'BOTH', 'direction': 'BOTH'},       'darvas_BOTH'),
        (run_trendline_scan,    {'exchange': 'BOTH'},                            'trendline_BOTH'),
        (run_breakout_scan,     {'exchange': 'BOTH', 'direction': 'BOTH'},       'breakout_BOTH'),
        (run_inside_bar_scan,   {'exchange': 'BOTH', 'direction': 'BOTH', 'n': 2}, 'insidebar_BOTH_2'),
        (run_accumulation_scan, {'exchange': 'BOTH', 'min_score': 1},            'accumulation_BOTH_1'),
        (run_momentum_scan,     {'exchange': 'BOTH', 'min_score': 2},            'momentum_BOTH_2'),
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


# ─────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────

def structure_score(r, scanner):
    s = 0
    if scanner == 'Pivot':
        hit = str(r.get('Weekly Hit', '')) + str(r.get('Monthly Hit', ''))
        if 'Inside CPR' in hit:  s += 2
        elif 'Near' in hit:      s += 1
        if r.get('Narrow CPR (W)') or r.get('Narrow CPR (M)'): s += 1
    elif scanner == 'Trendline':
        setup = str(r.get('Setup', ''))
        s += 2 if 'Horizontal' in setup else 1
    elif scanner == 'Darvas':
        s += 2
    elif scanner == 'Accumulation':
        score = r.get('Score', 0) or 0
        s += min(int(score), 2)
    return s


def trigger_score(r, scanner):
    s = 0
    trigger = str(r.get('Trigger', ''))
    if scanner == 'Breakout':
        if 'Fresh' in trigger:          s += 2
        elif 'Breakout' in trigger:     s += 1
        elif 'Breakdown' in trigger:    s += 1
        if str(r.get('Both TF', '')).startswith('⭐'): s += 1
        try:
            gap = abs(float(r.get('Gap %', 0) or 0))
            if gap >= 2: s += 1
        except:
            pass
    elif scanner == 'Inside Bar':
        if trigger == 'Breakout':       s += 2
        elif trigger == 'Breakdown':    s += 2
        elif trigger == 'Attempt':      s += 1
        if str(r.get('Both TF', '')).startswith('⭐'): s += 1
    elif scanner == 'Darvas':
        if 'Fresh' in trigger or trigger == 'Breakout': s += 2
        elif 'Retest' in trigger:       s += 1
    elif scanner == 'Momentum':
        if r.get('Agree') == '✅':      s += 2
        score = r.get('Score', 0) or 0
        if int(score) >= 3:             s += 1
    elif scanner == 'Pivot':
        setup = str(r.get('Setup', ''))
        if 'Narrow CPR' in setup:       s += 1
    return s


def momentum_score(r, scanner):
    s = 0
    if scanner == 'Momentum':
        if r.get('Agree') == '✅':      s += 2
        if r.get('Vol Spike') == '✅':  s += 1
        rsi_type = str(r.get('RSI Type', ''))
        if 'Bullish Divergence' in rsi_type or 'Bearish Divergence' in rsi_type:
            s += 1
    elif scanner == 'Accumulation':
        if r.get('OBV↑') == '✅':       s += 1
        if r.get('Vol Spike') == '✅':  s += 1
    return s


def vol_multiplier(r):
    vr = r.get('Vol Ratio', 1) or 1
    try:
        vr = float(vr)
    except:
        vr = 1.0
    vr = max(vr, 1.0)
    return 1 + math.log(vr)


# ─────────────────────────────────────────
# PICK TOP SETUPS
# ─────────────────────────────────────────

def pick_top_setups(cache_ref, top_n=10):

    scanner_dfs = {
        'Pivot':        cache_ref.get('pivot_BOTH'),
        'Darvas':       cache_ref.get('darvas_BOTH'),
        'Trendline':    cache_ref.get('trendline_BOTH'),
        'Breakout':     cache_ref.get('breakout_BOTH'),
        'Inside Bar':   cache_ref.get('insidebar_BOTH_2'),
        'Accumulation': cache_ref.get('accumulation_BOTH_1'),
        'Momentum':     cache_ref.get('momentum_BOTH_2'),
    }

    all_rows = []

    for scanner_name, df in scanner_dfs.items():
        if df is None or df.empty:
            continue
        rows = df.copy()
        rows['Scanner'] = scanner_name

        scored = []
        for _, r in rows.iterrows():
            st    = structure_score(r, scanner_name)
            tr    = trigger_score(r, scanner_name)
            mo    = momentum_score(r, scanner_name)
            base  = st + tr * 2 + mo
            vm    = vol_multiplier(r)
            final = base * vm
            scored.append((final, st, tr, mo, r))

        scored.sort(key=lambda x: x[0], reverse=True)
        for final, st, tr, mo, r in scored[:30]:
            row = r.copy()
            row['_final_score'] = round(final, 3)
            row['_st']          = st
            row['_tr']          = tr
            row['_mo']          = mo
            all_rows.append(row)

    if not all_rows:
        return pd.DataFrame()

    all_df = pd.DataFrame(all_rows)
    all_df = all_df.sort_values('_final_score', ascending=False)

    # ── confluence bonus ──
    symbol_scanners = {}
    for _, r in all_df.iterrows():
        sym = r.get('Symbol', '')
        sc  = r.get('Scanner', '')
        if sym not in symbol_scanners:
            symbol_scanners[sym] = set()
        symbol_scanners[sym].add(sc)

    def confluence_bonus(sym):
        scanners      = symbol_scanners.get(sym, set())
        n             = len(scanners)
        has_structure = bool(scanners & {'Pivot', 'Trendline', 'Darvas', 'Accumulation'})
        has_trigger   = bool(scanners & {'Breakout', 'Inside Bar'})
        has_momentum  = bool(scanners & {'Momentum'})
        lifecycle     = sum([has_structure, has_trigger, has_momentum])
        if lifecycle == 3: return 5
        if lifecycle == 2: return 2
        if n >= 3:         return 3
        if n == 2:         return 1
        return 0

    all_df['_confluence'] = all_df['Symbol'].apply(confluence_bonus)
    all_df['_total']      = all_df['_final_score'] + all_df['_confluence']
    all_df = all_df.sort_values('_total', ascending=False)

    # ── guaranteed 1 slot per scanner, fill rest globally ──
    seen     = set()
    reserved = {}

    for _, row in all_df.iterrows():
        sym = row.get('Symbol', '')
        sc  = row.get('Scanner', '')
        if sc not in reserved and sym not in seen:
            reserved[sc] = row
            seen.add(sym)

    # fill remaining slots
    top = []
    for _, row in all_df.iterrows():
        sym = row.get('Symbol', '')
        if sym not in seen:
            seen.add(sym)
            top.append(row)
        if len(top) >= top_n - len(reserved):
            break

    reserved_list = sorted(reserved.values(), key=lambda r: r['_total'], reverse=True)
    final_rows    = reserved_list + top
    final_df      = pd.DataFrame(final_rows).sort_values('_total', ascending=False).head(top_n)
    return final_df


# ─────────────────────────────────────────
# BUILD EMAIL HTML
# ─────────────────────────────────────────

def build_email_html(top_df, day):
    if top_df.empty:
        return "<p>No high-quality setups found today.</p>"

    rows_html = ''
    for _, r in top_df.iterrows():
        direction = str(r.get('Direction', '')) if pd.notna(r.get('Direction')) else '—'
        color = '#00cc66' if 'Long' in direction else '#ff4444' if 'Short' in direction else '#888888'
        scanner   = r.get('Scanner', '')
        symbol    = r.get('Symbol', '')
        exchange  = r.get('Exchange', '')
        price     = r.get('Price', '')
        signals   = str(r.get('Signals', r.get('Trigger', r.get('Setup', '-'))))
        vol_ratio = r.get('Vol Ratio', '')
        vol_str   = f"{vol_ratio:.1f}x" if isinstance(vol_ratio, float) else str(vol_ratio or '-')
        conf      = r.get('_confluence', 0)
        total     = r.get('_total', '')
        score_str = f"{round(float(total), 1)}{'  🔗' if conf > 0 else ''}" if total != '' else ''

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
          <td style="padding:10px 14px; text-align:center; color:#aaa;">{score_str}</td>
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

    <div style="background:#1a1d2e;border-radius:12px;border:1px solid #2a2d3e;
                overflow:hidden;margin-bottom:16px;">
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
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;font-size:0.82rem;">Score</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>

    <div style="background:#13151f;border-radius:8px;padding:12px 16px;
                border:1px solid #2a2d3e;margin-bottom:16px;font-size:0.78rem;color:#666;">
      <b style="color:#7c83fd;">How scores work:</b>
      &nbsp; Score = (Structure + Trigger×2 + Momentum) × log(Vol Ratio)
      &nbsp;|&nbsp; 🔗 = appears in multiple scanners (confluence bonus)
    </div>

    <p style="color:#444;font-size:0.78rem;margin-top:16px;text-align:center;">
      Generated by Stock Screener at 8:30 AM IST &nbsp;|&nbsp;
      Not financial advice. Do your own research.
    </p>
  </div>
</body>
</html>"""


# ─────────────────────────────────────────
# SEND EMAIL
# ─────────────────────────────────────────

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


# ─────────────────────────────────────────
# MAIN DIGEST JOB
# ─────────────────────────────────────────

def run_daily_digest(cache_ref):
    day = get_last_trading_day()
    print(f"\n[Digest] Starting daily digest for {day}...")
    try:
        preload_all(cache_ref)
        top_df = pick_top_setups(cache_ref, top_n=10)
        print(f"[Digest] Top setups: {len(top_df)}")
        if not top_df.empty:
            print(top_df[['Symbol', 'Exchange', 'Scanner', 'Direction',
                           'Price', '_st', '_tr', '_mo',
                           '_confluence', '_total']].to_string(index=False))
        send_digest_email(top_df, day)
    except Exception as e:
        print(f"[Digest] Error: {e}")
        import traceback
        traceback.print_exc()