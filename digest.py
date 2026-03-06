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

MAX_CONFLUENCE_BONUS = 4

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
            preload_histories(symbols, exch, intervals=('1d','1wk'), lookback_bars=252)
            store_stats()
        print(f"[Digest] {exch} preload complete.")

    from scanner import run_scan
    from darvas_scanner import run_darvas_scan
    from trendline_scanner import run_trendline_scan
    from inside_bar_scanner import run_inside_bar_scan
    from accumulation_scanner import run_accumulation_scan
    from momentum_scanner import run_momentum_scan
    from volume_helper import enrich_with_volume

    print("[Digest] Warming scanner caches...")
    for scan_fn, kwargs, key in [
        (run_scan,              {'exchange': 'BOTH'},                                        'pivot_BOTH'),
        (run_darvas_scan,       {'exchange': 'BOTH', 'direction': 'BOTH'},                   'darvas_BOTH'),
        (run_trendline_scan,    {'exchange': 'BOTH'},                                        'trendline_BOTH'),
        (run_inside_bar_scan,   {'exchange': 'BOTH', 'direction': 'BOTH', 'n': 2},          'insidebar_BOTH_2'),
        (run_accumulation_scan, {'exchange': 'BOTH', 'min_score': 1, 'min_vol_ratio': 1.5}, 'accumulation_BOTH_1'),
        (run_momentum_scan,     {'exchange': 'BOTH', 'min_score': 2},                       'momentum_BOTH_2'),
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
# PER-ROW BASE SCORES
# ─────────────────────────────────────────

def structure_score(r, scanner):
    s = 0
    if scanner == 'Pivot':
        hit = str(r.get('Weekly Hit', '')) + str(r.get('Monthly Hit', ''))
        if 'Inside CPR' in hit:  s += 2
        elif 'Near' in hit:      s += 1
        if r.get('Narrow CPR (W)') or r.get('Narrow CPR (M)'): s += 1
    elif scanner == 'Trendline':
        s += 2 if 'Horizontal' in str(r.get('Setup', '')) else 1
    elif scanner == 'Darvas':
        s += 2
    elif scanner == 'Accumulation':
        s += min(int(r.get('Score', 0) or 0), 2)
    return s


def trigger_score(r, scanner):
    s = 0
    trigger = str(r.get('Trigger', ''))
    if scanner == 'Inside Bar':
        if trigger in ('Breakout', 'Breakdown'): s += 2
        elif trigger == 'Attempt':               s += 1
        if str(r.get('Both TF', '')).startswith('⭐'): s += 1
    elif scanner == 'Darvas':
        if 'Fresh' in trigger or trigger == 'Breakout': s += 2
        elif 'Retest' in trigger:     s += 1
    elif scanner == 'Momentum':
        if r.get('Agree') == '✅':    s += 2
        if int(r.get('Score', 0) or 0) >= 3: s += 1
    elif scanner == 'Pivot':
        if 'Narrow CPR' in str(r.get('Setup', '')): s += 1
    return s


def momentum_score(r, scanner):
    s = 0
    if scanner == 'Momentum':
        if r.get('Agree') == '✅':     s += 2
        if r.get('Vol Spike') == '✅': s += 1
        if any(x in str(r.get('RSI Type', ''))
               for x in ('Bullish Divergence', 'Bearish Divergence')): s += 1
    elif scanner == 'Accumulation':
        if r.get('OBV↑') == '✅':      s += 1
        if r.get('Vol Spike') == '✅': s += 1
    return s


def vol_multiplier(r):
    try:
        vr = max(float(r.get('Vol Ratio', 1) or 1), 1.0)
    except:
        vr = 1.0
    return 1 + math.log(vr)


# ─────────────────────────────────────────
# CONFLUENCE TIER DETECTION
# ─────────────────────────────────────────

STRUCTURE_SCANNERS  = {'Pivot', 'Trendline', 'Darvas', 'Accumulation'}
TRIGGER_SCANNERS    = {'Inside Bar'}
MOMENTUM_SCANNERS   = {'Momentum'}

TIER1_PAIRS = [
    {'Inside Bar', 'Momentum'},
    {'Accumulation', 'Inside Bar'},
    {'Trendline', 'Inside Bar'},
    {'Darvas', 'Inside Bar'},
    {'Pivot', 'Inside Bar'},
]

TIER2_PAIRS = [
    {'Trendline', 'Momentum'},
    {'Pivot', 'Momentum'},
    {'Accumulation', 'Momentum'},
    {'Darvas', 'Momentum'},
    {'Inside Bar', 'Accumulation'},
]

TIER3_PAIRS = [
    {'Trendline', 'Inside Bar'},
    {'Pivot', 'Inside Bar'},
    {'Pivot', 'Accumulation'},
    {'Trendline', 'Accumulation'},
    {'Darvas', 'Accumulation'},
    {'Pivot', 'Darvas'},
]

SETUP_LABELS = [
    ({'Inside Bar', 'Momentum'},      'Compression Expansion'),
    ({'Accumulation', 'Inside Bar'},  'Accumulation + Compression'),
    ({'Trendline', 'Inside Bar'},     'Support + Compression'),
    ({'Darvas', 'Inside Bar'},        'Box Compression Breakout'),
    ({'Pivot', 'Inside Bar'},         'Pivot + Compression'),
    ({'Trendline', 'Momentum'},       'Trend Continuation'),
    ({'Accumulation', 'Momentum'},    'Accumulation + Momentum'),
    ({'Pivot', 'Momentum'},           'Pivot + Momentum'),
    ({'Inside Bar', 'Accumulation'},  'Compression + Accumulation'),
    ({'Darvas', 'Momentum'},          'Box + Momentum'),
]

FULL_STACK_LABEL = 'Full Stack Setup 🔥'


def get_confluence_bonus(scanners_set):
    has_structure = bool(scanners_set & STRUCTURE_SCANNERS)
    has_trigger   = bool(scanners_set & TRIGGER_SCANNERS)
    has_momentum  = bool(scanners_set & MOMENTUM_SCANNERS)
    if has_structure and has_trigger and has_momentum:
        return MAX_CONFLUENCE_BONUS
    if len(scanners_set) >= 3:
        return MAX_CONFLUENCE_BONUS

    bonus = 0
    for pair in TIER1_PAIRS:
        if pair.issubset(scanners_set):
            bonus = max(bonus, 3)
    for pair in TIER2_PAIRS:
        if pair.issubset(scanners_set):
            bonus = max(bonus, 2)
    for pair in TIER3_PAIRS:
        if pair.issubset(scanners_set):
            bonus = max(bonus, 1)

    return min(bonus, MAX_CONFLUENCE_BONUS)


def get_setup_label(scanners_set):
    has_structure = bool(scanners_set & STRUCTURE_SCANNERS)
    has_trigger   = bool(scanners_set & TRIGGER_SCANNERS)
    has_momentum  = bool(scanners_set & MOMENTUM_SCANNERS)
    if (has_structure and has_trigger and has_momentum) or len(scanners_set) >= 3:
        return FULL_STACK_LABEL
    for pair, label in SETUP_LABELS:
        if pair.issubset(scanners_set):
            return label
    return list(scanners_set)[0] if scanners_set else ''


def get_scanner_display(scanners_set):
    order   = ['Accumulation', 'Trendline', 'Darvas', 'Pivot', 'Inside Bar', 'Momentum']
    ordered = [s for s in order if s in scanners_set]
    return ' + '.join(ordered)


# ─────────────────────────────────────────
# MERGE-FIRST ARCHITECTURE
# ─────────────────────────────────────────

def pick_top_setups(cache_ref, top_n=10):

    scanner_map = {
        'Pivot':        cache_ref.get('pivot_BOTH'),
        'Darvas':       cache_ref.get('darvas_BOTH'),
        'Trendline':    cache_ref.get('trendline_BOTH'),
        'Inside Bar':   cache_ref.get('insidebar_BOTH_2'),
        'Accumulation': cache_ref.get('accumulation_BOTH_1'),
        'Momentum':     cache_ref.get('momentum_BOTH_2'),
    }

    all_rows = []
    for scanner_name, df in scanner_map.items():
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            row = dict(r)
            row['_scanner'] = scanner_name
            row['_st']      = structure_score(r, scanner_name)
            row['_tr']      = trigger_score(r, scanner_name)
            row['_mo']      = momentum_score(r, scanner_name)
            base            = row['_st'] + row['_tr'] * 2 + row['_mo']
            row['_base']    = base
            row['_vm']      = vol_multiplier(r)
            row['_raw']     = base * row['_vm']
            all_rows.append(row)

    if not all_rows:
        return pd.DataFrame()

    from collections import defaultdict
    groups = defaultdict(list)
    for row in all_rows:
        key = (row.get('Symbol', ''), row.get('Exchange', ''))
        groups[key].append(row)

    merged = []
    for (sym, exch), rows in groups.items():
        scanners_set = {r['_scanner'] for r in rows}

        best_per_scanner = {}
        for r in rows:
            sc = r['_scanner']
            if sc not in best_per_scanner or r['_raw'] > best_per_scanner[sc]['_raw']:
                best_per_scanner[sc] = r

        rep        = max(best_per_scanner.values(), key=lambda r: r['_raw'])
        total_base = sum(r['_base'] for r in best_per_scanner.values())
        vm         = rep['_vm']
        raw_score  = total_base * vm
        conf_bonus  = get_confluence_bonus(scanners_set)
        final_score = raw_score + conf_bonus

        merged.append({
            'Symbol':           sym,
            'Exchange':         exch,
            'Price':            rep.get('Price', ''),
            'Direction':        rep.get('Direction', ''),
            'Score':            round(final_score, 1),
            'Vol Ratio':        rep.get('Vol Ratio', ''),
            'Signals':          rep.get('Signals', rep.get('Trigger', rep.get('Setup', ''))),
            'Both TF':          rep.get('Both TF', ''),
            'Scanner':          get_scanner_display(scanners_set),
            'Setup':            get_setup_label(scanners_set),
            '_scanners':        scanners_set,
            '_scanner_display': get_scanner_display(scanners_set),
            '_setup_label':     get_setup_label(scanners_set),
            '_confluence':      conf_bonus,
            '_raw':             round(raw_score, 3),
            '_total':           round(final_score, 3),
            '_st':              sum(r['_st'] for r in best_per_scanner.values()),
            '_tr':              sum(r['_tr'] for r in best_per_scanner.values()),
            '_mo':              sum(r['_mo'] for r in best_per_scanner.values()),
            '_n_scanners':      len(scanners_set),
        })

    merged_df = pd.DataFrame(merged)
    merged_df  = merged_df.sort_values('_total', ascending=False)

    seen     = set()
    reserved = {}
    for _, row in merged_df.iterrows():
        sym      = row['Symbol']
        scanners = row['_scanners']
        for cat, cat_set in [('structure', STRUCTURE_SCANNERS),
                              ('trigger',   TRIGGER_SCANNERS),
                              ('momentum',  MOMENTUM_SCANNERS)]:
            if cat not in reserved and bool(scanners & cat_set) and sym not in seen:
                reserved[cat] = row
                seen.add(sym)

    top = []
    for _, row in merged_df.iterrows():
        sym = row['Symbol']
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
# RUN DIGEST SCAN  (called by /api/top10)
# ─────────────────────────────────────────

def run_digest_scan(top_n=20):
    """
    Run all scanners and return a ranked DataFrame of top setups.
    Used by /api/top10 in app.py — runs its own isolated cache so it
    doesn't pollute the main app cache.
    top_n=20 so we have enough rows for longs/shorts/overall top 10 each.
    """
    local_cache = {}
    preload_all(local_cache)
    return pick_top_setups(local_cache, top_n=top_n)


# ─────────────────────────────────────────
# BUILD EMAIL HTML
# ─────────────────────────────────────────

def build_email_html(top_df, day):
    if top_df.empty:
        return "<p>No high-quality setups found today.</p>"

    rows_html = ''
    for _, r in top_df.iterrows():
        direction = str(r.get('Direction', ''))
        if pd.isna(r.get('Direction')) or not direction or direction == 'nan':
            direction = '—'
        color = '#00cc66' if 'Long'  in direction else \
                '#ff4444' if 'Short' in direction else '#888888'

        symbol        = r.get('Symbol', '')
        exchange      = r.get('Exchange', '')
        price         = r.get('Price', '')
        scanner_disp  = r.get('_scanner_display', '')
        setup_label   = r.get('_setup_label', '')
        conf          = r.get('_confluence', 0)
        total         = r.get('_total', '')
        n_sc          = r.get('_n_scanners', 1)
        vol_ratio     = r.get('Vol Ratio', '')
        vol_str       = f"{vol_ratio:.1f}x" if isinstance(vol_ratio, float) else str(vol_ratio or '-')
        both_tf       = '⭐ Both TF' if str(r.get('Both TF', '')).startswith('⭐') else ''
        score_str     = f"{round(float(total), 1)}" if total != '' else ''
        conf_badge    = f'<span style="color:#ffcc44;font-size:0.75rem;">🔗 +{conf}</span>' \
                        if conf > 0 else ''
        sc_color  = '#00ffcc' if n_sc >= 3 else '#7c83fd' if n_sc == 2 else '#666'
        sc_weight = '700' if n_sc >= 2 else '400'

        rows_html += f"""
        <tr style="border-bottom:1px solid #1e2130;">
          <td style="padding:12px 14px;">
            <div style="font-weight:700;color:#e0e0e0;font-size:0.95rem;">{symbol}</div>
            <div style="font-size:0.75rem;color:#555;margin-top:2px;">{exchange}</div>
          </td>
          <td style="padding:12px 14px;">
            <div style="color:{sc_color};font-weight:{sc_weight};font-size:0.82rem;">{scanner_disp}</div>
            <div style="color:#ffaa44;font-size:0.75rem;margin-top:3px;font-style:italic;">{setup_label}</div>
          </td>
          <td style="padding:12px 14px;">
            <span style="color:{color};font-weight:700;">{direction}</span>
          </td>
          <td style="padding:12px 14px;color:#e0e0e0;">₹{price}</td>
          <td style="padding:12px 14px;color:#ffaa44;font-weight:600;">{vol_str}</td>
          <td style="padding:12px 14px;">
            <div style="color:#aaa;font-weight:700;font-size:0.95rem;">{score_str}</div>
            <div style="margin-top:2px;">{conf_badge} {both_tf}</div>
          </td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0f1117;
             font-family:'Segoe UI',sans-serif;color:#e0e0e0;">
  <div style="max-width:860px;margin:0 auto;padding:24px;">

    <div style="background:#1a1d2e;border-radius:12px;padding:24px 32px;
                border:1px solid #2a2d3e;margin-bottom:24px;">
      <h1 style="color:#7c83fd;margin:0 0 6px;font-size:1.4rem;letter-spacing:1px;">
        📊 Stock Screener — Daily Digest
      </h1>
      <p style="color:#666;margin:0;font-size:0.9rem;">
        {day} &nbsp;|&nbsp; Cross-scanner confluence ranking &nbsp;|&nbsp; NSE + BSE
      </p>
    </div>

    <div style="background:#1a1d2e;border-radius:12px;border:1px solid #2a2d3e;
                overflow:hidden;margin-bottom:16px;">
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#13151f;border-bottom:2px solid #2a2d3e;">
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;width:120px;">Symbol</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;">Scanners / Setup</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;">Direction</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;">Price</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;">Vol</th>
            <th style="padding:12px 14px;color:#7c83fd;text-align:left;
                       font-size:0.82rem;">Score</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>

    <div style="background:#13151f;border-radius:8px;padding:12px 16px;
                border:1px solid #2a2d3e;margin-bottom:16px;
                font-size:0.78rem;color:#666;line-height:1.8;">
      <b style="color:#7c83fd;">How scores work:</b>
      &nbsp; (Structure + Trigger×2 + Momentum) × log(Vol Ratio) + Confluence Bonus
      <br>
      <b style="color:#7c83fd;">Confluence tiers:</b>
      &nbsp; Full Stack 🔥 +4 &nbsp;|&nbsp;
      Tier 1 (Inside Bar+Momentum etc.) +3 &nbsp;|&nbsp;
      Tier 2 (Structure+Momentum) +2 &nbsp;|&nbsp;
      Tier 3 +1 &nbsp;|&nbsp; max +4
      <br>
      <b style="color:#7c83fd;">Scanner colors:</b>
      &nbsp;
      <span style="color:#00ffcc;">Cyan = 3+ scanners</span> &nbsp;|&nbsp;
      <span style="color:#7c83fd;">Purple = 2 scanners</span> &nbsp;|&nbsp;
      <span style="color:#666;">Grey = single scanner</span>
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
            print(top_df[[
                'Symbol', 'Exchange', '_scanner_display', '_setup_label',
                'Direction', 'Price', '_st', '_tr', '_mo',
                '_confluence', '_n_scanners', '_total'
            ]].to_string(index=False))
        send_digest_email(top_df, day)
    except Exception as e:
        print(f"[Digest] Error: {e}")
        import traceback
        traceback.print_exc()