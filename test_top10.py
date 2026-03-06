"""
Quick test — prints Top 20 digest results to terminal.
Run: python test_top10.py
"""
from digest import run_digest_scan

print("\nRunning digest scan...\n")
df = run_digest_scan(top_n=20)

if df is None or df.empty:
    print("No results found.")
else:
    cols = ['Symbol', 'Direction', 'Price', 'Score', 'Scanner', 'Setup',
            '_n_scanners', '_st', '_tr', '_mo', '_confluence', '_total']
    # only show cols that exist
    cols = [c for c in cols if c in df.columns]
    print(f"Total setups: {len(df)}\n")
    print(df[cols].to_string(index=False))
    print("\n--- Longs ---")
    longs = df[df['Direction'].str.contains('Long', na=False)]
    print(f"{len(longs)} longs")
    print("\n--- Shorts ---")
    shorts = df[df['Direction'].str.contains('Short', na=False)]
    print(f"{len(shorts)} shorts")
