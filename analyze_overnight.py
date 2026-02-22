"""Quick analysis of overnight production data."""
import pandas as pd
import glob
from datetime import datetime, timezone

def safe_load(pattern):
    files = glob.glob(pattern, recursive=True)
    dfs = []
    corrupt = 0
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception:
            corrupt += 1
    if corrupt:
        print(f"  (skipped {corrupt} corrupt files out of {len(files)})")
    return pd.concat(dfs) if dfs else pd.DataFrame(), len(files)

# Load paper fills
fills, n_fill_files = safe_load("data_warehouse/paper_fill/**/*.parquet")

# Load trades
trades, n_trade_files = safe_load("data_warehouse/trade/**/*.parquet")

# Load orderbook deltas
ob, n_ob_files = safe_load("data_warehouse/orderbook_delta/**/*.parquet")

print("=" * 60)
print("OVERNIGHT PRODUCTION DATA ANALYSIS")
print("=" * 60)

# 1. Data volume
print(f"\n--- DATA VOLUME ---")
print(f"Trade events:     {len(trades):,}")
print(f"Orderbook deltas: {len(ob):,}")
print(f"Paper fills:      {len(fills):,}")
print(f"Parquet files:    {n_fill_files} fills, {n_trade_files} trades, {n_ob_files} ob")

if len(trades) > 0:
    t_min = pd.to_datetime(trades["exchange_ts"].min(), unit="ms")
    t_max = pd.to_datetime(trades["exchange_ts"].max(), unit="ms")
    hours = (trades["exchange_ts"].max() - trades["exchange_ts"].min()) / 3.6e6
    print(f"\n--- TIME SPAN ---")
    print(f"From: {t_min} UTC")
    print(f"To:   {t_max} UTC")
    print(f"Duration: {hours:.1f} hours")

    print(f"\n--- TRADE ANALYSIS ---")
    print(f"Unique tickers traded: {trades['ticker'].nunique()}")
    kxbtc = trades[trades["ticker"].str.startswith("KXBTC")]
    print(f"KXBTC trades: {len(kxbtc):,}")
    print(f"KXBTC tickers: {kxbtc['ticker'].nunique()}")
    
    if "size" in trades.columns:
        print(f"Total volume (all): {trades['size'].sum():,} contracts")
        print(f"KXBTC volume: {kxbtc['size'].sum():,} contracts")
        print(f"Avg trade size: {trades['size'].mean():.1f} contracts")
        print(f"Max trade size: {trades['size'].max()} contracts")
    
    if "price_cents" in trades.columns:
        print(f"\n--- PRICE DISTRIBUTION (KXBTC) ---")
        for t in kxbtc["ticker"].unique()[:10]:
            sub = kxbtc[kxbtc["ticker"] == t]
            print(f"  {t}: {len(sub)} trades, price {sub['price_cents'].min()}-{sub['price_cents'].max()}c, vol {sub['size'].sum()}")
    
    # Trades per hour
    if len(kxbtc) > 0:
        kxbtc_ts = pd.to_datetime(kxbtc["exchange_ts"], unit="ms")
        hourly = kxbtc_ts.dt.hour.value_counts().sort_index()
        print(f"\n--- KXBTC TRADES BY HOUR (UTC) ---")
        for h, c in hourly.items():
            print(f"  {h:02d}:00 UTC = {c} trades")

if len(fills) > 0:
    print(f"\n--- PAPER FILL ANALYSIS ---")
    print(f"Total fills: {len(fills)}")
    if "size" in fills.columns:
        print(f"Total contracts filled: {fills['size'].sum()}")
    if "side" in fills.columns:
        print(f"Side breakdown:")
        print(fills["side"].value_counts().to_string())
    if "price_cents" in fills.columns:
        print(f"Avg fill price: {fills['price_cents'].mean():.1f}c")
        print(f"Price range: {fills['price_cents'].min()}-{fills['price_cents'].max()}c")
    if "ticker" in fills.columns:
        print(f"Tickers filled: {fills['ticker'].unique().tolist()}")
    
    # Fill timing
    f_min = pd.to_datetime(fills["exchange_ts"].min(), unit="ms")
    f_max = pd.to_datetime(fills["exchange_ts"].max(), unit="ms")
    print(f"Fill time span: {f_min} to {f_max}")
    
    # Check for new columns
    new_cols = [c for c in ["queue_ahead_at_fill", "time_since_quote_ms", "fair_prob_at_quote", "sigma_at_quote", "tte_ms_at_quote"] if c in fills.columns]
    if new_cols:
        print(f"\n--- QUOTE CONTEXT (new fields) ---")
        for c in new_cols:
            vals = fills[c].dropna()
            if len(vals) > 0:
                print(f"  {c}: mean={vals.mean():.4f} min={vals.min():.4f} max={vals.max():.4f}")

if len(ob) > 0:
    print(f"\n--- ORDERBOOK DELTA ANALYSIS ---")
    print(f"Total deltas: {len(ob):,}")
    if "ticker" in ob.columns:
        print(f"Tickers with book data: {ob['ticker'].nunique()}")
    if "price_cents" in ob.columns:
        print(f"Price range in books: {ob['price_cents'].min()}-{ob['price_cents'].max()}c")
    if "size" in ob.columns:
        print(f"Avg delta size: {ob['size'].abs().mean():.1f}")
        print(f"Max delta size: {ob['size'].abs().max()}")

print(f"\n{'=' * 60}")
print("ANALYSIS COMPLETE")
print(f"{'=' * 60}")
