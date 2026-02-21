import streamlit as st
import duckdb
import pandas as pd
import glob
import json
import logging
import os
from bisect import bisect_left
from pathlib import Path

st.set_page_config(layout="wide", page_title="Kalshi Institutional MM MVP")

# Guardrail #5 (Streamlit refresh interval)
# and Markouts strictly using exchanged_ts


@st.cache_resource
def _dashboard_state() -> dict:
    return {
        "bad_files": set(),
        "fast_path_disabled": set(),
        "warned_channels": set(),
    }


def _read_channel(con: duckdb.DuckDBPyConnection, channel: str, files: list[str], limit: int) -> pd.DataFrame:
    """
    Read a parquet channel defensively.
    Fast path reads all files in one query.
    Fallback skips unreadable files (e.g., truncated/partial parquet) instead of crashing the UI.
    """
    state = _dashboard_state()
    bad_files = state["bad_files"]
    fast_path_disabled = state["fast_path_disabled"]
    warned_channels = state["warned_channels"]

    files = [f for f in files if f not in bad_files]
    if not files:
        return pd.DataFrame()

    # DuckDB list-parameter reads can be fragile with very large file sets.
    # Use fast path only for smaller channel sets.
    use_fast_path = len(files) <= 500 and channel not in fast_path_disabled
    if use_fast_path:
        try:
            return con.execute(
                "SELECT * FROM read_parquet(?) ORDER BY exchange_ts DESC LIMIT ?",
                [files, limit]
            ).df()
        except Exception as e:
            fast_path_disabled.add(channel)
            if channel not in warned_channels:
                logging.warning("[%s] fast parquet read failed; switching to per-file scan: %s", channel, e)
                warned_channels.add(channel)

    collected = []
    rows = 0
    new_bad_files = []

    # Newest files first by path ordering (date/hour/timestamp naming).
    for path in sorted(files, reverse=True):
        try:
            df = con.execute("SELECT * FROM read_parquet(?)", [path]).df()
        except Exception:
            # DuckDB aborts the active transaction after a query error.
            # Roll back so subsequent files are still readable in this loop.
            try:
                con.rollback()
            except Exception:
                pass
            if path not in bad_files:
                bad_files.add(path)
                new_bad_files.append(path)
            continue

        if df.empty:
            continue

        collected.append(df)
        rows += len(df)

        # Collect a little extra before final sort/truncate.
        if rows >= (limit * 2):
            break

    if new_bad_files:
        sample = ", ".join(os.path.basename(p) for p in new_bad_files[:3])
        logging.warning("[%s] skipped %d unreadable parquet file(s): %s", channel, len(new_bad_files), sample)

    if not collected:
        return pd.DataFrame()

    merged = pd.concat(collected, ignore_index=True)
    if "exchange_ts" in merged.columns:
        merged = merged.sort_values("exchange_ts", ascending=False)
    return merged.head(limit)


def fetch_data():
    con = duckdb.connect()

    spot_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/spot/**/*.parquet", recursive=True)]
    trade_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/trade/**/*.parquet", recursive=True)]
    ob_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/orderbook_delta/**/*.parquet", recursive=True)]
    fill_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/paper_fill/**/*.parquet", recursive=True)]

    spot_df = _read_channel(con, "spot", spot_files, 1000)
    trade_df = _read_channel(con, "trade", trade_files, 500)
    ob_df = _read_channel(con, "orderbook_delta", ob_files, 500)
    fill_df = _read_channel(con, "paper_fill", fill_files, 500)

    return spot_df, trade_df, ob_df, fill_df


def _latest_exchange_ts_ms(df: pd.DataFrame) -> int | None:
    if df.empty or "exchange_ts" not in df.columns:
        return None
    try:
        return int(pd.to_numeric(df["exchange_ts"], errors="coerce").dropna().max())
    except Exception:
        return None


def _format_age(age_ms: int | None) -> str:
    if age_ms is None:
        return "n/a"
    age_ms = max(0, int(age_ms))
    if age_ms < 1000:
        return f"{age_ms} ms"
    return f"{age_ms / 1000.0:.1f} s"


def _load_runtime_status() -> dict:
    path = Path("data_warehouse/runtime/status.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalize_side(value) -> str:
    if value is None:
        return "unknown"
    s = str(value).strip().lower()
    if "yes_bid" in s or s == "yes":
        return "yes_bid"
    if "no_bid" in s or s == "no":
        return "no_bid"
    return s


def _build_mid_series(ob_df: pd.DataFrame) -> dict[str, tuple[list[int], list[float]]]:
    if ob_df.empty:
        return {}
    required = {"ticker", "exchange_ts", "side", "price_cents", "size"}
    if not required.issubset(set(ob_df.columns)):
        return {}

    deltas = ob_df[list(required)].copy()
    deltas["exchange_ts"] = pd.to_numeric(deltas["exchange_ts"], errors="coerce")
    deltas["price_cents"] = pd.to_numeric(deltas["price_cents"], errors="coerce")
    deltas["size"] = pd.to_numeric(deltas["size"], errors="coerce")
    deltas["side"] = deltas["side"].map(_normalize_side)
    deltas = deltas.dropna(subset=["exchange_ts", "price_cents", "size"])
    if deltas.empty:
        return {}

    deltas["exchange_ts"] = deltas["exchange_ts"].astype("int64")
    deltas["price_cents"] = deltas["price_cents"].astype("int64")
    deltas["size"] = deltas["size"].astype("int64")
    deltas = deltas.sort_values(["ticker", "exchange_ts"]).reset_index(drop=True)

    yes_books: dict[str, dict[int, int]] = {}
    no_books: dict[str, dict[int, int]] = {}
    out_ts: dict[str, list[int]] = {}
    out_mid: dict[str, list[float]] = {}

    for row in deltas.itertuples(index=False):
        ticker = str(row.ticker)
        side = row.side
        price = int(row.price_cents)
        delta = int(row.size)
        if side not in {"yes_bid", "no_bid"}:
            continue

        yes_book = yes_books.setdefault(ticker, {})
        no_book = no_books.setdefault(ticker, {})
        target = yes_book if side == "yes_bid" else no_book

        new_size = target.get(price, 0) + delta
        if new_size <= 0:
            target.pop(price, None)
        else:
            target[price] = new_size

        if not yes_book or not no_book:
            continue

        best_yes = max(yes_book.keys())
        best_no = max(no_book.keys())
        implied_yes_ask = 100 - best_no
        mid_yes = (best_yes + implied_yes_ask) / 2.0

        out_ts.setdefault(ticker, []).append(int(row.exchange_ts))
        out_mid.setdefault(ticker, []).append(float(mid_yes))

    mids: dict[str, tuple[list[int], list[float]]] = {}
    for ticker, ts_vals in out_ts.items():
        mids[ticker] = (ts_vals, out_mid.get(ticker, []))
    return mids


def _compute_markouts(fill_df: pd.DataFrame, ob_df: pd.DataFrame) -> pd.DataFrame:
    if fill_df.empty:
        return pd.DataFrame()
    required = {"ticker", "exchange_ts", "price_cents", "size"}
    if not required.issubset(set(fill_df.columns)):
        return pd.DataFrame()

    mids = _build_mid_series(ob_df)
    if not mids:
        return pd.DataFrame()

    fills = fill_df.copy()
    fills["exchange_ts"] = pd.to_numeric(fills["exchange_ts"], errors="coerce")
    fills["price_cents"] = pd.to_numeric(fills["price_cents"], errors="coerce")
    fills["size"] = pd.to_numeric(fills["size"], errors="coerce")
    fills = fills.dropna(subset=["exchange_ts", "price_cents", "size"])
    if fills.empty:
        return pd.DataFrame()

    if "side" in fills.columns:
        fills["side_norm"] = fills["side"].map(_normalize_side)
    elif "is_bid" in fills.columns:
        fills["side_norm"] = fills["is_bid"].map(lambda b: "yes_bid" if bool(b) else "no_bid")
    else:
        fills["side_norm"] = "unknown"

    horizons_sec = [1, 10, 60]
    rows = []
    for fill in fills.itertuples(index=False):
        ticker = str(fill.ticker)
        side = str(fill.side_norm)
        if ticker not in mids or side not in {"yes_bid", "no_bid"}:
            continue
        ts_list, mid_list = mids[ticker]
        if not ts_list or not mid_list:
            continue

        fill_ts = int(fill.exchange_ts)
        fill_price = float(fill.price_cents)
        size = int(fill.size)
        fill_yes_price = fill_price if side == "yes_bid" else (100.0 - fill_price)
        sign = 1.0 if side == "yes_bid" else -1.0

        for h in horizons_sec:
            target_ts = fill_ts + (h * 1000)
            idx = bisect_left(ts_list, target_ts)
            if idx >= len(ts_list):
                continue
            mid_after = float(mid_list[idx])
            markout_cents = sign * (mid_after - fill_yes_price)
            rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "horizon_s": h,
                    "fill_ts": fill_ts,
                    "markout_cents": markout_cents,
                    "size": size,
                    "markout_usd": (markout_cents * size) / 100.0,
                }
            )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _render_health_panel(spot_df: pd.DataFrame, trade_df: pd.DataFrame, ob_df: pd.DataFrame):
    now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    warmup_samples = int(os.getenv("WARMUP_SAMPLES", "300"))
    stale_sec = int(os.getenv("HEALTH_STALE_SEC", "15"))

    last_spot = _latest_exchange_ts_ms(spot_df)
    last_trade = _latest_exchange_ts_ms(trade_df)
    last_ob = _latest_exchange_ts_ms(ob_df)

    spot_age = (now_ms - last_spot) if last_spot is not None else None
    trade_age = (now_ms - last_trade) if last_trade is not None else None
    ob_age = (now_ms - last_ob) if last_ob is not None else None

    ws_live = ob_age is not None and ob_age <= (stale_sec * 1000)
    warmup_ready = (len(spot_df) >= warmup_samples) and (not ob_df.empty)

    st.subheader("System Health")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Kalshi Feed", "Live" if ws_live else "Stale/Unknown")
    c2.metric("Orderbook Age", _format_age(ob_age))
    c3.metric("Trade Age", _format_age(trade_age))
    c4.metric("Model Spot Age", _format_age(spot_age))
    c5.metric("Warmup Gate", "Ready" if warmup_ready else "Collecting")

    if not warmup_ready:
        st.caption(f"Warmup progress: spot_samples={len(spot_df)}/{warmup_samples}, orderbook_ready={not ob_df.empty}")

    runtime = _load_runtime_status()
    if runtime:
        tickers = runtime.get("tickers", [])
        tickers_str = ", ".join(tickers[:3]) if tickers else "n/a"
        st.caption(
            "Run config: "
            f"env={runtime.get('kalshi_env','n/a')} "
            f"mode={runtime.get('kalshi_mode','n/a')} "
            f"discovery_used={runtime.get('discovery_used', False)} "
            f"close={runtime.get('discovered_close_iso','n/a')} "
            f"kalshi_heartbeat_ms={runtime.get('kalshi_heartbeat_ms','n/a')} "
            f"min_quote_tte_ms={runtime.get('min_quote_tte_ms','n/a')} "
            f"tickers={tickers_str}"
        )


def _render_live_panels(show_mock_spot: bool):
    spot_df, trade_df, ob_df, fill_df = fetch_data()
    _render_health_panel(spot_df, trade_df, ob_df)

    if show_mock_spot:
        st.subheader("Mock BTC Spot (Simulation Input)")
        st.caption("This is not a real market spot feed. It is generated by MockSpotFeed for model warmup.")
        if not spot_df.empty:
            spot_df["Time"] = pd.to_datetime(spot_df["exchange_ts"], unit="ms")
            chart_df = (
                spot_df[["Time", "price"]]
                .sort_values("Time")
                .tail(1000)
                .set_index("Time")["price"]
            )
            st.line_chart(chart_df)
        else:
            st.info("No model spot samples yet.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Kalshi Trades (Live)")
        if not trade_df.empty:
            trade_df["Time"] = pd.to_datetime(trade_df["exchange_ts"], unit="ms")
            st.dataframe(trade_df[["Time", "ticker", "price_cents", "size", "side"]].head(20))
        else:
            st.info("No Kalshi trades received yet.")

    with col2:
        st.subheader("Paper Fills (Virtual Order Executions)")
        if not fill_df.empty:
            fill_df["Time"] = pd.to_datetime(fill_df["exchange_ts"], unit="ms")

            if "side" in fill_df.columns:
                fill_df["side"] = fill_df["side"].map(_normalize_side)
            elif "is_bid" in fill_df.columns:
                fill_df["side"] = fill_df["is_bid"].map(lambda x: "yes_bid" if bool(x) else "no_bid")
            else:
                fill_df["side"] = "unknown"

            def highlight_side(val):
                if val == "yes_bid":
                    color = "#4CAF50"
                elif val == "no_bid":
                    color = "#FF9800"
                else:
                    color = "#F44336"
                return f"background-color: {color}"

            display_cols = ["Time", "ticker", "side", "price_cents", "size"]
            st.dataframe(fill_df[display_cols].head(20).style.map(highlight_side, subset=["side"]))

            yes_bids = fill_df[fill_df["side"] == "yes_bid"]["size"].sum()
            no_bids = fill_df[fill_df["side"] == "no_bid"]["size"].sum()
            net_inventory = yes_bids - no_bids
            st.metric("Net MVP Inventory", f"{net_inventory} contracts")
        else:
            st.info("No paper fills yet.")

    st.subheader("Kalshi Orderbook Deltas (Live)")
    if not ob_df.empty:
        ob_df["Time"] = pd.to_datetime(ob_df["exchange_ts"], unit="ms")
        st.dataframe(ob_df[["Time", "ticker", "side", "price_cents", "size"]].head(20))
    else:
        st.info("No orderbook delta rows yet.")

    st.subheader("P&L Attribution & Markouts (1s / 10s / 60s)")
    st.markdown(
        """
    *Metrics calculated purely on exchange timestamps (`exchange_ts`). Local ingestion times are stored but discarded for metric validity.*
    """
    )
    if not fill_df.empty:
        markout_df = _compute_markouts(fill_df, ob_df)
        if markout_df.empty:
            st.info("Not enough orderbook history yet to compute 1s/10s/60s markouts.")
        else:
            summary = (
                markout_df.groupby("horizon_s", as_index=False)
                .agg(
                    fills=("markout_usd", "count"),
                    contracts=("size", "sum"),
                    avg_markout_cents=("markout_cents", "mean"),
                    total_markout_usd=("markout_usd", "sum"),
                )
                .sort_values("horizon_s")
            )
            cols = st.columns(max(1, len(summary)))
            for idx, row in enumerate(summary.itertuples(index=False)):
                cols[idx].metric(
                    f"{int(row.horizon_s)}s Markout",
                    f"{row.avg_markout_cents:.2f}c",
                    f"${row.total_markout_usd:.2f} total",
                )

            st.dataframe(
                summary.rename(
                    columns={
                        "horizon_s": "Horizon (s)",
                        "fills": "Fills",
                        "contracts": "Contracts",
                        "avg_markout_cents": "Avg Markout (c)",
                        "total_markout_usd": "Total Markout ($)",
                    }
                )
            )


def main():
    st.title("Kalshi Prediction Market Maker (MVP)")
    st.markdown("Live Inventory-Aware Market Making on BTC Threshold Ladders")

    live_update = st.sidebar.checkbox("Live Update", value=True)
    refresh_sec = st.sidebar.slider("Refresh Seconds", min_value=1, max_value=30, value=2)
    show_mock_spot = st.sidebar.checkbox("Show Mock Spot Panel", value=False)

    if live_update:
        @st.fragment(run_every=f"{refresh_sec}s")
        def _live_fragment():
            _render_live_panels(show_mock_spot)

        _live_fragment()
    else:
        _render_live_panels(show_mock_spot)


if __name__ == "__main__":
    main()
