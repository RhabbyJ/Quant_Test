import streamlit as st
import duckdb
import pandas as pd
import glob
import logging
import os

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

            def highlight_side(val):
                color = "#4CAF50" if val else "#F44336"
                return f"background-color: {color}"

            display_cols = ["Time", "ticker", "price_cents", "size", "is_bid"]
            st.dataframe(fill_df[display_cols].head(20).style.map(highlight_side, subset=["is_bid"]))

            bids = fill_df[fill_df["is_bid"] == True]["size"].sum()
            asks = fill_df[fill_df["is_bid"] == False]["size"].sum()
            net_inventory = bids - asks
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
        total_value = fill_df.apply(
            lambda row: row["price_cents"] * row["size"] * (-1 if row["is_bid"] else 1), axis=1
        ).sum()
        dollars = total_value / 100.0
        st.metric("Gross Filled Value (Unrealized)", f"${dollars:.2f}")


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
