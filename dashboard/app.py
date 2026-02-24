import streamlit as st
import duckdb
import pandas as pd
import glob
import json
import logging
import os
import random
from pathlib import Path

from research.markouts import compute_fill_markouts, normalize_side

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
    meta_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/market_meta/**/*.parquet", recursive=True)]
    audit_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/quote_audit/**/*.parquet", recursive=True)]
    edge_files = [p.replace("\\", "/") for p in glob.glob("data_warehouse/edge_metric/**/*.parquet", recursive=True)]

    spot_df = _read_channel(con, "spot", spot_files, 1000)
    trade_df = _read_channel(con, "trade", trade_files, 500)
    ob_df = _read_channel(con, "orderbook_delta", ob_files, 500)
    fill_df = _read_channel(con, "paper_fill", fill_files, 500)
    meta_df = _read_channel(con, "market_meta", meta_files, 500)
    audit_df = _read_channel(con, "quote_audit", audit_files, 500)
    edge_df = _read_channel(con, "edge_metric", edge_files, 1000)

    return spot_df, trade_df, ob_df, fill_df, meta_df, audit_df, edge_df


def _mean_ci95(series: pd.Series) -> tuple[float, float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return float("nan"), float("nan")
    mean = float(values.mean())
    if len(values) < 2:
        return mean, float("nan")
    std = float(values.std(ddof=1))
    ci_half = 1.96 * std / max(1.0, (len(values) ** 0.5))
    return mean, ci_half


def _bootstrap_ci95(series: pd.Series, samples: int = 300, seed: int = 0) -> tuple[float, float]:
    values = pd.to_numeric(series, errors="coerce").dropna().tolist()
    if not values:
        return float("nan"), float("nan")
    if len(values) < 2:
        v = float(values[0])
        return v, v
    rng = random.Random(seed + len(values))
    n = len(values)
    means = []
    draws = max(50, int(samples))
    for _ in range(draws):
        sample_mean = sum(values[rng.randrange(n)] for __ in range(n)) / n
        means.append(sample_mean)
    means.sort()
    lo = means[int(0.025 * (len(means) - 1))]
    hi = means[int(0.975 * (len(means) - 1))]
    return float(lo), float(hi)


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
    return normalize_side(value)


def _compute_markouts(fill_df: pd.DataFrame, ob_df: pd.DataFrame) -> pd.DataFrame:
    return compute_fill_markouts(fill_df=fill_df, ob_df=ob_df)


def _render_health_panel(spot_df: pd.DataFrame, trade_df: pd.DataFrame, ob_df: pd.DataFrame):
    now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    warmup_samples = int(os.getenv("WARMUP_SAMPLES", "300"))
    runtime = _load_runtime_status()
    heartbeat_ms_default = int(runtime.get("kalshi_heartbeat_ms", os.getenv("KALSHI_HEARTBEAT_MS", "30000")))
    stale_default_sec = max(1, (heartbeat_ms_default + 999) // 1000)
    stale_sec = int(os.getenv("HEALTH_STALE_SEC", str(stale_default_sec)))

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
            f"health_stale_sec={runtime.get('health_stale_sec','n/a')} "
            f"vol_half_life_sec={runtime.get('vol_half_life_sec','n/a')} "
            f"ewma_decay_factor={runtime.get('ewma_decay_factor','n/a')} "
            f"vol_spread_mult={runtime.get('vol_spread_mult','n/a')} "
            f"quote_size={runtime.get('quote_size','n/a')} "
            f"edge_horizon_ms={runtime.get('edge_horizon_ms','n/a')} "
            f"spread_gov_window={runtime.get('spread_governor_window_fills','n/a')} "
            f"spread_gov_min_fills={runtime.get('spread_governor_min_fills','n/a')} "
            f"profit_kpi_min_fills={runtime.get('profit_kpi_min_fills','n/a')} "
            f"market_rank={runtime.get('market_rank_enabled','n/a')} "
            f"realism={runtime.get('paper_realism_mode','n/a')} "
            f"risk_auto_recover_ms={runtime.get('risk_auto_recover_ms','n/a')} "
            f"min_quote_tte_ms={runtime.get('min_quote_tte_ms','n/a')} "
            f"market_min_trades_hr={runtime.get('market_min_trades_per_hour','n/a')} "
            f"market_min_top_depth={runtime.get('market_min_top_depth','n/a')} "
            f"dead_failover={runtime.get('dead_market_failover_enabled', False)} "
            f"fixed_ladder={runtime.get('research_fixed_ladder', False)} "
            f"tickers={tickers_str}"
        )


def _render_live_panels(show_mock_spot: bool):
    spot_df, trade_df, ob_df, fill_df, meta_df, audit_df, edge_df = fetch_data()
    runtime = _load_runtime_status()
    _render_health_panel(spot_df, trade_df, ob_df)

    st.subheader("Contract Specs (Exchange Metadata)")
    if not meta_df.empty:
        specs = meta_df.copy()
        specs["exchange_ts"] = pd.to_numeric(specs["exchange_ts"], errors="coerce")
        specs = specs.dropna(subset=["exchange_ts"])
        specs = specs.sort_values("exchange_ts", ascending=False).drop_duplicates(subset=["ticker"], keep="first")
        if "close_ts" in specs.columns:
            specs["close_time_utc"] = pd.to_datetime(pd.to_numeric(specs["close_ts"], errors="coerce"), unit="ms", utc=True)
        cols = [
            c
            for c in [
                "ticker",
                "direction",
                "strike_low",
                "strike_high",
                "close_time_utc",
                "settlement_window",
                "oracle_risk_score",
                "oracle_blocked",
                "oracle_reason",
            ]
            if c in specs.columns
        ]
        st.dataframe(specs[cols].head(20))
    else:
        st.info("No market metadata rows yet.")

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
            for col in [
                "queue_ahead_at_fill",
                "time_since_quote_ms",
                "fair_prob_at_quote",
                "sigma_at_quote",
                "tte_ms_at_quote",
            ]:
                if col in fill_df.columns:
                    display_cols.append(col)
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
                    avg_markout_cents=("signed_markout_cents", "mean"),
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

            short_h = markout_df[markout_df["horizon_s"] == 1]
            if not short_h.empty:
                signed_markout_1s = float(short_h["signed_markout_cents"].mean())
                fill_rate_per_min = 0.0
                if not fill_df.empty and "exchange_ts" in fill_df.columns:
                    ts = pd.to_numeric(fill_df["exchange_ts"], errors="coerce").dropna()
                    if not ts.empty:
                        span_ms = max(1.0, float(ts.max() - ts.min()))
                        fill_rate_per_min = len(fill_df) / max(1e-9, (span_ms / 60000.0))
                toxicity_score = (-signed_markout_1s) * fill_rate_per_min
                t1, t2, t3 = st.columns(3)
                t1.metric("Signed 1s Markout", f"{signed_markout_1s:.2f}c")
                t2.metric("Fill Rate", f"{fill_rate_per_min:.2f}/min")
                t3.metric("Toxicity Score", f"{toxicity_score:.3f}")

            # Use one horizon for calibration to avoid triple-counting each fill across 1s/10s/60s rows.
            calib_horizons = sorted(markout_df["horizon_s"].dropna().unique().tolist())
            calib_horizon = 10 if 10 in calib_horizons else (calib_horizons[0] if calib_horizons else None)
            calib_source = markout_df if calib_horizon is None else markout_df[markout_df["horizon_s"] == calib_horizon]
            calib = calib_source.dropna(subset=["fair_prob_at_quote", "realized_yes_prob"]).copy()
            if not calib.empty:
                calib["fair_prob_at_quote"] = pd.to_numeric(calib["fair_prob_at_quote"], errors="coerce")
                calib["realized_yes_prob"] = pd.to_numeric(calib["realized_yes_prob"], errors="coerce")
                calib = calib.dropna(subset=["fair_prob_at_quote", "realized_yes_prob"])
                calib = calib[(calib["fair_prob_at_quote"] >= 0) & (calib["fair_prob_at_quote"] <= 1)]
                calib = calib[(calib["realized_yes_prob"] >= 0) & (calib["realized_yes_prob"] <= 1)]
                if not calib.empty:
                    calib = calib.sort_values("fill_ts")
                    sq_err = (calib["fair_prob_at_quote"] - calib["realized_yes_prob"]) ** 2
                    calib["bin"] = pd.cut(calib["fair_prob_at_quote"], bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], include_lowest=True)
                    by_bin = (
                        calib.groupby("bin", observed=False, as_index=False)
                        .agg(
                            samples=("realized_yes_prob", "count"),
                            predicted_p=("fair_prob_at_quote", "mean"),
                            realized_p=("realized_yes_prob", "mean"),
                        )
                    )
                    by_bin["calibration_error"] = by_bin["predicted_p"] - by_bin["realized_p"]
                    brier = float(sq_err.mean())
                    rolling_window = min(100, len(calib))
                    rolling_brier = float(sq_err.rolling(window=rolling_window, min_periods=10).mean().iloc[-1]) if len(calib) >= 10 else float("nan")
                    st.subheader("Calibration Telemetry (Fill-based)")
                    c1, c2 = st.columns(2)
                    c1.metric("Brier Score (soft realized)", f"{brier:.4f}")
                    if pd.notna(rolling_brier):
                        c2.metric(f"Rolling Brier ({rolling_window} fills)", f"{rolling_brier:.4f}")
                    else:
                        c2.metric("Rolling Brier", "n/a")
                    if calib_horizon is not None:
                        st.caption(f"Calibration horizon: {int(calib_horizon)}s")
                    st.dataframe(by_bin)

    st.subheader("Fee-Adjusted Edge (Profit Compass)")
    st.caption("Per-contract: signed_markout(horizon) - maker_fee - slippage_buffer")
    if not edge_df.empty:
        edge = edge_df.copy()
        edge["exchange_ts"] = pd.to_numeric(edge["exchange_ts"], errors="coerce")
        edge["fee_adjusted_edge_cents"] = pd.to_numeric(edge["fee_adjusted_edge_cents"], errors="coerce")
        edge["signed_markout_cents"] = pd.to_numeric(edge["signed_markout_cents"], errors="coerce")
        edge["maker_fee_cents_per_contract"] = pd.to_numeric(edge["maker_fee_cents_per_contract"], errors="coerce")
        edge = edge.dropna(subset=["exchange_ts", "fee_adjusted_edge_cents"])
        edge = edge.sort_values("exchange_ts")
        edge["Time"] = pd.to_datetime(edge["exchange_ts"], unit="ms", utc=True)

        mean_edge, ci_half = _mean_ci95(edge["fee_adjusted_edge_cents"])
        lcb, ucb = _bootstrap_ci95(edge["fee_adjusted_edge_cents"], samples=300, seed=42)
        min_fills_gate = int(runtime.get("profit_kpi_min_fills", runtime.get("spread_governor_min_fills", 50)))
        gate_pass = (len(edge) >= min_fills_gate) and pd.notna(lcb) and (lcb > 0.0)
        c1, c2, c3 = st.columns(3)
        c1.metric("Mean Fee-Adj Edge", f"{mean_edge:.3f}c")
        c2.metric("95% CI Half-Width", f"{ci_half:.3f}c" if pd.notna(ci_half) else "n/a")
        c3.metric("Samples", f"{len(edge)}")
        c4, c5, c6 = st.columns(3)
        c4.metric("Bootstrap LCB", f"{lcb:.3f}c" if pd.notna(lcb) else "n/a")
        c5.metric("Bootstrap UCB", f"{ucb:.3f}c" if pd.notna(ucb) else "n/a")
        c6.metric("Profit Gate", "PASS" if gate_pass else "FAIL")

        if "fill_size" in edge.columns:
            edge["fill_size"] = pd.to_numeric(edge["fill_size"], errors="coerce").fillna(0.0)
            span_ms = max(1.0, float(edge["exchange_ts"].max() - edge["exchange_ts"].min()))
            fills_per_hour = len(edge) * (3600000.0 / span_ms)
            avg_fill_size = float(edge["fill_size"].mean()) if len(edge) > 0 else 0.0
            expected_edge_per_hour_cents = mean_edge * fills_per_hour * avg_fill_size
            expected_edge_per_hour_usd = expected_edge_per_hour_cents / 100.0
            p1, p2, p3 = st.columns(3)
            p1.metric("Fills / Hour", f"{fills_per_hour:.2f}")
            p2.metric("Avg Fill Size", f"{avg_fill_size:.2f}")
            p3.metric("Expected Edge / Hour", f"${expected_edge_per_hour_usd:.2f}")

        by_side = (
            edge.groupby("side", as_index=False)
            .agg(
                samples=("fee_adjusted_edge_cents", "count"),
                mean_fee_adj_edge_cents=("fee_adjusted_edge_cents", "mean"),
                mean_signed_markout_cents=("signed_markout_cents", "mean"),
                mean_fee_cents=("maker_fee_cents_per_contract", "mean"),
            )
            .sort_values("samples", ascending=False)
        )
        st.dataframe(by_side)

        by_ticker = (
            edge.groupby("ticker", as_index=False)
            .agg(
                samples=("fee_adjusted_edge_cents", "count"),
                mean_fee_adj_edge_cents=("fee_adjusted_edge_cents", "mean"),
                std_fee_adj_edge_cents=("fee_adjusted_edge_cents", "std"),
            )
            .sort_values("mean_fee_adj_edge_cents", ascending=False)
        )
        st.dataframe(by_ticker.head(20))

        if "sigma_at_quote" in edge.columns:
            edge["sigma_at_quote"] = pd.to_numeric(edge["sigma_at_quote"], errors="coerce")
            sigma_vals = edge["sigma_at_quote"].dropna()
            if len(sigma_vals) >= 10:
                q1, q2 = sigma_vals.quantile([0.33, 0.66]).tolist()
                edge["vol_regime"] = pd.cut(
                    edge["sigma_at_quote"],
                    bins=[-float("inf"), q1, q2, float("inf")],
                    labels=["low_vol", "mid_vol", "high_vol"],
                )
                vol_summary = (
                    edge.groupby("vol_regime", observed=False, as_index=False)
                    .agg(
                        samples=("fee_adjusted_edge_cents", "count"),
                        mean_fee_adj_edge_cents=("fee_adjusted_edge_cents", "mean"),
                    )
                )
                st.dataframe(vol_summary)

        if "tte_ms_at_quote" in edge.columns:
            edge["tte_ms_at_quote"] = pd.to_numeric(edge["tte_ms_at_quote"], errors="coerce")
            edge["tte_min"] = edge["tte_ms_at_quote"] / 60000.0
            edge["expiry_window"] = pd.cut(
                edge["tte_min"],
                bins=[-float("inf"), 15, 60, 180, float("inf")],
                labels=["<15m", "15-60m", "60-180m", ">180m"],
            )
            expiry_summary = (
                edge.groupby("expiry_window", observed=False, as_index=False)
                .agg(
                    samples=("fee_adjusted_edge_cents", "count"),
                    mean_fee_adj_edge_cents=("fee_adjusted_edge_cents", "mean"),
                )
            )
            st.dataframe(expiry_summary)

        show_cols = [
            "Time",
            "ticker",
            "side",
            "horizon_s",
            "signed_markout_cents",
            "maker_fee_cents_per_contract",
            "slippage_buffer_cents",
            "fee_adjusted_edge_cents",
            "sigma_at_quote",
            "tte_ms_at_quote",
        ]
        show_cols = [c for c in show_cols if c in edge.columns]
        st.dataframe(edge[show_cols].tail(50).sort_values("Time", ascending=False))
    else:
        st.info("No edge samples yet. Need fills + horizon markout data.")

    st.subheader("Quote Decision Audit (Why We Quoted Here)")
    if not audit_df.empty:
        aud = audit_df.copy()
        aud["Time"] = pd.to_datetime(pd.to_numeric(aud["exchange_ts"], errors="coerce"), unit="ms")
        display_cols = [
            "Time",
            "ticker",
            "fair_prob",
            "sigma",
            "tte_ms",
            "sigma_t",
            "inventory",
            "inventory_skew",
            "yes_bid_cents",
            "no_bid_cents",
            "half_spread_cents",
            "fee_widen_steps",
        ]
        display_cols = [c for c in display_cols if c in aud.columns]
        st.dataframe(aud[display_cols].head(30))
    else:
        st.info("No quote audit rows yet.")


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
