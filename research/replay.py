import argparse
import asyncio
import itertools
import json
import math
from datetime import datetime, timezone
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from core.types import LifecycleEvent, MarketMetadataEvent, OrderbookDeltaEvent, Side, SpotEvent, TradeEvent
from engine.simulator import EngineLoop
from research.loaders import load_channel
from research.markouts import compute_fill_markouts


@dataclass
class ReplayConfig:
    base_dir: str = "data_warehouse"
    start_ts_ms: Optional[int] = None
    end_ts_ms: Optional[int] = None
    tickers: Optional[list[str]] = None
    warmup_samples: Optional[int] = None
    spot_heartbeat_ms: Optional[int] = None
    kalshi_heartbeat_ms: Optional[int] = None
    min_quote_tte_ms: Optional[int] = None
    quoter_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayResult:
    config: ReplayConfig
    summary: dict[str, Any]
    fills_df: pd.DataFrame
    markouts_df: pd.DataFrame


class ReplayStore:
    """In-memory sink for EngineLoop ingest calls during offline replay."""

    def __init__(self):
        self.events: dict[str, list[dict[str, Any]]] = {}

    async def ingest_event(self, channel: str, event: Any):
        row = asdict(event)
        if "side" in row and hasattr(row["side"], "value"):
            row["side"] = row["side"].value
        self.events.setdefault(channel, []).append(row)

    def to_df(self, channel: str) -> pd.DataFrame:
        return pd.DataFrame(self.events.get(channel, []))


def _default_run_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _write_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    if df.shape[1] > 0:
        df.to_parquet(parquet_path, index=False)


def persist_replay_result(
    result: ReplayResult,
    run_root: str = "research_runs",
    run_id: str | None = None,
) -> Path:
    rid = run_id or _default_run_id("replay")
    run_dir = Path(run_root) / rid
    run_dir.mkdir(parents=True, exist_ok=True)

    config_payload = asdict(result.config)
    summary_payload = dict(result.summary)
    (run_dir / "config.json").write_text(json.dumps(config_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    summary_df = pd.DataFrame([summary_payload])
    _write_df(summary_df, run_dir / "summary.parquet", run_dir / "summary.csv")
    _write_df(result.fills_df, run_dir / "fills.parquet", run_dir / "fills.csv")
    _write_df(result.markouts_df, run_dir / "markouts.parquet", run_dir / "markouts.csv")
    return run_dir


def persist_sweep_results(
    sweep_df: pd.DataFrame,
    base_cfg: ReplayConfig,
    grid: dict[str, list[Any]],
    run_root: str = "research_runs",
    run_id: str | None = None,
) -> Path:
    rid = run_id or _default_run_id("sweep")
    run_dir = Path(run_root) / rid
    run_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "base_config": asdict(base_cfg),
        "grid": grid,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (run_dir / "sweep_meta.json").write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_df(sweep_df, run_dir / "sweep.parquet", run_dir / "sweep.csv")
    return run_dir


def _is_nan(val: Any) -> bool:
    if pd.isna(val):
        return True
    try:
        return bool(math.isnan(val))
    except Exception:
        return False


def _to_int(raw: Any, default: int = 0) -> int:
    if raw is None or _is_nan(raw):
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _to_float(raw: Any, default: float = 0.0) -> float:
    if raw is None or _is_nan(raw):
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _parse_side(raw: Any) -> Optional[Side]:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if "yes" in s:
        return Side.YES_BID
    if "no" in s:
        return Side.NO_BID
    return None


def _filter_df(df: pd.DataFrame, cfg: ReplayConfig) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "exchange_ts" in out.columns:
        out["exchange_ts"] = pd.to_numeric(out["exchange_ts"], errors="coerce")
        out = out.dropna(subset=["exchange_ts"])
        out["exchange_ts"] = out["exchange_ts"].astype("int64")
        if cfg.start_ts_ms is not None:
            out = out[out["exchange_ts"] >= int(cfg.start_ts_ms)]
        if cfg.end_ts_ms is not None:
            out = out[out["exchange_ts"] <= int(cfg.end_ts_ms)]
    if cfg.tickers and "ticker" in out.columns:
        ticker_set = set(cfg.tickers)
        out = out[out["ticker"].astype(str).isin(ticker_set)]
    return out


def _load_replay_channels(cfg: ReplayConfig) -> dict[str, pd.DataFrame]:
    channels = ["market_meta", "orderbook_delta", "trade", "spot", "lifecycle"]
    loaded = {}
    for ch in channels:
        df = load_channel(ch, base_dir=cfg.base_dir)
        loaded[ch] = _filter_df(df, cfg)
    return loaded


def _build_replay_events(data: dict[str, pd.DataFrame]) -> list[tuple[int, int, Any]]:
    priority = {
        "market_meta": 0,
        "orderbook_delta": 1,
        "trade": 2,
        "spot": 3,
        "lifecycle": 4,
    }
    events: list[tuple[int, int, Any]] = []

    for _, row in data["market_meta"].iterrows():
        ts = _to_int(row.get("exchange_ts"))
        if ts <= 0:
            continue
        events.append(
            (
                ts,
                priority["market_meta"],
                MarketMetadataEvent(
                    exchange_ts=ts,
                    ingest_ts=_to_int(row.get("ingest_ts"), ts),
                    ticker=str(row.get("ticker", "")),
                    close_ts=_to_int(row.get("close_ts")),
                    status=str(row.get("status", "")).lower() or None,
                    direction=str(row.get("direction", "")).lower() or None,
                    strike_low=_to_float(row.get("strike_low")) if "strike_low" in row else None,
                    strike_high=_to_float(row.get("strike_high")) if "strike_high" in row else None,
                    settlement_window=str(row.get("settlement_window", "")) or None,
                    oracle_risk_score=_to_float(row.get("oracle_risk_score")) if "oracle_risk_score" in row else None,
                    oracle_blocked=(
                        None
                        if ("oracle_blocked" not in row or _is_nan(row.get("oracle_blocked")))
                        else bool(row.get("oracle_blocked"))
                    ),
                    oracle_reason=str(row.get("oracle_reason", "")) or None,
                ),
            )
        )

    for _, row in data["orderbook_delta"].iterrows():
        ts = _to_int(row.get("exchange_ts"))
        side = _parse_side(row.get("side"))
        if ts <= 0 or side is None:
            continue
        events.append(
            (
                ts,
                priority["orderbook_delta"],
                OrderbookDeltaEvent(
                    exchange_ts=ts,
                    ingest_ts=_to_int(row.get("ingest_ts"), ts),
                    ticker=str(row.get("ticker", "")),
                    price_cents=_to_int(row.get("price_cents")),
                    size=_to_int(row.get("size")),
                    side=side,
                    seq=_to_int(row.get("seq")) if "seq" in row else None,
                ),
            )
        )

    for _, row in data["trade"].iterrows():
        ts = _to_int(row.get("exchange_ts"))
        if ts <= 0:
            continue
        events.append(
            (
                ts,
                priority["trade"],
                TradeEvent(
                    exchange_ts=ts,
                    ingest_ts=_to_int(row.get("ingest_ts"), ts),
                    ticker=str(row.get("ticker", "")),
                    price_cents=_to_int(row.get("price_cents")),
                    size=_to_int(row.get("size")),
                    side=_parse_side(row.get("side")),
                    seq=_to_int(row.get("seq")) if "seq" in row else None,
                ),
            )
        )

    for _, row in data["spot"].iterrows():
        ts = _to_int(row.get("exchange_ts"))
        if ts <= 0:
            continue
        events.append(
            (
                ts,
                priority["spot"],
                SpotEvent(
                    exchange_ts=ts,
                    ingest_ts=_to_int(row.get("ingest_ts"), ts),
                    ticker=str(row.get("ticker", "BTC")),
                    price=_to_float(row.get("price")),
                ),
            )
        )

    for _, row in data["lifecycle"].iterrows():
        ts = _to_int(row.get("exchange_ts"))
        if ts <= 0:
            continue
        events.append(
            (
                ts,
                priority["lifecycle"],
                LifecycleEvent(
                    exchange_ts=ts,
                    ingest_ts=_to_int(row.get("ingest_ts"), ts),
                    ticker=str(row.get("ticker", "")),
                    status=str(row.get("status", "unknown")).lower(),
                    seq=_to_int(row.get("seq")) if "seq" in row else None,
                ),
            )
        )

    events.sort(key=lambda x: (x[0], x[1]))
    return events


def _derive_tickers(data: dict[str, pd.DataFrame], fallback: Optional[list[str]] = None) -> list[str]:
    if fallback:
        return [t for t in fallback if t]
    tickers = set()
    for ch in ("market_meta", "orderbook_delta", "trade", "lifecycle"):
        df = data.get(ch)
        if df is None or df.empty or "ticker" not in df.columns:
            continue
        tickers.update(df["ticker"].dropna().astype(str).tolist())
    return sorted(tickers)


def _apply_config(engine: EngineLoop, cfg: ReplayConfig):
    if cfg.warmup_samples is not None:
        engine.risk.warmup_samples = int(cfg.warmup_samples)
    if cfg.spot_heartbeat_ms is not None:
        engine.risk.spot_heartbeat_ms = int(cfg.spot_heartbeat_ms)
    if cfg.kalshi_heartbeat_ms is not None:
        engine.risk.kalshi_heartbeat_ms = int(cfg.kalshi_heartbeat_ms)
    if cfg.min_quote_tte_ms is not None:
        engine.min_quote_tte_ms = int(cfg.min_quote_tte_ms)

    for key, val in cfg.quoter_overrides.items():
        if hasattr(engine.quoter, key):
            setattr(engine.quoter, key, val)


def _summarize(
    cfg: ReplayConfig,
    engine: EngineLoop,
    fills_df: pd.DataFrame,
    markouts_df: pd.DataFrame,
    edge_df: pd.DataFrame,
    quote_audit_df: pd.DataFrame,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "events_processed": 0,
        "fills": 0,
        "fill_contracts": 0,
        "final_net_yes": int(sum(engine.quoter.inventory.values())) if engine.quoter.inventory else 0,
        "tickers": ",".join(sorted(engine.active_tickers)) if engine.active_tickers else "",
    }
    if not fills_df.empty:
        summary["fills"] = int(len(fills_df))
        if "size" in fills_df.columns:
            summary["fill_contracts"] = int(pd.to_numeric(fills_df["size"], errors="coerce").fillna(0).sum())
        else:
            summary["fill_contracts"] = 0
        if "time_since_quote_ms" in fills_df.columns:
            summary["avg_time_since_quote_ms"] = float(
                pd.to_numeric(fills_df["time_since_quote_ms"], errors="coerce").dropna().mean()
            )
    else:
        summary["avg_time_since_quote_ms"] = float("nan")

    if not markouts_df.empty:
        for horizon in sorted(markouts_df["horizon_s"].unique()):
            subset = markouts_df[markouts_df["horizon_s"] == horizon]
            summary[f"markout_{int(horizon)}s_count"] = int(len(subset))
            summary[f"markout_{int(horizon)}s_avg_cents"] = float(subset["markout_cents"].mean())
            summary[f"markout_{int(horizon)}s_total_usd"] = float(subset["markout_usd"].sum())
            if "fee_adjusted_edge_cents" in subset.columns:
                summary[f"fee_adjusted_edge_{int(horizon)}s_avg_cents"] = float(subset["fee_adjusted_edge_cents"].mean())
                summary[f"fee_adjusted_edge_{int(horizon)}s_total_cents"] = float(subset["fee_adjusted_edge_cents"].sum())

    if not edge_df.empty:
        tmp = edge_df.copy()
        tmp["exchange_ts"] = pd.to_numeric(tmp.get("exchange_ts"), errors="coerce")
        tmp["fee_adjusted_edge_cents"] = pd.to_numeric(tmp.get("fee_adjusted_edge_cents"), errors="coerce")
        tmp["signed_markout_cents"] = pd.to_numeric(tmp.get("signed_markout_cents"), errors="coerce")
        tmp["fill_size"] = pd.to_numeric(tmp.get("fill_size"), errors="coerce")
        tmp = tmp.dropna(subset=["exchange_ts", "fee_adjusted_edge_cents"])
        if not tmp.empty:
            summary["edge_samples"] = int(len(tmp))
            summary["edge_mean_cents"] = float(tmp["fee_adjusted_edge_cents"].mean())
            if len(tmp) >= 2:
                summary["edge_std_cents"] = float(tmp["fee_adjusted_edge_cents"].std(ddof=1))
                summary["edge_ci95_half_cents"] = float(1.96 * summary["edge_std_cents"] / math.sqrt(len(tmp)))
            else:
                summary["edge_std_cents"] = float("nan")
                summary["edge_ci95_half_cents"] = float("nan")
            span_ms = max(1.0, float(tmp["exchange_ts"].max() - tmp["exchange_ts"].min()))
            fills_per_hour = len(tmp) * (3600000.0 / span_ms)
            avg_fill_size = float(tmp["fill_size"].dropna().mean()) if tmp["fill_size"].notna().any() else 0.0
            edge_per_hour_cents = summary["edge_mean_cents"] * fills_per_hour * avg_fill_size
            summary["fills_per_hour"] = float(fills_per_hour)
            summary["avg_fill_size"] = float(avg_fill_size)
            summary["edge_per_hour_cents"] = float(edge_per_hour_cents)
            summary["edge_per_hour_usd"] = float(edge_per_hour_cents / 100.0)
            mean_signed_markout = float(tmp["signed_markout_cents"].dropna().mean()) if tmp["signed_markout_cents"].notna().any() else float("nan")
            summary["toxicity"] = float((-mean_signed_markout) * (fills_per_hour / 60.0)) if not math.isnan(mean_signed_markout) else float("nan")

    if not quote_audit_df.empty and "inventory" in quote_audit_df.columns:
        inv = pd.to_numeric(quote_audit_df["inventory"], errors="coerce").dropna()
        if not inv.empty:
            summary["inventory_variance"] = float(inv.var(ddof=1)) if len(inv) > 1 else 0.0
    return summary


async def _run_replay_on_loaded_data(cfg: ReplayConfig, data: dict[str, pd.DataFrame]) -> ReplayResult:
    replay_events = _build_replay_events(data)
    tickers = _derive_tickers(data, fallback=cfg.tickers)

    store = ReplayStore()
    engine = EngineLoop(data_store=store, tickers=tickers if tickers else None)
    if tickers:
        engine.set_active_tickers(tickers)
    _apply_config(engine, cfg)

    idx = 0
    while idx < len(replay_events):
        ts = replay_events[idx][0]
        batch = []
        while idx < len(replay_events) and replay_events[idx][0] == ts:
            batch.append(replay_events[idx][2])
            idx += 1

        await engine._apply_exchange_events(batch)
        await engine._resolve_paper_fills(batch)
        await engine._run_strategy_tick(ts)

    fills_df = store.to_df("paper_fill")
    ob_df = data.get("orderbook_delta", pd.DataFrame())
    markouts_df = compute_fill_markouts(fill_df=fills_df, ob_df=ob_df)
    edge_df = store.to_df("edge_metric")
    quote_audit_df = store.to_df("quote_audit")
    summary = _summarize(
        cfg=cfg,
        engine=engine,
        fills_df=fills_df,
        markouts_df=markouts_df,
        edge_df=edge_df,
        quote_audit_df=quote_audit_df,
    )
    summary["events_processed"] = len(replay_events)
    return ReplayResult(config=cfg, summary=summary, fills_df=fills_df, markouts_df=markouts_df)


async def run_replay_async(cfg: ReplayConfig) -> ReplayResult:
    data = _load_replay_channels(cfg)
    return await _run_replay_on_loaded_data(cfg=cfg, data=data)


def run_replay(cfg: ReplayConfig) -> ReplayResult:
    return asyncio.run(run_replay_async(cfg))


def sweep_quoter_parameters(base_cfg: ReplayConfig, grid: dict[str, list[Any]]) -> pd.DataFrame:
    if not grid:
        return pd.DataFrame()
    loaded_data = _load_replay_channels(base_cfg)
    keys = list(grid.keys())
    rows = []
    for combo in itertools.product(*(grid[k] for k in keys)):
        overrides = dict(base_cfg.quoter_overrides)
        overrides.update(dict(zip(keys, combo)))
        cfg = ReplayConfig(
            base_dir=base_cfg.base_dir,
            start_ts_ms=base_cfg.start_ts_ms,
            end_ts_ms=base_cfg.end_ts_ms,
            tickers=base_cfg.tickers,
            warmup_samples=base_cfg.warmup_samples,
            spot_heartbeat_ms=base_cfg.spot_heartbeat_ms,
            kalshi_heartbeat_ms=base_cfg.kalshi_heartbeat_ms,
            min_quote_tte_ms=base_cfg.min_quote_tte_ms,
            quoter_overrides=overrides,
        )
        result = asyncio.run(_run_replay_on_loaded_data(cfg=cfg, data=loaded_data))
        row = {k: overrides.get(k) for k in keys}
        row.update(result.summary)
        rows.append(row)
    return pd.DataFrame(rows)


def _parse_keyvals(raw_items: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in raw_items:
        if "=" not in item:
            continue
        key, raw_val = item.split("=", 1)
        key = key.strip()
        val = raw_val.strip()
        if not key:
            continue
        try:
            if "." in val:
                out[key] = float(val)
            else:
                out[key] = int(val)
        except ValueError:
            out[key] = val
    return out


def _parse_grid(raw_items: list[str]) -> dict[str, list[Any]]:
    grid: dict[str, list[Any]] = {}
    for item in raw_items:
        if "=" not in item:
            continue
        key, raw_vals = item.split("=", 1)
        key = key.strip()
        if not key:
            continue
        vals = []
        for tok in raw_vals.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                if "." in tok:
                    vals.append(float(tok))
                else:
                    vals.append(int(tok))
            except ValueError:
                vals.append(tok)
        if vals:
            grid[key] = vals
    return grid


def main():
    parser = argparse.ArgumentParser(description="Deterministic offline replay over parquet events.")
    parser.add_argument("--base-dir", default="data_warehouse")
    parser.add_argument("--start-ts-ms", type=int, default=None)
    parser.add_argument("--end-ts-ms", type=int, default=None)
    parser.add_argument("--tickers", default="", help="Comma-separated ticker filter.")
    parser.add_argument("--warmup-samples", type=int, default=None)
    parser.add_argument("--spot-heartbeat-ms", type=int, default=None)
    parser.add_argument("--kalshi-heartbeat-ms", type=int, default=None)
    parser.add_argument("--min-quote-tte-ms", type=int, default=None)
    parser.add_argument("--quoter", action="append", default=[], help="Override: key=value")
    parser.add_argument("--sweep", action="append", default=[], help="Grid: key=v1,v2,v3")
    parser.add_argument("--out-csv", default="", help="Optional path to write summary/sweep CSV.")
    parser.add_argument("--persist", action="store_true", help="Persist run artifacts to disk.")
    parser.add_argument("--run-root", default="research_runs", help="Root folder for persisted artifacts.")
    parser.add_argument("--run-id", default="", help="Optional explicit run id folder name.")
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] if args.tickers else None
    cfg = ReplayConfig(
        base_dir=args.base_dir,
        start_ts_ms=args.start_ts_ms,
        end_ts_ms=args.end_ts_ms,
        tickers=tickers,
        warmup_samples=args.warmup_samples,
        spot_heartbeat_ms=args.spot_heartbeat_ms,
        kalshi_heartbeat_ms=args.kalshi_heartbeat_ms,
        min_quote_tte_ms=args.min_quote_tte_ms,
        quoter_overrides=_parse_keyvals(args.quoter),
    )

    grid = _parse_grid(args.sweep)
    if grid:
        df = sweep_quoter_parameters(cfg, grid)
        print(df.to_string(index=False))
        if args.out_csv:
            df.to_csv(args.out_csv, index=False)
        if args.persist:
            run_dir = persist_sweep_results(
                sweep_df=df,
                base_cfg=cfg,
                grid=grid,
                run_root=args.run_root,
                run_id=args.run_id or None,
            )
            print(f"persisted={run_dir}")
        return

    result = run_replay(cfg)
    print(pd.DataFrame([result.summary]).to_string(index=False))
    if args.out_csv:
        pd.DataFrame([result.summary]).to_csv(args.out_csv, index=False)
    if args.persist:
        run_dir = persist_replay_result(
            result=result,
            run_root=args.run_root,
            run_id=args.run_id or None,
        )
        print(f"persisted={run_dir}")


if __name__ == "__main__":
    main()
