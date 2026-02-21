import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd


@dataclass
class RunArtifacts:
    run_id: str
    run_dir: Path
    run_type: str
    summary: dict[str, Any]
    config: dict[str, Any]
    fills_df: pd.DataFrame
    markouts_df: pd.DataFrame
    sweep_df: pd.DataFrame
    sweep_meta: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_table(parquet_path: Path, csv_path: Path) -> pd.DataFrame:
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            pass
    if csv_path.exists():
        try:
            return pd.read_csv(csv_path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _detect_run_type(run_dir: Path) -> str:
    is_replay = (run_dir / "summary.json").exists() or (run_dir / "summary.csv").exists()
    is_sweep = (run_dir / "sweep.csv").exists() or (run_dir / "sweep.parquet").exists()
    if is_replay and is_sweep:
        return "mixed"
    if is_replay:
        return "replay"
    if is_sweep:
        return "sweep"
    return "unknown"


def load_run(run_root: str, run_id: str) -> RunArtifacts:
    run_dir = Path(run_root) / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Run not found: {run_dir}")
    run_type = _detect_run_type(run_dir)
    return RunArtifacts(
        run_id=run_id,
        run_dir=run_dir,
        run_type=run_type,
        summary=_read_json(run_dir / "summary.json"),
        config=_read_json(run_dir / "config.json"),
        fills_df=_read_table(run_dir / "fills.parquet", run_dir / "fills.csv"),
        markouts_df=_read_table(run_dir / "markouts.parquet", run_dir / "markouts.csv"),
        sweep_df=_read_table(run_dir / "sweep.parquet", run_dir / "sweep.csv"),
        sweep_meta=_read_json(run_dir / "sweep_meta.json"),
    )


def _is_number(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return not (isinstance(v, float) and math.isnan(v))
    return False


def _to_float(v: Any) -> float:
    if _is_number(v):
        return float(v)
    return float("nan")


def _fmt(v: Any) -> str:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def _flatten_config(cfg: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out = {}
    for k, v in cfg.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten_config(v, key))
        else:
            out[key] = v
    return out


def compare_replay_runs(base: RunArtifacts, cand: RunArtifacts) -> dict[str, pd.DataFrame]:
    metrics_rows = []
    keys = sorted(set(base.summary.keys()) | set(cand.summary.keys()))
    for key in keys:
        b = base.summary.get(key)
        c = cand.summary.get(key)
        if _is_number(b) or _is_number(c):
            bf = _to_float(b)
            cf = _to_float(c)
            delta = cf - bf if not (math.isnan(bf) or math.isnan(cf)) else float("nan")
            metrics_rows.append({"metric": key, "base": bf, "candidate": cf, "delta": delta})

    metrics_df = pd.DataFrame(metrics_rows).sort_values("metric") if metrics_rows else pd.DataFrame()

    cfg_rows = []
    bflat = _flatten_config(base.config)
    cflat = _flatten_config(cand.config)
    for key in sorted(set(bflat.keys()) | set(cflat.keys())):
        bv = bflat.get(key)
        cv = cflat.get(key)
        if json.dumps(bv, sort_keys=True, default=str) != json.dumps(cv, sort_keys=True, default=str):
            cfg_rows.append({"key": key, "base": _fmt(bv), "candidate": _fmt(cv)})
    config_df = pd.DataFrame(cfg_rows)

    def _markout_agg(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "horizon_s" not in df.columns:
            return pd.DataFrame()
        tmp = df.copy()
        tmp["horizon_s"] = pd.to_numeric(tmp["horizon_s"], errors="coerce")
        tmp["markout_cents"] = pd.to_numeric(tmp.get("markout_cents"), errors="coerce")
        tmp["markout_usd"] = pd.to_numeric(tmp.get("markout_usd"), errors="coerce")
        tmp = tmp.dropna(subset=["horizon_s"])
        if tmp.empty:
            return pd.DataFrame()
        out = (
            tmp.groupby("horizon_s", as_index=False)
            .agg(
                samples=("horizon_s", "count"),
                avg_markout_cents=("markout_cents", "mean"),
                total_markout_usd=("markout_usd", "sum"),
            )
            .sort_values("horizon_s")
        )
        out["horizon_s"] = out["horizon_s"].astype(int)
        return out

    bmk = _markout_agg(base.markouts_df)
    cmk = _markout_agg(cand.markouts_df)
    if not bmk.empty or not cmk.empty:
        markouts_df = bmk.merge(cmk, on="horizon_s", how="outer", suffixes=("_base", "_candidate"))
        for col in ("samples", "avg_markout_cents", "total_markout_usd"):
            markouts_df[f"{col}_delta"] = markouts_df[f"{col}_candidate"] - markouts_df[f"{col}_base"]
        markouts_df = markouts_df.sort_values("horizon_s")
    else:
        markouts_df = pd.DataFrame()

    def _fill_summary(df: pd.DataFrame) -> dict[str, float]:
        if df.empty:
            return {"fills": 0.0, "fill_contracts": 0.0, "avg_time_since_quote_ms": float("nan")}
        fills = float(len(df))
        contracts = float(pd.to_numeric(df.get("size"), errors="coerce").fillna(0).sum()) if "size" in df.columns else 0.0
        if "time_since_quote_ms" in df.columns:
            avg_tsq = float(pd.to_numeric(df["time_since_quote_ms"], errors="coerce").dropna().mean())
        else:
            avg_tsq = float("nan")
        return {"fills": fills, "fill_contracts": contracts, "avg_time_since_quote_ms": avg_tsq}

    bfill = _fill_summary(base.fills_df)
    cfill = _fill_summary(cand.fills_df)
    fill_rows = []
    for key in sorted(set(bfill.keys()) | set(cfill.keys())):
        bv = bfill.get(key, float("nan"))
        cv = cfill.get(key, float("nan"))
        delta = cv - bv if not (math.isnan(bv) or math.isnan(cv)) else float("nan")
        fill_rows.append({"metric": key, "base": bv, "candidate": cv, "delta": delta})
    fills_df = pd.DataFrame(fill_rows)

    return {
        "summary_metrics": metrics_df,
        "config_diff": config_df,
        "markout_by_horizon": markouts_df,
        "fill_metrics": fills_df,
    }


def _choose_score_metric(df: pd.DataFrame, requested: str = "", excluded: Optional[set[str]] = None) -> Optional[str]:
    excluded = excluded or set()
    if requested and requested in df.columns:
        return requested
    for key in (
        "markout_10s_total_usd",
        "markout_10s_avg_cents",
        "markout_1s_total_usd",
        "fills",
        "fill_contracts",
    ):
        if key in df.columns and key not in excluded:
            return key
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in excluded]
    return numeric[0] if numeric else None


def compare_sweep_runs(base: RunArtifacts, cand: RunArtifacts, score_metric: str = "") -> dict[str, pd.DataFrame]:
    if base.sweep_df.empty or cand.sweep_df.empty:
        return {"best_rows": pd.DataFrame(), "common_grid_delta": pd.DataFrame()}

    bdf = base.sweep_df.copy()
    cdf = cand.sweep_df.copy()
    grid_keys = []
    b_grid = base.sweep_meta.get("grid") if isinstance(base.sweep_meta, dict) else {}
    c_grid = cand.sweep_meta.get("grid") if isinstance(cand.sweep_meta, dict) else {}
    if isinstance(b_grid, dict) and isinstance(c_grid, dict):
        grid_keys = sorted(set(b_grid.keys()) & set(c_grid.keys()))
    if not grid_keys:
        grid_keys = [c for c in bdf.columns if c in cdf.columns and c not in {"tickers"}]
    score = _choose_score_metric(bdf, requested=score_metric, excluded=set(grid_keys)) or _choose_score_metric(
        cdf, requested=score_metric, excluded=set(grid_keys)
    )
    if score is None:
        return {"best_rows": pd.DataFrame(), "common_grid_delta": pd.DataFrame()}

    if score not in bdf.columns:
        bdf[score] = float("nan")
    if score not in cdf.columns:
        cdf[score] = float("nan")

    b_best = bdf.sort_values(score, ascending=False).head(1).copy()
    c_best = cdf.sort_values(score, ascending=False).head(1).copy()
    if not b_best.empty:
        b_best.insert(0, "run_id", base.run_id)
    if not c_best.empty:
        c_best.insert(0, "run_id", cand.run_id)
    best_rows = pd.concat([b_best, c_best], ignore_index=True)

    if grid_keys:
        merged = bdf.merge(cdf, on=grid_keys, how="inner", suffixes=("_base", "_candidate"))
        if f"{score}_base" in merged.columns and f"{score}_candidate" in merged.columns:
            merged["score_delta"] = merged[f"{score}_candidate"] - merged[f"{score}_base"]
            merged = merged.sort_values("score_delta", ascending=False)
        common_grid_delta = merged
    else:
        common_grid_delta = pd.DataFrame()

    return {
        "best_rows": best_rows,
        "common_grid_delta": common_grid_delta,
    }


def _print_section(title: str, df: pd.DataFrame, max_rows: int = 20):
    print(f"\n## {title}")
    if df.empty:
        print("(no rows)")
        return
    show = df.head(max_rows).copy()
    for col in show.columns:
        if pd.api.types.is_float_dtype(show[col]):
            show[col] = show[col].map(lambda x: f"{x:.6g}" if pd.notna(x) else "NaN")
    print(show.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Compare persisted research replay/sweep runs.")
    parser.add_argument("--run-root", default="research_runs")
    parser.add_argument("--base-run", required=True, help="Base run id.")
    parser.add_argument("--candidate-run", required=True, help="Candidate run id.")
    parser.add_argument("--score", default="", help="Score metric for sweep comparisons.")
    parser.add_argument("--max-rows", type=int, default=20)
    parser.add_argument("--out-csv", default="", help="Optional CSV path for primary delta table.")
    args = parser.parse_args()

    base = load_run(args.run_root, args.base_run)
    cand = load_run(args.run_root, args.candidate_run)

    if base.run_type != cand.run_type:
        raise ValueError(f"Run type mismatch: base={base.run_type}, candidate={cand.run_type}")
    if base.run_type not in {"replay", "sweep"}:
        raise ValueError(f"Unsupported run type: {base.run_type}")

    print(f"base_run={base.run_id} candidate_run={cand.run_id} type={base.run_type}")

    if base.run_type == "replay":
        sections = compare_replay_runs(base, cand)
        _print_section("Summary Metric Delta", sections["summary_metrics"], max_rows=args.max_rows)
        _print_section("Fill Metric Delta", sections["fill_metrics"], max_rows=args.max_rows)
        _print_section("Markout By Horizon", sections["markout_by_horizon"], max_rows=args.max_rows)
        _print_section("Config Differences", sections["config_diff"], max_rows=args.max_rows)
        if args.out_csv:
            sections["summary_metrics"].to_csv(args.out_csv, index=False)
    else:
        sections = compare_sweep_runs(base, cand, score_metric=args.score)
        _print_section("Best Rows", sections["best_rows"], max_rows=args.max_rows)
        _print_section("Common Grid Delta", sections["common_grid_delta"], max_rows=args.max_rows)
        if args.out_csv:
            sections["common_grid_delta"].to_csv(args.out_csv, index=False)


if __name__ == "__main__":
    main()
