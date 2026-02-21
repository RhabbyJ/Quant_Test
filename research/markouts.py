from bisect import bisect_left
from typing import Iterable

import pandas as pd


def normalize_side(value) -> str:
    if value is None:
        return "unknown"
    s = str(value).strip().lower()
    if "yes_bid" in s or s == "yes":
        return "yes_bid"
    if "no_bid" in s or s == "no":
        return "no_bid"
    return s


def build_mid_series_from_deltas(ob_df: pd.DataFrame) -> dict[str, tuple[list[int], list[float]]]:
    if ob_df.empty:
        return {}
    required = {"ticker", "exchange_ts", "side", "price_cents", "size"}
    if not required.issubset(set(ob_df.columns)):
        return {}

    deltas = ob_df[list(required)].copy()
    deltas["exchange_ts"] = pd.to_numeric(deltas["exchange_ts"], errors="coerce")
    deltas["price_cents"] = pd.to_numeric(deltas["price_cents"], errors="coerce")
    deltas["size"] = pd.to_numeric(deltas["size"], errors="coerce")
    deltas["side"] = deltas["side"].map(normalize_side)
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


def _nearest_idx_within_tolerance(ts_values: list[int], target_ts: int, tolerance_ms: int) -> int | None:
    if not ts_values:
        return None
    idx = bisect_left(ts_values, target_ts)
    candidates = []
    if 0 <= idx < len(ts_values):
        candidates.append(idx)
    if idx - 1 >= 0:
        candidates.append(idx - 1)
    if not candidates:
        return None

    best_idx = min(candidates, key=lambda i: abs(ts_values[i] - target_ts))
    if abs(ts_values[best_idx] - target_ts) > tolerance_ms:
        return None
    return best_idx


def compute_fill_markouts(
    fill_df: pd.DataFrame,
    ob_df: pd.DataFrame,
    horizons_sec: Iterable[int] = (1, 10, 60),
    tolerance_ms_by_horizon: dict[int, int] | None = None,
) -> pd.DataFrame:
    if fill_df.empty:
        return pd.DataFrame()
    required = {"ticker", "exchange_ts", "price_cents", "size"}
    if not required.issubset(set(fill_df.columns)):
        return pd.DataFrame()

    mids = build_mid_series_from_deltas(ob_df)
    if not mids:
        return pd.DataFrame()

    tol = tolerance_ms_by_horizon or {1: 500, 10: 2000, 60: 5000}

    fills = fill_df.copy()
    fills["exchange_ts"] = pd.to_numeric(fills["exchange_ts"], errors="coerce")
    fills["price_cents"] = pd.to_numeric(fills["price_cents"], errors="coerce")
    fills["size"] = pd.to_numeric(fills["size"], errors="coerce")
    fills = fills.dropna(subset=["exchange_ts", "price_cents", "size"])
    if fills.empty:
        return pd.DataFrame()

    if "side" in fills.columns:
        fills["side_norm"] = fills["side"].map(normalize_side)
    elif "is_bid" in fills.columns:
        fills["side_norm"] = fills["is_bid"].map(lambda b: "yes_bid" if bool(b) else "no_bid")
    else:
        fills["side_norm"] = "unknown"

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
            target_ts = fill_ts + (int(h) * 1000)
            tolerance_ms = int(tol.get(int(h), 1000))
            mid_idx = _nearest_idx_within_tolerance(ts_list, target_ts, tolerance_ms)
            if mid_idx is None:
                continue

            mid_after = float(mid_list[mid_idx])
            mid_sample_ts = int(ts_list[mid_idx])
            markout_cents = sign * (mid_after - fill_yes_price)
            rows.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "horizon_s": int(h),
                    "fill_ts": fill_ts,
                    "mid_sample_ts": mid_sample_ts,
                    "mid_sample_dt_ms": abs(mid_sample_ts - target_ts),
                    "markout_cents": markout_cents,
                    "size": size,
                    "markout_usd": (markout_cents * size) / 100.0,
                }
            )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

