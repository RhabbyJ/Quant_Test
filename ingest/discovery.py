import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Optional

import requests


@dataclass
class DiscoveredMarket:
    ticker: str
    close_ts_ms: int
    strike: float
    activity_score: float


@dataclass
class DiscoveryResult:
    tickers: List[str]
    close_ts_ms: Optional[int]
    reference_spot: Optional[float]
    selected: List[DiscoveredMarket]
    total_candidates: int


def _parse_time_to_ms(raw: Any) -> int:
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        val = int(raw)
        if val < 10_000_000_000:
            return val * 1000
        return val
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return 0
        if s.isdigit():
            val = int(s)
            if val < 10_000_000_000:
                return val * 1000
            return val
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return 0
    return 0


def _to_float(raw: Any) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return 0.0
        try:
            return float(s)
        except ValueError:
            return 0.0
    return 0.0


def _parse_strike_from_market(market: Dict[str, Any]) -> Optional[float]:
    floor_strike = market.get("floor_strike")
    cap_strike = market.get("cap_strike")
    strike = market.get("strike")
    strike_type = str(market.get("strike_type") or "").lower()

    if floor_strike is not None and cap_strike is not None:
        floor_v = _to_float(floor_strike)
        cap_v = _to_float(cap_strike)
        if floor_v > 0 and cap_v > 0:
            return (floor_v + cap_v) / 2.0

    if strike is not None:
        strike_v = _to_float(strike)
        if strike_v > 0:
            return strike_v

    if floor_strike is not None:
        floor_v = _to_float(floor_strike)
        if floor_v > 0:
            return floor_v
    if cap_strike is not None:
        cap_v = _to_float(cap_strike)
        if cap_v > 0:
            return cap_v

    # Fallback parse from ticker suffix for KXBTC styles: ...-B70125 / ...-T75999.99
    ticker = str(market.get("ticker") or market.get("market_ticker") or "")
    m = re.search(r"(?:-|^)([TB])(\d+(?:\.\d+)?)$", ticker)
    if m:
        try:
            return float(m.group(2))
        except ValueError:
            return None

    # In threshold-style markets strike may be encoded separately.
    if strike_type in {"above", "below", "between"}:
        return None
    return None


def _market_activity_score(market: Dict[str, Any]) -> float:
    # Lightweight proxy: favor liquidity/open_interest/volume, then price updates.
    liquidity = _to_float(market.get("liquidity")) + _to_float(market.get("liquidity_dollars"))
    open_interest = _to_float(market.get("open_interest")) + _to_float(market.get("open_interest_fp"))
    volume = _to_float(market.get("volume")) + _to_float(market.get("volume_fp"))
    last_price = _to_float(market.get("last_price")) + _to_float(market.get("last_price_dollars"))
    return (2.0 * liquidity) + open_interest + volume + (0.1 * last_price)


def _fetch_open_markets(
    rest_base_url: str,
    series_ticker: str,
    max_pages: int,
    timeout_sec: int = 15,
) -> List[Dict[str, Any]]:
    all_markets: List[Dict[str, Any]] = []
    cursor = None

    for _ in range(max_pages):
        params: Dict[str, Any] = {
            "status": "open",
            "series_ticker": series_ticker,
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(f"{rest_base_url}/trade-api/v2/markets", params=params, timeout=timeout_sec)
        if resp.status_code != 200:
            break

        payload = resp.json()
        markets = payload.get("markets", [])
        all_markets.extend(markets)

        cursor = payload.get("cursor")
        if not cursor:
            break

    return all_markets


def discover_tickers(
    rest_base_url: str,
    series_tickers: Optional[List[str]] = None,
    desired_count: int = 3,
    min_close_min: int = 30,
    max_close_min: int = 180,
    min_runway_sec: int = 0,
    reference_spot: Optional[float] = None,
    max_pages: int = 6,
    # Legacy single-series param for backward compat
    series_ticker: Optional[str] = None,
) -> DiscoveryResult:
    """Multi-family discovery with runway constraint.

    Args:
        series_tickers: List of product families to search, e.g. ["KXBTC", "KXETH"].
        min_runway_sec: Minimum TTE in seconds. Overrides min_close_min if larger.
        Other args: same as before.
    """
    # Resolve family list
    families = series_tickers or ([series_ticker] if series_ticker else ["KXBTC"])

    # Enforce runway constraint as a floor on min_close_min
    if min_runway_sec > 0:
        runway_min = int(math.ceil(min_runway_sec / 60.0))
        min_close_min = max(min_close_min, runway_min)

    now_ms = int(time.time() * 1000)
    min_close_ms = now_ms + (min_close_min * 60 * 1000)
    max_close_ms = now_ms + (max_close_min * 60 * 1000)

    # Fetch across all product families
    all_markets: List[Dict[str, Any]] = []
    for family in families:
        try:
            all_markets.extend(_fetch_open_markets(rest_base_url, family, max_pages=max_pages))
        except Exception:
            pass  # Skip unreachable families

    candidates: List[DiscoveredMarket] = []

    for market in all_markets:
        ticker = str(market.get("ticker") or market.get("market_ticker") or "")
        if not ticker:
            continue

        close_ts = _parse_time_to_ms(market.get("close_time") or market.get("close_ts"))
        if close_ts <= 0:
            continue
        if close_ts < min_close_ms or close_ts > max_close_ms:
            continue

        strike = _parse_strike_from_market(market)
        if strike is None or strike <= 0:
            continue

        status = str(market.get("status") or "").lower()
        if status and status not in {"open", "active"}:
            continue

        candidates.append(
            DiscoveredMarket(
                ticker=ticker,
                close_ts_ms=close_ts,
                strike=strike,
                activity_score=_market_activity_score(market),
            )
        )

    if not candidates:
        return DiscoveryResult([], None, reference_spot, [], 0)

    by_close: Dict[int, List[DiscoveredMarket]] = {}
    for c in candidates:
        by_close.setdefault(c.close_ts_ms, []).append(c)

    # Rank expiry ladders by total activity, then by soonest close.
    ranked_closes = sorted(
        by_close.items(),
        key=lambda kv: (-sum(m.activity_score for m in kv[1]), kv[0]),
    )
    chosen_close_ts, ladder = ranked_closes[0]

    if reference_spot is None or reference_spot <= 0:
        reference_spot = median([m.strike for m in ladder])

    def moneyness_distance(m: DiscoveredMarket) -> float:
        if reference_spot and reference_spot > 0:
            return abs(math.log(m.strike / reference_spot))
        return abs(m.strike - median([x.strike for x in ladder]))

    selected = sorted(ladder, key=moneyness_distance)[: max(1, desired_count)]
    tickers = [m.ticker for m in selected]

    return DiscoveryResult(
        tickers=tickers,
        close_ts_ms=chosen_close_ts,
        reference_spot=reference_spot,
        selected=selected,
        total_candidates=len(candidates),
    )


# Backward-compatible alias
def discover_kxbtc_tickers(
    rest_base_url: str,
    series_ticker: str = "KXBTC",
    desired_count: int = 3,
    min_close_min: int = 30,
    max_close_min: int = 180,
    reference_spot: Optional[float] = None,
    max_pages: int = 6,
) -> DiscoveryResult:
    return discover_tickers(
        rest_base_url=rest_base_url,
        series_tickers=[series_ticker],
        desired_count=desired_count,
        min_close_min=min_close_min,
        max_close_min=max_close_min,
        reference_spot=reference_spot,
        max_pages=max_pages,
    )
