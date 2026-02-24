import asyncio
import time
import logging
import uuid
import re
import math
import random
from collections import defaultdict, deque
from typing import Any, Dict, List
from dataclasses import dataclass

from core.types import (
    LifecycleEvent,
    EdgeMetricEvent,
    MarketMetadataEvent,
    OrderbookDeltaEvent,
    OrderbookSnapshotEvent,
    PaperFillEvent,
    QuoteAuditEvent,
    Side,
    SpotEvent,
    TradeEvent,
)
from data.store import BufferedParquetWriter
from engine.orderbook import Orderbook
from engine.quant import QuantEngine
from engine.quoter import Quoter
from engine.risk import RiskEngine

@dataclass
class VirtualOrder:
    order_id: str
    ticker: str
    side: Side
    price_cents: int
    size: int
    queue_ahead: int
    placed_at_ts: int
    fair_prob_at_quote: float
    sigma_at_quote: float
    tte_ms_at_quote: int
    half_spread_cents_at_quote: int = 0
    top_depth_at_quote: int = 0
    active_from_ts: int = 0
    cancel_requested_ts: int | None = None
    cancel_effective_ts: int | None = None

@dataclass
class ContractSpec:
    ticker: str
    direction: str
    strike_low: float | None
    strike_high: float | None
    expiration_ts: int
    settlement_window: str | None
    oracle_risk_score: float | None = None
    oracle_blocked: bool = False
    oracle_reason: str | None = None

class EngineLoop:
    """
    Guardrail #4: Deterministic simulator event ordering.
    A single deterministic "engine loop" consumes events from the isolated queues.
    Ordering rule: Apply exchange events -> Resolve fills -> Apply cancels/amends -> Generate new quotes
    """
    def __init__(self, data_store: BufferedParquetWriter, tickers: list[str] | None = None):
        self.event_queue = asyncio.Queue()
        self.data_store = data_store
        
        # Unified state
        self.orderbooks: Dict[str, Orderbook] = {}
        self.quant = QuantEngine()
        self.quoter = Quoter(max_inventory=1000)
        self.risk = RiskEngine()
        
        # Paper trading ledger
        self.active_orders: Dict[str, VirtualOrder] = {}
        self.is_running = False
        self.ticker_close_ts: Dict[str, int] = {}
        self.min_quote_tte_ms = 3 * 60 * 1000
        self.active_tickers = set()
        self.recent_trade_ts = defaultdict(deque)
        self.trade_history_ms = 10 * 60 * 1000
        self.last_book_update_ts: Dict[str, int] = {}
        self.mid_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5000))
        self.ticker_direction: Dict[str, str] = {}
        self.contract_specs: Dict[str, ContractSpec] = {}
        self.oracle_blocked: Dict[str, bool] = {}
        self.oracle_reason: Dict[str, str] = {}
        self._logged_contract_specs = set()
        self._logged_oracle_blocks = set()
        self.pending_edge_evals = deque()
        self.completed_edges = deque(maxlen=1000)
        self.edge_horizon_ms = 10_000
        self.edge_tolerance_ms = 2_000
        self.edge_slippage_buffer_cents = 0.20
        self.edge_governor_window = 30
        self.edge_governor_min_fills = 50
        self.edge_governor_cooldown_ms = 5_000
        self.edge_governor_last_adjust_ts = 0
        self.edge_governor_bootstrap_samples = 300
        self.edge_governor_tighten_lcb_cents = 0.20
        self.edge_governor_widen_ucb_cents = 0.0
        self.edge_governor_hysteresis_cents = 0.05
        self.governor_toxicity_spike = 1.0
        self.governor_toxicity_low = 0.25
        self.governor_step_up = 0.5
        self.governor_step_down = 0.2
        self.governor_positive_streak = 0
        self.governor_negative_streak = 0
        self.governor_size_down_streak = 2
        self.governor_size_up_streak = 4
        self.governor_spread_mult_min = 2.0
        self.governor_spread_mult_max = 20.0
        self.base_quote_size = 10
        self.quote_size = 10
        self.min_quote_size = 1
        self.max_quote_size = 25

        # Paper-fill realism mode (execution friction simulation).
        self.realism_mode = "base"  # off | base | pessimistic
        self.realism_order_latency_ms = 100
        self.realism_cancel_lag_ms = 100
        self.realism_queue_haircut = 0.80

        self.market_min_trades_per_hour = 0
        self.market_min_top_depth = 5
        self.market_max_book_age_ms = 20_000
        self.exclude_last_minutes = 3

        # KPI locking and ranking controls.
        self.profit_kpi_min_fills = 50
        self.market_rank_enabled = False
        self.market_rank_interval_ms = 60 * 60 * 1000
        self.market_rank_window_ms = 60 * 60 * 1000
        self.market_rank_min_fills = 20
        self.market_rank_top_n = 3
        self.market_rank_toxicity_weight = 5.0
        self.market_rank_ci_weight = 1.0
        self._last_rank_ts = 0
        self._ranked_tickers: set[str] | None = None
        self.current_ts = 0
        
        # Ticker -> strike mapping for fair value ordering.
        # Supports legacy BTC-...-Txxxxx and new KXBTC-...-(B|T)xxxxx[.yy] styles.
        self.ticker_to_strike = {}
        seed_tickers = tickers or [
            "BTC-24DEC31-T100000",
            "BTC-24DEC31-T110000",
            "BTC-24DEC31-T120000",
        ]
        self.set_active_tickers(seed_tickers)

    @staticmethod
    def _parse_contract_from_ticker(ticker: str) -> tuple[float | None, str]:
        # Match suffixes like T100000, T75999.99, B70125.
        m = re.search(r"(?:-|^)([TB])(\d+(?:\.\d+)?)$", ticker)
        if not m:
            return None, "above"
        try:
            strike = float(m.group(2))
            direction = "below" if m.group(1) == "B" else "above"
            return strike, direction
        except ValueError:
            return None, "above"

    def set_active_tickers(self, tickers: list[str]):
        normalized = [t for t in tickers if t]
        new_set = set(normalized)
        removed = self.active_tickers - new_set
        self.active_tickers = new_set
        self._ranked_tickers = None

        for ticker in removed:
            self._cancel_orders_for_ticker(ticker, force_immediate=True)
            self.orderbooks.pop(ticker, None)
            self.ticker_close_ts.pop(ticker, None)
            self.ticker_to_strike.pop(ticker, None)
            self.ticker_direction.pop(ticker, None)
            self.contract_specs.pop(ticker, None)
            self.oracle_blocked.pop(ticker, None)
            self.oracle_reason.pop(ticker, None)
            self._logged_oracle_blocks.discard(ticker)
            self.last_book_update_ts.pop(ticker, None)
            self.mid_history.pop(ticker, None)
            self.quoter.inventory.pop(ticker, None)
            self.quoter.last_quotes.pop(ticker, None)
            self.quoter.last_quote_time.pop(ticker, None)
            self.risk.inventory.pop(ticker, None)
            self.recent_trade_ts.pop(ticker, None)

        for ticker in normalized:
            strike, direction = self._parse_contract_from_ticker(ticker)
            if strike is not None:
                self.ticker_to_strike[ticker] = strike
            self.ticker_direction[ticker] = direction

    def recent_trade_count(self, window_ms: int, current_ts: int, tickers: list[str] | None = None) -> int:
        if window_ms <= 0:
            return 0
        selected = tickers if tickers is not None else list(self.active_tickers)
        cutoff = current_ts - window_ms
        total = 0
        for ticker in selected:
            dq = self.recent_trade_ts.get(ticker)
            if not dq:
                continue
            while dq and dq[0] < cutoff:
                dq.popleft()
            total += len(dq)
        return total

    def _record_mid_from_book(self, ticker: str, ts_ms: int):
        ob = self.orderbooks.get(ticker)
        if not ob or not ob.yes_bids or not ob.no_bids:
            return
        bbo = ob.get_yes_bbo()
        if bbo.bid <= 0 or bbo.ask >= 100:
            return
        self.mid_history[ticker].append((int(ts_ms), float(bbo.mid)))
        self.last_book_update_ts[ticker] = int(ts_ms)

    def _realism_params(self) -> tuple[int, int, float]:
        mode = (self.realism_mode or "base").strip().lower()
        if mode == "off":
            return 0, 0, 1.0
        if mode == "pessimistic":
            return max(0, self.realism_order_latency_ms), max(0, self.realism_cancel_lag_ms), min(1.0, max(0.1, self.realism_queue_haircut))
        # base mode defaults
        return max(0, self.realism_order_latency_ms), max(0, self.realism_cancel_lag_ms), min(1.0, max(0.1, self.realism_queue_haircut))

    def _is_order_active_for_trade(self, order: VirtualOrder, trade_ts: int) -> bool:
        if trade_ts < order.active_from_ts:
            return False
        if order.cancel_effective_ts is not None and trade_ts >= order.cancel_effective_ts:
            return False
        return True

    def _cleanup_inactive_orders(self, current_ts: int):
        for order_id, order in list(self.active_orders.items()):
            if order.cancel_effective_ts is not None and current_ts >= order.cancel_effective_ts:
                del self.active_orders[order_id]

    def _nearest_mid(self, ticker: str, target_ts_ms: int, tolerance_ms: int) -> float | None:
        points = self.mid_history.get(ticker)
        if not points:
            return None
        best_dt = None
        best_mid = None
        # Reverse scan is efficient because we mostly query recent fills.
        for ts, mid in reversed(points):
            dt = abs(ts - target_ts_ms)
            if best_dt is None or dt < best_dt:
                best_dt = dt
                best_mid = mid
            if ts < (target_ts_ms - tolerance_ms) and best_dt is not None:
                break
        if best_dt is None or best_dt > tolerance_ms:
            return None
        return float(best_mid)

    def _compute_dynamic_slippage_buffer_cents(
        self,
        half_spread_cents: int,
        top_depth_at_quote: int,
        sigma_at_quote: float | None,
        tte_ms_at_quote: int | None,
    ) -> float:
        # Conservative execution buffer for paper-edge realism.
        buffer = max(0.10, float(self.edge_slippage_buffer_cents))
        if half_spread_cents >= 4:
            buffer += 0.25
        if half_spread_cents >= 8:
            buffer += 0.25
        if top_depth_at_quote < 10:
            buffer += 0.25
        if top_depth_at_quote < 5:
            buffer += 0.25
        if sigma_at_quote is not None and sigma_at_quote >= 1.0:
            buffer += 0.25
        if sigma_at_quote is not None and sigma_at_quote >= 1.5:
            buffer += 0.25
        if tte_ms_at_quote is not None and tte_ms_at_quote < (15 * 60 * 1000):
            buffer += 0.25
        return max(0.25, min(1.50, buffer))

    def _bootstrap_mean_ci(self, values: list[float], current_ts: int) -> tuple[float, float]:
        if not values:
            return float("nan"), float("nan")
        mean = sum(values) / len(values)
        if len(values) < 2:
            return mean, float("nan")

        rng = random.Random(current_ts)
        means: list[float] = []
        n = len(values)
        for _ in range(max(50, self.edge_governor_bootstrap_samples)):
            sample = [values[rng.randrange(n)] for __ in range(n)]
            means.append(sum(sample) / n)
        means.sort()
        lo_idx = int(0.025 * (len(means) - 1))
        hi_idx = int(0.975 * (len(means) - 1))
        return float(means[lo_idx]), float(means[hi_idx])

    def _edge_window_for_ticker(self, ticker: str, current_ts: int) -> list[dict]:
        cutoff = current_ts - max(1, self.market_rank_window_ms)
        return [e for e in self.completed_edges if e.get("ticker") == ticker and int(e.get("ts", 0)) >= cutoff]

    def _edge_stats(self, edges: list[dict], current_ts: int) -> dict:
        if not edges:
            return {
                "fills": 0,
                "mean_edge": float("nan"),
                "lcb": float("nan"),
                "ucb": float("nan"),
                "ci_width": float("nan"),
                "fills_per_hour": 0.0,
                "avg_fill_size": 0.0,
                "edge_per_hour_cents": float("nan"),
                "toxicity": float("nan"),
            }
        sorted_edges = sorted(edges, key=lambda x: int(x.get("ts", 0)))
        edge_values = [float(e.get("fee_adjusted_edge_cents", 0.0)) for e in sorted_edges]
        lcb, ucb = self._bootstrap_mean_ci(edge_values, current_ts=current_ts)
        mean_edge = sum(edge_values) / len(edge_values)
        first_ts = int(sorted_edges[0].get("ts", current_ts))
        last_ts = int(sorted_edges[-1].get("ts", current_ts))
        span_ms = max(float(self.market_rank_window_ms), float(last_ts - first_ts), 1.0)
        fills_per_hour = len(sorted_edges) * (3600000.0 / span_ms)
        avg_fill_size = sum(float(e.get("fill_size", 0.0)) for e in sorted_edges) / len(sorted_edges)
        edge_per_hour_cents = mean_edge * fills_per_hour * avg_fill_size
        signed_markout_mean = sum(float(e.get("signed_markout_cents", 0.0)) for e in sorted_edges) / len(sorted_edges)
        toxicity = (-signed_markout_mean) * (fills_per_hour / 60.0)
        return {
            "fills": len(sorted_edges),
            "mean_edge": mean_edge,
            "lcb": lcb,
            "ucb": ucb,
            "ci_width": (ucb - lcb) if (not math.isnan(lcb) and not math.isnan(ucb)) else float("nan"),
            "fills_per_hour": fills_per_hour,
            "avg_fill_size": avg_fill_size,
            "edge_per_hour_cents": edge_per_hour_cents,
            "toxicity": toxicity,
        }

    def _is_profit_gate_pass(self, stats: dict) -> bool:
        if int(stats.get("fills", 0)) < self.profit_kpi_min_fills:
            return False
        lcb = float(stats.get("lcb", float("nan")))
        return (not math.isnan(lcb)) and lcb > 0.0

    def _rank_eligible_tickers(self, candidate_tickers: list[str], current_ts: int) -> list[str]:
        if not self.market_rank_enabled:
            return candidate_tickers
        if not candidate_tickers:
            return candidate_tickers
        if (
            self._ranked_tickers is not None
            and (current_ts - self._last_rank_ts) < self.market_rank_interval_ms
        ):
            filtered = [t for t in candidate_tickers if t in self._ranked_tickers]
            return filtered if filtered else candidate_tickers

        rows = []
        for ticker in candidate_tickers:
            stats = self._edge_stats(self._edge_window_for_ticker(ticker, current_ts), current_ts=current_ts)
            if int(stats["fills"]) < self.market_rank_min_fills:
                score = float("-inf")
            else:
                toxicity = float(stats["toxicity"]) if not math.isnan(float(stats["toxicity"])) else 0.0
                ci_width = float(stats["ci_width"]) if not math.isnan(float(stats["ci_width"])) else 0.0
                score = float(stats["edge_per_hour_cents"]) - (self.market_rank_toxicity_weight * max(0.0, toxicity)) - (self.market_rank_ci_weight * ci_width)
            rows.append((ticker, score, stats))

        ranked = sorted(rows, key=lambda x: x[1], reverse=True)
        finite_ranked = [r for r in ranked if math.isfinite(r[1])]
        if finite_ranked:
            top_n = max(1, min(self.market_rank_top_n, len(finite_ranked)))
            selected = [r[0] for r in finite_ranked[:top_n]]
        else:
            selected = candidate_tickers

        self._ranked_tickers = set(selected)
        self._last_rank_ts = current_ts
        logging.info(
            "Market ranking selected=%s details=%s",
            selected,
            [
                {
                    "ticker": t,
                    "score": round(s, 3) if math.isfinite(s) else None,
                    "fills": int(st["fills"]),
                    "edge_per_hour_cents": round(float(st["edge_per_hour_cents"]), 3)
                    if not math.isnan(float(st["edge_per_hour_cents"]))
                    else None,
                }
                for (t, s, st) in ranked
            ],
        )
        return [t for t in candidate_tickers if t in self._ranked_tickers] or candidate_tickers

    async def _finalize_fee_adjusted_edges(self, current_ts: int):
        if not self.pending_edge_evals:
            return

        while self.pending_edge_evals:
            head = self.pending_edge_evals[0]
            if current_ts < (head["target_ts"] - self.edge_tolerance_ms):
                break
            pending = self.pending_edge_evals.popleft()
            mid_after = self._nearest_mid(
                ticker=pending["ticker"],
                target_ts_ms=pending["target_ts"],
                tolerance_ms=self.edge_tolerance_ms,
            )
            if mid_after is None:
                # No reliable book sample near target horizon.
                continue

            signed_markout = pending["sign"] * (mid_after - pending["fill_yes_price"])
            dynamic_slippage_buffer = self._compute_dynamic_slippage_buffer_cents(
                half_spread_cents=int(pending.get("half_spread_cents", 0)),
                top_depth_at_quote=int(pending.get("top_depth_at_quote", 0)),
                sigma_at_quote=pending.get("sigma_at_quote"),
                tte_ms_at_quote=pending.get("tte_ms_at_quote"),
            )
            fee_adjusted_edge = (
                signed_markout
                - pending["maker_fee_cents_per_contract"]
                - dynamic_slippage_buffer
            )
            edge = {
                "ts": int(current_ts),
                "ticker": pending["ticker"],
                "side": pending["side"],
                "signed_markout_cents": float(signed_markout),
                "fee_adjusted_edge_cents": float(fee_adjusted_edge),
                "fill_size": int(pending.get("fill_size", 0)),
                "sigma_at_quote": pending.get("sigma_at_quote"),
                "tte_ms_at_quote": pending.get("tte_ms_at_quote"),
            }
            self.completed_edges.append(edge)

            event = EdgeMetricEvent(
                exchange_ts=int(current_ts),
                ingest_ts=int(time.time() * 1000),
                ticker=pending["ticker"],
                side=pending["side"],
                horizon_s=max(1, int(self.edge_horizon_ms / 1000)),
                fill_ts=int(pending["fill_ts"]),
                fill_size=int(pending.get("fill_size", 0)),
                fill_yes_price=float(pending["fill_yes_price"]),
                mid_yes_after=float(mid_after),
                signed_markout_cents=float(signed_markout),
                maker_fee_cents_per_contract=float(pending["maker_fee_cents_per_contract"]),
                slippage_buffer_cents=float(dynamic_slippage_buffer),
                fee_adjusted_edge_cents=float(fee_adjusted_edge),
                sigma_at_quote=pending.get("sigma_at_quote"),
                tte_ms_at_quote=pending.get("tte_ms_at_quote"),
            )
            await self.data_store.ingest_event("edge_metric", event)

    def _recent_edges(self) -> list[dict]:
        if not self.completed_edges:
            return []
        n = max(1, self.edge_governor_window)
        return list(self.completed_edges)[-n:]

    def _apply_spread_governor(self, current_ts: int):
        if (current_ts - self.edge_governor_last_adjust_ts) < self.edge_governor_cooldown_ms:
            return

        window = self._recent_edges()
        required_fills = max(5, self.edge_governor_min_fills, self.profit_kpi_min_fills)
        if len(window) < required_fills:
            return

        edges = [float(x["fee_adjusted_edge_cents"]) for x in window]
        markouts = [float(x["signed_markout_cents"]) for x in window]
        lcb, ucb = self._bootstrap_mean_ci(edges, current_ts=current_ts)
        mean_edge = sum(edges) / len(edges)

        mean_signed_markout = sum(markouts) / len(markouts)
        window_ms = max(1, window[-1]["ts"] - window[0]["ts"])
        fill_rate_per_min = len(window) / (window_ms / 60000.0)
        toxicity = (-mean_signed_markout) * fill_rate_per_min

        old_mult = self.quoter.vol_spread_mult
        old_size = self.quote_size

        # Hysteresis thresholds prevent ping-pong around breakeven.
        tighten_lcb = self.edge_governor_tighten_lcb_cents + self.edge_governor_hysteresis_cents
        widen_ucb = self.edge_governor_widen_ucb_cents - self.edge_governor_hysteresis_cents
        action = None

        if toxicity > self.governor_toxicity_spike:
            self.quoter.vol_spread_mult = min(self.governor_spread_mult_max, self.quoter.vol_spread_mult + (2.0 * self.governor_step_up))
            self.quote_size = max(self.min_quote_size, self.quote_size - 1)
            self.governor_negative_streak += 1
            self.governor_positive_streak = 0
            action = "toxicity_spike_widen_and_size_down"
        elif ucb < widen_ucb:
            self.governor_negative_streak += 1
            self.governor_positive_streak = 0
            self.quoter.vol_spread_mult = min(self.governor_spread_mult_max, self.quoter.vol_spread_mult + self.governor_step_up)
            action = "negative_edge_widen"
            # Size is secondary knob: only cut after persistent negative regimes.
            if self.governor_negative_streak >= self.governor_size_down_streak:
                self.quote_size = max(self.min_quote_size, self.quote_size - 1)
                action = "negative_edge_widen_then_size_down"
        elif lcb > tighten_lcb and toxicity < self.governor_toxicity_low:
            self.governor_positive_streak += 1
            self.governor_negative_streak = 0
            if self.governor_positive_streak >= 2:
                self.quoter.vol_spread_mult = max(self.governor_spread_mult_min, self.quoter.vol_spread_mult - self.governor_step_down)
                action = "positive_edge_tighten"
            # Increase size only after sustained positive edge and low toxicity.
            if self.governor_positive_streak >= self.governor_size_up_streak:
                self.quote_size = min(self.max_quote_size, self.quote_size + 1)
                action = "positive_edge_tighten_then_size_up"
        else:
            self.governor_negative_streak = max(0, self.governor_negative_streak - 1)
            self.governor_positive_streak = max(0, self.governor_positive_streak - 1)
            return

        self.edge_governor_last_adjust_ts = current_ts
        if old_mult != self.quoter.vol_spread_mult or old_size != self.quote_size:
            logging.info(
                "Spread governor action=%s mean_edge=%.3fc ci=[%.3f, %.3f] toxicity=%.3f spread_mult %.2f->%.2f quote_size %s->%s",
                action,
                mean_edge,
                lcb,
                ucb,
                toxicity,
                old_mult,
                self.quoter.vol_spread_mult,
                old_size,
                self.quote_size,
            )

    def _passes_market_eligibility(self, ticker: str, current_ts: int) -> bool:
        # Enforce depth/liquidity/freshness filters before quoting.
        if self.oracle_blocked.get(ticker, False):
            if ticker not in self._logged_oracle_blocks:
                self._logged_oracle_blocks.add(ticker)
                logging.warning("Market blocked by oracle-risk filter ticker=%s reason=%s", ticker, self.oracle_reason.get(ticker, ""))
            return False
        ob = self.orderbooks.get(ticker)
        if not ob:
            return False
        last_book_ts = self.last_book_update_ts.get(ticker, 0)
        if last_book_ts <= 0 or (current_ts - last_book_ts) > self.market_max_book_age_ms:
            return False
        # if not ob.yes_bids or not ob.no_bids:
        #     return False
        best_yes = max(ob.yes_bids.keys()) if ob.yes_bids else 0
        best_no = max(ob.no_bids.keys()) if ob.no_bids else 0
        top_depth = min(ob.yes_bids.get(best_yes, 0), ob.no_bids.get(best_no, 0))
        if top_depth < self.market_min_top_depth:
            return False

        trades_last_hour = self.recent_trade_count(
            window_ms=60 * 60 * 1000,
            current_ts=current_ts,
            tickers=[ticker],
        )
        if trades_last_hour < self.market_min_trades_per_hour:
            return False
        return True
        
    async def start(self):
        self.is_running = True
        logging.info("Starting deterministic engine loop")
        
        while self.is_running:
            try:
                batch = []
                event = await self.event_queue.get()
                batch.append(event)
                
                while not self.event_queue.empty():
                    batch.append(self.event_queue.get_nowait())
                    
                await self._apply_exchange_events(batch)
                await self._resolve_paper_fills(batch)
                
                # Ensure time only moves forward even if feeds have clock skew
                self.current_ts = max(self.current_ts, batch[-1].exchange_ts)
                await self._run_strategy_tick(self.current_ts)
                
                for _ in batch:
                    self.event_queue.task_done()
                    
            except Exception as e:
                logging.error(f"Error in engine loop: {e}", exc_info=True)
                
    async def _apply_exchange_events(self, batch: list[Any]):
        """Update L2 Books, Volatility Estimates, and Write to Storage"""
        for event in batch:
            if isinstance(event, OrderbookSnapshotEvent):
                # Don't persist snapshot blobs to parquet directly in this MVP to save complexity,
                # just initialize the local L2 book so delta updates have a base.
                ob = self.orderbooks.setdefault(event.ticker, Orderbook(event.ticker))
                ob.apply_snapshot(event.yes_bids, event.no_bids)
                self.risk.last_kalshi_ts = event.exchange_ts
                self.last_book_update_ts[event.ticker] = event.exchange_ts
                self._record_mid_from_book(event.ticker, event.exchange_ts)
                logging.info(f"[{event.ticker}] Applied REST Snapshot")
                if event.ticker not in self.ticker_to_strike:
                    strike, direction = self._parse_contract_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                    self.ticker_direction[event.ticker] = direction
                
            elif isinstance(event, OrderbookDeltaEvent):
                await self.data_store.ingest_event("orderbook_delta", event)
                ob = self.orderbooks.setdefault(event.ticker, Orderbook(event.ticker))
                ob.apply_delta(event)
                self.risk.last_kalshi_ts = event.exchange_ts
                self.last_book_update_ts[event.ticker] = event.exchange_ts
                self._record_mid_from_book(event.ticker, event.exchange_ts)
                if event.ticker not in self.ticker_to_strike:
                    strike, direction = self._parse_contract_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                    self.ticker_direction[event.ticker] = direction
                
            elif isinstance(event, TradeEvent):
                await self.data_store.ingest_event("trade", event)
                self.risk.last_kalshi_ts = event.exchange_ts
                if event.ticker in self.active_tickers:
                    dq = self.recent_trade_ts[event.ticker]
                    dq.append(event.exchange_ts)
                    trim_before = event.exchange_ts - self.trade_history_ms
                    while dq and dq[0] < trim_before:
                        dq.popleft()
                if event.ticker not in self.ticker_to_strike:
                    strike, direction = self._parse_contract_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                    self.ticker_direction[event.ticker] = direction

            elif isinstance(event, MarketMetadataEvent):
                await self.data_store.ingest_event("market_meta", event)
                self.risk.last_kalshi_ts = event.exchange_ts
                if event.close_ts > 0:
                    self.ticker_close_ts[event.ticker] = event.close_ts
                # Prefer exchange-provided contract fields when available.
                direction = (event.direction or "").lower() if event.direction else None
                if direction not in {"above", "below", "range"}:
                    _, suffix_dir = self._parse_contract_from_ticker(event.ticker)
                    direction = suffix_dir
                self.ticker_direction[event.ticker] = direction

                low = event.strike_low
                high = event.strike_high
                strike_for_model = None
                if low is not None and high is not None:
                    strike_for_model = (float(low) + float(high)) / 2.0
                elif low is not None:
                    strike_for_model = float(low)
                elif high is not None:
                    strike_for_model = float(high)
                if strike_for_model is None:
                    strike, _ = self._parse_contract_from_ticker(event.ticker)
                    strike_for_model = strike
                if strike_for_model is not None:
                    self.ticker_to_strike[event.ticker] = strike_for_model

                spec = ContractSpec(
                    ticker=event.ticker,
                    direction=direction,
                    strike_low=float(low) if low is not None else None,
                    strike_high=float(high) if high is not None else None,
                    expiration_ts=int(event.close_ts),
                    settlement_window=event.settlement_window,
                    oracle_risk_score=event.oracle_risk_score,
                    oracle_blocked=bool(event.oracle_blocked),
                    oracle_reason=event.oracle_reason,
                )
                self.contract_specs[event.ticker] = spec
                self.oracle_blocked[event.ticker] = bool(event.oracle_blocked)
                self.oracle_reason[event.ticker] = event.oracle_reason or ""
                spec_key = (
                    spec.ticker,
                    spec.direction,
                    spec.strike_low,
                    spec.strike_high,
                    spec.expiration_ts,
                    spec.settlement_window,
                    spec.oracle_risk_score,
                    spec.oracle_blocked,
                    spec.oracle_reason,
                )
                if spec_key not in self._logged_contract_specs:
                    self._logged_contract_specs.add(spec_key)
                    logging.info(
                        "CONTRACT SPEC ticker=%s direction=%s strike_low=%s strike_high=%s close_ts=%s settlement=%s oracle_blocked=%s oracle_risk=%.2f reason=%s",
                        spec.ticker,
                        spec.direction,
                        spec.strike_low,
                        spec.strike_high,
                        spec.expiration_ts,
                        spec.settlement_window,
                        spec.oracle_blocked,
                        float(spec.oracle_risk_score or 0.0),
                        spec.oracle_reason,
                    )
                
            elif isinstance(event, SpotEvent):
                await self.data_store.ingest_event("spot", event)
                self.risk.last_spot_ts = event.exchange_ts
                self.quant.update_spot(event.price, event.exchange_ts)
                
            elif isinstance(event, LifecycleEvent):
                await self.data_store.ingest_event("lifecycle", event)
                self.risk.last_kalshi_ts = event.exchange_ts
                if event.status != "open":
                    self.risk.trigger_risk_off(
                        f"Market Lifecycle Closed for {event.ticker}",
                        recoverable=False,
                        current_ts=event.exchange_ts,
                    )

    async def _resolve_paper_fills(self, batch: list[Any]):
        """Burn down queue_ahead using TradeEvents, record fills on YES/NO bid books."""
        trades = [e for e in batch if isinstance(e, TradeEvent)]
        _, _, queue_haircut = self._realism_params()
        
        for trade in trades:
            yes_trade_price = trade.price_cents
            no_trade_price = 100 - trade.price_cents

            for order_id, vorder in list(self.active_orders.items()):
                if vorder.ticker != trade.ticker:
                    continue

                matched = (
                    (vorder.side == Side.YES_BID and vorder.price_cents == yes_trade_price)
                    or (vorder.side == Side.NO_BID and vorder.price_cents == no_trade_price)
                )
                if not matched:
                    continue
                if not self._is_order_active_for_trade(vorder, trade.exchange_ts):
                    continue

                # Conservative simulator model: any public trade at our resting level burns queue_ahead.
                queue_ahead_before_burn = max(0, vorder.queue_ahead)
                effective_trade_size = max(0, int(math.floor(trade.size * queue_haircut)))
                if effective_trade_size <= 0:
                    continue
                vorder.queue_ahead -= effective_trade_size
                
                if vorder.queue_ahead < 0:
                    fill_qty = min(vorder.size, abs(vorder.queue_ahead))
                    time_since_quote_ms = max(0, int(trade.exchange_ts - vorder.placed_at_ts))
                    
                    logging.info(f"PAPER FILL: {vorder.ticker} {vorder.side.value} {fill_qty} @ {vorder.price_cents}c")
                    
                    fill_event = PaperFillEvent(
                        exchange_ts=trade.exchange_ts,
                        ingest_ts=int(time.time() * 1000),
                        ticker=vorder.ticker,
                        price_cents=vorder.price_cents,
                        size=fill_qty,
                        is_bid=(vorder.side == Side.YES_BID),
                        side=vorder.side.value,
                        queue_ahead_at_fill=queue_ahead_before_burn,
                        time_since_quote_ms=time_since_quote_ms,
                        fair_prob_at_quote=vorder.fair_prob_at_quote,
                        sigma_at_quote=vorder.sigma_at_quote,
                        tte_ms_at_quote=vorder.tte_ms_at_quote,
                    )
                    await self.data_store.ingest_event("paper_fill", fill_event)

                    maker_fee_total_cents = self.quoter.get_maker_fee_cents(vorder.price_cents, fill_qty)
                    maker_fee_per_contract = (maker_fee_total_cents / fill_qty) if fill_qty > 0 else 0.0
                    fill_yes_price = float(vorder.price_cents) if vorder.side == Side.YES_BID else float(100 - vorder.price_cents)
                    self.pending_edge_evals.append(
                        {
                            "ticker": vorder.ticker,
                            "side": vorder.side.value,
                            "fill_ts": int(trade.exchange_ts),
                            "fill_size": int(fill_qty),
                            "target_ts": int(trade.exchange_ts + self.edge_horizon_ms),
                            "fill_yes_price": fill_yes_price,
                            "sign": 1.0 if vorder.side == Side.YES_BID else -1.0,
                            "maker_fee_cents_per_contract": float(maker_fee_per_contract),
                            "sigma_at_quote": vorder.sigma_at_quote,
                            "tte_ms_at_quote": vorder.tte_ms_at_quote,
                            "half_spread_cents": int(vorder.half_spread_cents_at_quote),
                            "top_depth_at_quote": int(vorder.top_depth_at_quote),
                        }
                    )
                    
                    # Inventory is tracked as net YES contracts.
                    direction = 1 if vorder.side == Side.YES_BID else -1
                    self.quoter.inventory[vorder.ticker] = self.quoter.inventory.get(vorder.ticker, 0) + (fill_qty * direction)
                    self.risk.update_inventory(vorder.ticker, fill_qty * direction)
                    
                    vorder.size -= fill_qty
                    vorder.queue_ahead = 0
                    
                    if vorder.size <= 0:
                        del self.active_orders[order_id]

    @staticmethod
    def _years_to_expiry(close_ts_ms: int, now_ts_ms: int) -> float:
        if close_ts_ms <= now_ts_ms:
            return 0.0
        return (close_ts_ms - now_ts_ms) / (365.0 * 24.0 * 3600.0 * 1000.0)

    def _find_order_for_ticker_side(self, ticker: str, side: Side):
        candidates: list[tuple[str, VirtualOrder]] = []
        for order_id, order in self.active_orders.items():
            if order.ticker == ticker and order.side == side:
                candidates.append((order_id, order))
        if not candidates:
            return None, None
        # Prefer non-canceling live order with latest placement.
        non_canceling = [c for c in candidates if c[1].cancel_effective_ts is None]
        pick_from = non_canceling if non_canceling else candidates
        pick_from.sort(key=lambda x: x[1].placed_at_ts, reverse=True)
        return pick_from[0]

    def _request_cancel_order(self, order_id: str, current_ts: int):
        order = self.active_orders.get(order_id)
        if not order:
            return
        if order.cancel_effective_ts is not None:
            return
        _, cancel_lag_ms, _ = self._realism_params()
        if cancel_lag_ms <= 0:
            self.active_orders.pop(order_id, None)
            return
        order.cancel_requested_ts = current_ts
        order.cancel_effective_ts = current_ts + cancel_lag_ms

    def _cancel_orders_for_ticker(self, ticker: str, current_ts: int | None = None, force_immediate: bool = False):
        now_ts = int(current_ts if current_ts is not None else (time.time() * 1000))
        for order_id, order in list(self.active_orders.items()):
            if order.ticker == ticker:
                if force_immediate:
                    self.active_orders.pop(order_id, None)
                else:
                    self._request_cancel_order(order_id, now_ts)

    def _upsert_virtual_order(
        self,
        ticker: str,
        side: Side,
        price_cents: int,
        size: int,
        queue_ahead: int,
        placed_at_ts: int,
        fair_prob_at_quote: float,
        sigma_at_quote: float,
        tte_ms_at_quote: int,
        half_spread_cents_at_quote: int,
        top_depth_at_quote: int,
    ):
        existing_id, existing = self._find_order_for_ticker_side(ticker, side)
        if (
            existing
            and existing.price_cents == price_cents
            and existing.size == size
            and existing.cancel_effective_ts is None
        ):
            return

        if existing_id and existing:
            for order_id, order in list(self.active_orders.items()):
                if order.ticker == ticker and order.side == side and order.cancel_effective_ts is None:
                    self._request_cancel_order(order_id, placed_at_ts)

        order_latency_ms, _, _ = self._realism_params()
        new_order = VirtualOrder(
            order_id=str(uuid.uuid4()),
            ticker=ticker,
            side=side,
            price_cents=price_cents,
            size=size,
            queue_ahead=queue_ahead,
            placed_at_ts=placed_at_ts,
            fair_prob_at_quote=fair_prob_at_quote,
            sigma_at_quote=sigma_at_quote,
            tte_ms_at_quote=tte_ms_at_quote,
            half_spread_cents_at_quote=half_spread_cents_at_quote,
            top_depth_at_quote=top_depth_at_quote,
            active_from_ts=placed_at_ts + max(0, order_latency_ms),
        )
        self.active_orders[new_order.order_id] = new_order
        
    async def _run_strategy_tick(self, current_ts: int):
        """Compute Fair value, bounds check inventory, send Virtual Orders"""
        self._cleanup_inactive_orders(current_ts)
        await self._finalize_fee_adjusted_edges(current_ts)
        self._apply_spread_governor(current_ts)

        # Guardrail #2: Heartbeat check
        if not self.risk.check_heartbeat(current_ts):
            logging.warning("Tick abort: Heartbeat check failed")
            for order_id in list(self.active_orders.keys()):
                self._request_cancel_order(order_id, current_ts)
            return

        # Guardrail #3: Warmup gate
        if not self.risk.is_warmed_up(self.quant.samples_collected, len(self.orderbooks) > 0):
            logging.warning(f"Tick abort: Warmup gate failed (samples={self.quant.samples_collected}, ob_len={len(self.orderbooks)})")
            return

        # 1. Compute fair values consistently
        spot = self.quant.last_spot
        if spot <= 0:
            logging.warning("Tick abort: spot <= 0")
            return

        eligible_tickers = []
        for ticker in self.active_tickers:
            strike = self.ticker_to_strike.get(ticker)
            if strike is None:
                continue
            close_ts = self.ticker_close_ts.get(ticker, 0)
            if close_ts <= 0:
                continue
            min_tte_ms = max(self.min_quote_tte_ms, self.exclude_last_minutes * 60 * 1000)
            if (close_ts - current_ts) <= min_tte_ms:
                continue
            if ticker not in self.orderbooks:
                continue
            if not self._passes_market_eligibility(ticker=ticker, current_ts=current_ts):
                continue
            eligible_tickers.append((ticker, strike, close_ts))

        if not eligible_tickers:
            logging.warning("Tick abort: eligible_tickers is empty")
            return

        # Belt-and-suspenders: enforce one expiry ladder inside the engine.
        by_close_ts: Dict[int, List[tuple[str, float, int]]] = {}
        for item in eligible_tickers:
            by_close_ts.setdefault(item[2], []).append(item)
        selected_close_ts, selected_group = sorted(
            by_close_ts.items(),
            key=lambda kv: (-len(kv[1]), kv[0]),
        )[0]
        eligible_tickers = selected_group
        eligible_tickers.sort(key=lambda x: x[1])
        ranked_tickers = self._rank_eligible_tickers([t[0] for t in eligible_tickers], current_ts=current_ts)
        eligible_tickers = [t for t in eligible_tickers if t[0] in set(ranked_tickers)]
        if not eligible_tickers:
            logging.warning("Tick abort: ranked_tickers resulted in empty eligible list")
            return
        sorted_tickers = [t[0] for t in eligible_tickers]
        strikes = [t[1] for t in eligible_tickers]
        eligible_set = set(sorted_tickers)
        for order_id, order in list(self.active_orders.items()):
            if order.ticker not in eligible_set:
                self._request_cancel_order(order_id, current_ts)

        tte_years = self._years_to_expiry(selected_close_ts, current_ts)
        tte_ms_at_quote = max(0, int(selected_close_ts - current_ts))
        if tte_years <= 0:
            for ticker in sorted_tickers:
                self._cancel_orders_for_ticker(ticker, current_ts=current_ts)
            return

        fair_values = self.quant.compute_fair_values(spot, strikes, tte_years)
        
        # 2. Generate Quotes
        for val, ticker in zip(fair_values, sorted_tickers):
            ob = self.orderbooks.get(ticker)
            if not ob or not ob.is_valid:
                self._cancel_orders_for_ticker(ticker, current_ts=current_ts)
                continue

            direction = self.ticker_direction.get(ticker, "above")
            logging.warning(f"Ticker {ticker}: direction={direction}, is_valid={ob.is_valid}")
            
            # Temporary bypass for testnet 'B' (Between) tracking
            if direction == "range":
                direction = "above"

            fair_prob = val.prob if direction != "below" else (1.0 - val.prob)
            fair_prob = max(0.001, min(0.999, fair_prob))

            quote = self.quoter.generate_quote(
                ticker=ticker,
                fair_prob=fair_prob,
                spot=spot,
                strike=val.strike,
                current_time_ms=current_ts,
                vol=self.quant.sigma,
                quote_size=self.quote_size,
                tte_years=tte_years,
            )
            
            logging.warning(f"Quoter returned: {quote}")

            if quote:
                logging.info(f"VARIANT 1 QUOTER OUTPUT: {ticker} bid={quote.yes_bid_cents} ask={100-quote.no_bid_cents}")
                
            if not quote:
                self._cancel_orders_for_ticker(ticker, current_ts=current_ts)
                continue

            is_new_quote = self.quoter.last_quote_time.get(ticker, 0) == current_ts
            if is_new_quote:
                audit_event = QuoteAuditEvent(
                    exchange_ts=current_ts,
                    ingest_ts=int(time.time() * 1000),
                    ticker=ticker,
                    fair_prob=float(quote.fair_prob),
                    sigma=float(quote.sigma),
                    tte_ms=tte_ms_at_quote,
                    sigma_t=float(quote.sigma_t),
                    inventory=int(quote.inventory),
                    inventory_skew=float(quote.inventory_skew),
                    yes_bid_cents=int(quote.yes_bid_cents),
                    no_bid_cents=int(quote.no_bid_cents),
                    half_spread_cents=int(quote.half_spread_cents),
                    fee_widen_steps=int(quote.fee_widen_steps),
                )
                await self.data_store.ingest_event("quote_audit", audit_event)

            # Kalshi is bids-only complement book:
            # - Buy YES by resting YES_BID at quote.yes_bid_cents
            # - Sell YES by resting NO_BID at quote.no_bid_cents
            yes_bid_price = quote.yes_bid_cents
            no_bid_price = quote.no_bid_cents

            yes_qa = ob.yes_bids.get(yes_bid_price, 0)
            no_qa = ob.no_bids.get(no_bid_price, 0)
            best_yes = max(ob.yes_bids.keys()) if ob.yes_bids else yes_bid_price
            best_no = max(ob.no_bids.keys()) if ob.no_bids else no_bid_price
            yes_top_depth = int(ob.yes_bids.get(best_yes, 0))
            no_top_depth = int(ob.no_bids.get(best_no, 0))

            self._upsert_virtual_order(
                ticker=ticker,
                side=Side.YES_BID,
                price_cents=yes_bid_price,
                size=quote.size,
                queue_ahead=yes_qa,
                placed_at_ts=current_ts,
                fair_prob_at_quote=fair_prob,
                sigma_at_quote=self.quant.sigma,
                tte_ms_at_quote=tte_ms_at_quote,
                half_spread_cents_at_quote=quote.half_spread_cents,
                top_depth_at_quote=yes_top_depth,
            )
            self._upsert_virtual_order(
                ticker=ticker,
                side=Side.NO_BID,
                price_cents=no_bid_price,
                size=quote.size,
                queue_ahead=no_qa,
                placed_at_ts=current_ts,
                fair_prob_at_quote=fair_prob,
                sigma_at_quote=self.quant.sigma,
                tte_ms_at_quote=tte_ms_at_quote,
                half_spread_cents_at_quote=quote.half_spread_cents,
                top_depth_at_quote=no_top_depth,
            )
