import asyncio
import time
import logging
import uuid
import re
from typing import Any, Dict, List
from dataclasses import dataclass

from core.types import (
    LifecycleEvent,
    MarketMetadataEvent,
    OrderbookDeltaEvent,
    OrderbookSnapshotEvent,
    PaperFillEvent,
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
        
        # Ticker -> strike mapping for fair value ordering.
        # Supports legacy BTC-...-Txxxxx and new KXBTC-...-(B|T)xxxxx[.yy] styles.
        self.ticker_to_strike = {}
        seed_tickers = tickers or [
            "BTC-24DEC31-T100000",
            "BTC-24DEC31-T110000",
            "BTC-24DEC31-T120000",
        ]
        for ticker in seed_tickers:
            strike = self._parse_strike_from_ticker(ticker)
            if strike is not None:
                self.ticker_to_strike[ticker] = strike

    @staticmethod
    def _parse_strike_from_ticker(ticker: str) -> float | None:
        # Match suffixes like T100000, T75999.99, B70125.
        m = re.search(r"(?:-|^)([TB])(\d+(?:\.\d+)?)$", ticker)
        if not m:
            return None
        try:
            return float(m.group(2))
        except ValueError:
            return None
        
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
                self._run_strategy_tick(batch[-1].exchange_ts)
                
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
                logging.info(f"[{event.ticker}] Applied REST Snapshot")
                if event.ticker not in self.ticker_to_strike:
                    strike = self._parse_strike_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                
            elif isinstance(event, OrderbookDeltaEvent):
                await self.data_store.ingest_event("orderbook_delta", event)
                ob = self.orderbooks.setdefault(event.ticker, Orderbook(event.ticker))
                ob.apply_delta(event)
                self.risk.last_kalshi_ts = event.exchange_ts
                if event.ticker not in self.ticker_to_strike:
                    strike = self._parse_strike_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                
            elif isinstance(event, TradeEvent):
                await self.data_store.ingest_event("trade", event)
                self.risk.last_kalshi_ts = event.exchange_ts
                if event.ticker not in self.ticker_to_strike:
                    strike = self._parse_strike_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike

            elif isinstance(event, MarketMetadataEvent):
                await self.data_store.ingest_event("market_meta", event)
                if event.close_ts > 0:
                    self.ticker_close_ts[event.ticker] = event.close_ts
                if event.ticker not in self.ticker_to_strike:
                    strike = self._parse_strike_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                
            elif isinstance(event, SpotEvent):
                await self.data_store.ingest_event("spot", event)
                self.risk.last_spot_ts = event.exchange_ts
                self.quant.update_spot(event.price)
                
            elif isinstance(event, LifecycleEvent):
                await self.data_store.ingest_event("lifecycle", event)
                if event.status != "open":
                    self.risk.trigger_risk_off(f"Market Lifecycle Closed for {event.ticker}")

    async def _resolve_paper_fills(self, batch: list[Any]):
        """Burn down queue_ahead using TradeEvents, record fills on YES/NO bid books."""
        trades = [e for e in batch if isinstance(e, TradeEvent)]
        
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

                # Conservative simulator model: any public trade at our resting level burns queue_ahead.
                vorder.queue_ahead -= trade.size
                
                if vorder.queue_ahead < 0:
                    fill_qty = min(vorder.size, abs(vorder.queue_ahead))
                    
                    logging.info(f"PAPER FILL: {vorder.ticker} {vorder.side.value} {fill_qty} @ {vorder.price_cents}c")
                    
                    fill_event = PaperFillEvent(
                        exchange_ts=trade.exchange_ts,
                        ingest_ts=int(time.time() * 1000),
                        ticker=vorder.ticker,
                        price_cents=vorder.price_cents,
                        size=fill_qty,
                        is_bid=(vorder.side == Side.YES_BID),
                        side=vorder.side.value,
                    )
                    await self.data_store.ingest_event("paper_fill", fill_event)
                    
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
        for order_id, order in self.active_orders.items():
            if order.ticker == ticker and order.side == side:
                return order_id, order
        return None, None

    def _cancel_orders_for_ticker(self, ticker: str):
        for order_id, order in list(self.active_orders.items()):
            if order.ticker == ticker:
                del self.active_orders[order_id]

    def _upsert_virtual_order(
        self,
        ticker: str,
        side: Side,
        price_cents: int,
        size: int,
        queue_ahead: int,
        placed_at_ts: int,
    ):
        existing_id, existing = self._find_order_for_ticker_side(ticker, side)
        if existing and existing.price_cents == price_cents and existing.size == size:
            return

        if existing_id:
            del self.active_orders[existing_id]

        new_order = VirtualOrder(
            order_id=str(uuid.uuid4()),
            ticker=ticker,
            side=side,
            price_cents=price_cents,
            size=size,
            queue_ahead=queue_ahead,
            placed_at_ts=placed_at_ts,
        )
        self.active_orders[new_order.order_id] = new_order
        
    def _run_strategy_tick(self, current_ts: int):
        """Compute Fair value, bounds check inventory, send Virtual Orders"""
        # Guardrail #2: Heartbeat check
        if not self.risk.check_heartbeat(current_ts):
            self.active_orders.clear() # Cancel all
            return
            
        # Guardrail #3: Warmup gate
        if not self.risk.is_warmed_up(self.quant.samples_collected, len(self.orderbooks) > 0):
            return

        # 1. Compute fair values consistently
        spot = self.quant.last_spot
        eligible_tickers = []
        for ticker, strike in self.ticker_to_strike.items():
            close_ts = self.ticker_close_ts.get(ticker, 0)
            if close_ts <= 0:
                continue
            if (close_ts - current_ts) <= self.min_quote_tte_ms:
                continue
            if ticker not in self.orderbooks:
                continue
            eligible_tickers.append((ticker, strike, close_ts))

        if not eligible_tickers:
            return

        eligible_tickers.sort(key=lambda x: x[1])
        sorted_tickers = [t[0] for t in eligible_tickers]
        strikes = [t[1] for t in eligible_tickers]
        eligible_set = set(sorted_tickers)
        for order_id, order in list(self.active_orders.items()):
            if order.ticker not in eligible_set:
                del self.active_orders[order_id]

        min_close_ts = min(t[2] for t in eligible_tickers)
        tte_years = self._years_to_expiry(min_close_ts, current_ts)
        if tte_years <= 0:
            for ticker in sorted_tickers:
                self._cancel_orders_for_ticker(ticker)
            return

        fair_values = self.quant.compute_fair_values(spot, strikes, tte_years)
        
        # 2. Generate Quotes
        for val, ticker in zip(fair_values, sorted_tickers):
            ob = self.orderbooks.get(ticker)
            if not ob or not ob.is_valid:
                self._cancel_orders_for_ticker(ticker)
                continue

            quote = self.quoter.generate_quote(
                ticker=ticker,
                fair_prob=val.prob,
                spot=spot,
                strike=val.strike,
                current_time_ms=current_ts,
                vol=self.quant.sigma,
                quote_size=10
            )
            
            if not quote:
                self._cancel_orders_for_ticker(ticker)
                continue

            # Kalshi is bids-only complement book:
            # - Buy YES by resting YES_BID at quote.bid_cents
            # - Sell YES by resting NO_BID at (100 - quote.ask_cents)
            yes_bid_price = quote.bid_cents
            no_bid_price = max(1, min(99, 100 - quote.ask_cents))

            yes_qa = ob.yes_bids.get(yes_bid_price, 0)
            no_qa = ob.no_bids.get(no_bid_price, 0)

            self._upsert_virtual_order(
                ticker=ticker,
                side=Side.YES_BID,
                price_cents=yes_bid_price,
                size=quote.size,
                queue_ahead=yes_qa,
                placed_at_ts=current_ts,
            )
            self._upsert_virtual_order(
                ticker=ticker,
                side=Side.NO_BID,
                price_cents=no_bid_price,
                size=quote.size,
                queue_ahead=no_qa,
                placed_at_ts=current_ts,
            )
