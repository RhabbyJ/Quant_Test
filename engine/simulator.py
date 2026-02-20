import asyncio
import time
import logging
import uuid
import re
from typing import Any, Dict, List
from dataclasses import dataclass

from core.types import OrderbookDeltaEvent, TradeEvent, LifecycleEvent, SpotEvent, Side
from data.store import BufferedParquetWriter
from engine.orderbook import Orderbook
from engine.quant import QuantEngine
from engine.quoter import Quoter
from engine.risk import RiskEngine

@dataclass
class VirtualOrder:
    order_id: str
    ticker: str
    is_bid: bool
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
        from core.types import OrderbookSnapshotEvent
        for event in batch:
            if isinstance(event, OrderbookSnapshotEvent):
                # Don't persist snapshot blobs to parquet directly in this MVP to save complexity,
                # just initialize the local L2 book so delta updates have a base.
                ob = self.orderbooks.setdefault(event.ticker, Orderbook(event.ticker))
                ob.apply_snapshot(event.yes_bids, event.no_bids)
                logging.info(f"[{event.ticker}] Applied REST Snapshot")
                
            elif isinstance(event, OrderbookDeltaEvent):
                await self.data_store.ingest_event("orderbook_delta", event)
                ob = self.orderbooks.setdefault(event.ticker, Orderbook(event.ticker))
                ob.apply_delta(event)
                if event.ticker not in self.ticker_to_strike:
                    strike = self._parse_strike_from_ticker(event.ticker)
                    if strike is not None:
                        self.ticker_to_strike[event.ticker] = strike
                
            elif isinstance(event, TradeEvent):
                await self.data_store.ingest_event("trade", event)
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
        """Burn down queue_ahead using TradeEvents, record fills."""
        trades = [e for e in batch if isinstance(e, TradeEvent)]
        
        for trade in trades:
            for order_id, vorder in list(self.active_orders.items()):
                if vorder.ticker == trade.ticker and vorder.price_cents == trade.price_cents:
                    # Very conservative: any public trade at our price burns our queue ahead
                    vorder.queue_ahead -= trade.size
                    
                    if vorder.queue_ahead < 0:
                        fill_qty = min(vorder.size, abs(vorder.queue_ahead))
                        
                        # Process Fill
                        logging.info(f"PAPER FILL: {vorder.ticker} {fill_qty} contracts @ {vorder.price_cents}c")
                        
                        from core.types import PaperFillEvent
                        fill_event = PaperFillEvent(
                            exchange_ts=trade.exchange_ts,
                            ingest_ts=int(time.time() * 1000),
                            ticker=vorder.ticker,
                            price_cents=vorder.price_cents,
                            size=fill_qty,
                            is_bid=vorder.is_bid
                        )
                        await self.data_store.ingest_event("paper_fill", fill_event)
                        
                        # Update Inventory
                        direction = 1 if vorder.is_bid else -1
                        self.quoter.inventory[vorder.ticker] = self.quoter.inventory.get(vorder.ticker, 0) + (fill_qty * direction)
                        self.risk.update_inventory(vorder.ticker, fill_qty * direction)
                        
                        vorder.size -= fill_qty
                        vorder.queue_ahead = 0
                        
                        if vorder.size <= 0:
                            del self.active_orders[order_id]
        
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
        sorted_tickers = sorted(self.ticker_to_strike.keys(), key=lambda t: self.ticker_to_strike[t])
        strikes = [self.ticker_to_strike[t] for t in sorted_tickers]
        
        # Dummy TTE (in reality, compute from Expiry - current_ts)
        tte_years = 0.05 
        fair_values = self.quant.compute_fair_values(spot, strikes, tte_years)
        
        # 2. Generate Quotes
        self.active_orders.clear() # Simple cancel/replace every tick for MVP purposes if limits allow
        
        for val, ticker in zip(fair_values, sorted_tickers):
            quote = self.quoter.generate_quote(
                ticker=ticker,
                fair_prob=val.prob,
                spot=spot,
                strike=val.strike,
                current_time_ms=current_ts,
                vol=self.quant.sigma,
                quote_size=10
            )
            
            if quote:
                ob = self.orderbooks.get(ticker)
                
                # Estimate Queue Ahead:
                bid_qa = ob.yes_bids.get(quote.bid_cents, 0) if ob else 0
                ask_qa = ob.yes_bids.get(quote.ask_cents, 0) if ob else 0 
                # (Remember Kalshi is bids-only. Ask QA calculation implies translating NO_BIDs, 
                # but for simpicity we assume 0 or derived here.)
                
                bid_order = VirtualOrder(str(uuid.uuid4()), ticker, True, quote.bid_cents, quote.size, bid_qa, current_ts)
                ask_order = VirtualOrder(str(uuid.uuid4()), ticker, False, quote.ask_cents, quote.size, ask_qa, current_ts)
                
                self.active_orders[bid_order.order_id] = bid_order
                self.active_orders[ask_order.order_id] = ask_order
