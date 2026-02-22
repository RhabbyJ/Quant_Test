from dataclasses import dataclass
from typing import Optional
from enum import Enum

class Side(Enum):
    YES_BID = "yes_bid"
    NO_BID = "no_bid"
    YES_ASK = "yes_ask" # derived
    NO_ASK = "no_ask"   # derived

@dataclass
class OrderbookSnapshotEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    yes_bids: list[tuple[int, int]] # list of (price, size)
    no_bids: list[tuple[int, int]]
    
@dataclass
class OrderbookDeltaEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    price_cents: int
    size: int
    side: Side
    seq: Optional[int] = None

@dataclass
class TradeEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    price_cents: int
    size: int
    side: Optional[Side] = None # Public trades might not always specify aggressor side cleanly, but we capture if available
    seq: Optional[int] = None
@dataclass
class LifecycleEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    status: str
    seq: Optional[int] = None

@dataclass
class MarketMetadataEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    close_ts: int
    status: Optional[str] = None
    direction: Optional[str] = None   # above | below | range
    strike_low: Optional[float] = None
    strike_high: Optional[float] = None
    settlement_window: Optional[str] = None

@dataclass
class PaperFillEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    price_cents: int
    size: int
    is_bid: bool
    side: Optional[str] = None
    queue_ahead_at_fill: Optional[int] = None
    time_since_quote_ms: Optional[int] = None
    fair_prob_at_quote: Optional[float] = None
    sigma_at_quote: Optional[float] = None
    tte_ms_at_quote: Optional[int] = None

@dataclass
class SpotEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str # e.g. "BTC"
    price: float


@dataclass
class QuoteAuditEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    fair_prob: float
    sigma: float
    tte_ms: int
    sigma_t: float
    inventory: int
    inventory_skew: float
    yes_bid_cents: int
    no_bid_cents: int
    half_spread_cents: int
    fee_widen_steps: int
