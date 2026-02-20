from dataclasses import dataclass
from typing import Optional, Literal
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
class PaperFillEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str
    price_cents: int
    size: int
    is_bid: bool

@dataclass
class SpotEvent:
    exchange_ts: int
    ingest_ts: int
    ticker: str # e.g. "BTC"
    price: float
