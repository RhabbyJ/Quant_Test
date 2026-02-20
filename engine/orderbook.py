from typing import Dict, List, Tuple
from dataclasses import dataclass
import logging
from core.types import Side, OrderbookDeltaEvent

@dataclass
class BBO:
    bid: int
    ask: int
    mid: float

class Orderbook:
    """
    Maintains a local reconstruction of a Kalshi L2 orderbook.
    Important: Kalshi returns "bids only" for YES and NO, since YES/NO are complements.
    """
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.yes_bids: Dict[int, int] = {}  # price_cents -> size
        self.no_bids: Dict[int, int] = {}
        self.is_valid = True

    def apply_snapshot(self, yes_bids: List[Tuple[int, int]], no_bids: List[Tuple[int, int]]):
        """Reset the book with a full snapshot"""
        self.yes_bids.clear()
        self.no_bids.clear()
        
        for price, size in yes_bids:
            if size > 0: 
                self.yes_bids[price] = size
        for price, size in no_bids:
            if size > 0: 
                self.no_bids[price] = size
                
        self._check_consistency()

    def apply_delta(self, event: OrderbookDeltaEvent):
        """Apply an incremental update to the book"""
        target_book = self.yes_bids if event.side == Side.YES_BID else self.no_bids

        # Kalshi orderbook_delta events are level deltas, not absolute sizes.
        # We update in-place and remove depleted levels.
        new_size = target_book.get(event.price_cents, 0) + event.size
        if new_size <= 0:
            target_book.pop(event.price_cents, None)
        else:
            target_book[event.price_cents] = new_size
            
        self._check_consistency()

    def _check_consistency(self):
        """
        Guardrail #7: Bids-only complement sanity checks to prevent corrupted midprices.
        """
        best_yes_bid = max(self.yes_bids.keys()) if self.yes_bids else 0
        best_no_bid = max(self.no_bids.keys()) if self.no_bids else 0
        
        derived_yes_ask = 100 - best_no_bid if best_no_bid > 0 else 100
        
        if best_yes_bid > derived_yes_ask:
            if self.is_valid:
                logging.error(f"[{self.ticker}] Data inconsistency: YES BID {best_yes_bid} > YES ASK {derived_yes_ask}")
            self.is_valid = False
        else:
            self.is_valid = True

    def get_yes_bbo(self) -> BBO:
        """
        Computes the executable implied mid for YES.
        Derived Ask = 100 - Best NO Bid.
        """
        best_yes_bid = max(self.yes_bids.keys()) if self.yes_bids else 0
        best_no_bid = max(self.no_bids.keys()) if self.no_bids else 0
        
        derived_yes_ask = 100 - best_no_bid if best_no_bid > 0 else 100
        
        # If orderbook is crossed/invalid due to timing/latency, we return the crossed state
        # but the strategy engine must check `self.is_valid` before quoting.
        mid = (best_yes_bid + derived_yes_ask) / 2.0
        
        return BBO(bid=best_yes_bid, ask=derived_yes_ask, mid=mid)
