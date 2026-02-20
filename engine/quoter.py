import math
from typing import Dict, Optional
from dataclasses import dataclass

@dataclass
class Quote:
    ticker: str
    bid_cents: int
    ask_cents: int
    size: int

class Quoter:
    """
    Translates Fair Value probabilities into discrete Kalshi quote prices (cents).
    Enforces inventory skew, fee-cleared minimum spreads, and API throttling.
    """
    def __init__(self, max_inventory: int = 1000):
        self.inventory: Dict[str, int] = {} # ticker -> net YES contracts
        self.max_inventory = max_inventory
        self.last_quotes: Dict[str, Quote] = {}
        self.last_quote_time: Dict[str, int] = {}
        
    def get_maker_fee_cents(self, prob_cents: int, size: int) -> int:
        """
        Calculates the Kalshi Maker fee for a given probability and size.
        Formula: ceil(0.0175 * C * P * (1-P))
        """
        p = prob_cents / 100.0
        fee_dollars = 0.0175 * size * p * (1.0 - p)
        # Convert to cents and apply ceiling
        return math.ceil(fee_dollars * 100)

    def generate_quote(self, ticker: str, fair_prob: float, spot: float, strike: float,
                       current_time_ms: int, vol: float, quote_size: int = 10) -> Optional[Quote]:
        """
        Guardrail #5 constraints applied: Only quote if strike is near ATM.
        Guardrail #8 constraints applied: Throttle updates to avoid cancellation storms.
        """
        # Guardrail #5: Strike eligibility (Only quote strikes within ~2% of spot)
        if spot <= 0 or abs(math.log(strike / spot)) > 0.02:
            return None
            
        # Inventory Skew (Shift mid downward if long YES, upward if short YES)
        net_pos = self.inventory.get(ticker, 0)
        # Max skew of 5% probability at max inventory limits
        inventory_skew = (net_pos / self.max_inventory) * 0.05 
        
        skewed_prob = fair_prob - inventory_skew
        skewed_cents = round(skewed_prob * 100)
        
        # Volatility-based spread widening
        # Base spread 1 cent, widened heavily by high volatility
        half_spread_cents = max(1, round(vol * 5.0)) 
        
        bid = skewed_cents - half_spread_cents
        ask = skewed_cents + half_spread_cents
        
        # Clamp to valid tick sizes [1, 99]
        bid = max(1, min(98, bid))
        ask = max(2, min(99, ask))
        if bid >= ask:
            bid = ask - 1
            
        # Fee clearance check: widen spread until it clears maker fees for both legs
        fee_bid = self.get_maker_fee_cents(bid, quote_size)
        fee_ask = self.get_maker_fee_cents(ask, quote_size)
        
        # Expected gross edge minus fees
        if (ask - bid) * quote_size <= (fee_bid + fee_ask):
            ask += 1
            bid -= 1
            bid = max(1, bid)
            ask = min(99, ask)
            
        # If it still doesn't clear (due to clamping), we refuse to quote.
        fee_bid = self.get_maker_fee_cents(bid, quote_size)
        fee_ask = self.get_maker_fee_cents(ask, quote_size)
        if (ask - bid) * quote_size <= (fee_bid + fee_ask):
            return None 

        # Guardrail #8: Rate-Aware Quoter
        last_quote = self.last_quotes.get(ticker)
        last_time = self.last_quote_time.get(ticker, 0)
        
        # Minimum quote lifetime = 500ms to avoid API churn
        if current_time_ms - last_time < 500:
            return last_quote
            
        # Only re-quote if target output changed by minimum of 1 tick
        if last_quote and last_quote.bid_cents == bid and last_quote.ask_cents == ask:
            return last_quote

        quote = Quote(ticker, bid, ask, quote_size)
        self.last_quotes[ticker] = quote
        self.last_quote_time[ticker] = current_time_ms
        return quote
