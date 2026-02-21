import math
from typing import Dict, Optional
from dataclasses import dataclass

@dataclass
class Quote:
    ticker: str
    yes_bid_cents: int
    no_bid_cents: int
    size: int

class Quoter:
    """
    Translates Fair Value probabilities into discrete Kalshi quote prices (cents).
    Enforces inventory skew, fee-cleared minimum spreads, and API throttling.
    """
    def __init__(
        self,
        max_inventory: int = 1000,
        inventory_skew_coef: float = 0.05,
        vol_spread_mult: float = 5.0,
        min_quote_lifetime_ms: int = 500,
        atm_log_moneyness_limit: float = 0.02,
    ):
        self.inventory: Dict[str, int] = {} # ticker -> net YES contracts
        self.max_inventory = max_inventory
        self.inventory_skew_coef = inventory_skew_coef
        self.vol_spread_mult = vol_spread_mult
        self.min_quote_lifetime_ms = min_quote_lifetime_ms
        self.atm_log_moneyness_limit = atm_log_moneyness_limit
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
        if spot <= 0 or abs(math.log(strike / spot)) > self.atm_log_moneyness_limit:
            return None
            
        # Inventory Skew (Shift mid downward if long YES, upward if short YES)
        net_pos = self.inventory.get(ticker, 0)
        # Max skew of 5% probability at max inventory limits
        inventory_skew = (net_pos / self.max_inventory) * self.inventory_skew_coef
        
        skewed_prob = fair_prob - inventory_skew
        skewed_cents = round(skewed_prob * 100)
        
        # Volatility-based spread widening
        # Base spread 1 cent, widened heavily by high volatility
        half_spread_cents = max(1, round(vol * self.vol_spread_mult))
        
        yes_bid = skewed_cents - half_spread_cents
        yes_ask = skewed_cents + half_spread_cents
        
        # Clamp to valid tick sizes [1, 99]
        yes_bid = max(1, min(98, yes_bid))
        yes_ask = max(2, min(99, yes_ask))
        if yes_bid >= yes_ask:
            yes_bid = yes_ask - 1
             
        # Fee clearance check: widen spread until it clears maker fees for both legs
        fee_bid = self.get_maker_fee_cents(yes_bid, quote_size)
        fee_ask = self.get_maker_fee_cents(yes_ask, quote_size)
        
        # Expected gross edge minus fees
        if (yes_ask - yes_bid) * quote_size <= (fee_bid + fee_ask):
            yes_ask += 1
            yes_bid -= 1
            yes_bid = max(1, yes_bid)
            yes_ask = min(99, yes_ask)
             
        # If it still doesn't clear (due to clamping), we refuse to quote.
        fee_bid = self.get_maker_fee_cents(yes_bid, quote_size)
        fee_ask = self.get_maker_fee_cents(yes_ask, quote_size)
        if (yes_ask - yes_bid) * quote_size <= (fee_bid + fee_ask):
            return None 

        # Kalshi bids-only representation:
        # sell YES leg is posted as NO bid complement of YES ask.
        no_bid = max(1, min(99, 100 - yes_ask))

        # Guardrail #8: Rate-Aware Quoter
        last_quote = self.last_quotes.get(ticker)
        last_time = self.last_quote_time.get(ticker, 0)
        
        # Minimum quote lifetime = 500ms to avoid API churn
        if current_time_ms - last_time < self.min_quote_lifetime_ms:
            return last_quote
            
        # Only re-quote if target output changed by minimum of 1 tick
        if last_quote and last_quote.yes_bid_cents == yes_bid and last_quote.no_bid_cents == no_bid:
            return last_quote

        quote = Quote(ticker, yes_bid, no_bid, quote_size)
        self.last_quotes[ticker] = quote
        self.last_quote_time[ticker] = current_time_ms
        return quote
