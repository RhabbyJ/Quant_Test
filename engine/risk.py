import time
import logging
from typing import Dict

class RiskEngine:
    """ Guardrail #2: Spot Heartbeat / Stale Data Kills
        Guardrail #3: Warm-up gate
        Inventory Limits
    """
    def __init__(self, max_inventory: int = 1000, spot_heartbeat_ms: int = 2000, warmup_samples: int = 300):
        self.max_inventory = max_inventory
        self.spot_heartbeat_ms = spot_heartbeat_ms
        self.warmup_samples = warmup_samples
        
        self.last_spot_ts = 0
        self.inventory: Dict[str, int] = {}
        self.is_risk_off = False
        self.risk_off_reason = ""
    
    def check_heartbeat(self, current_ts: int) -> bool:
        """ Returns False if heartbeat failed, triggering Risk Off """
        if self.last_spot_ts > 0 and (current_ts - self.last_spot_ts) > self.spot_heartbeat_ms:
            self.trigger_risk_off("Spot feed stale / Clock jumped")
            return False
            
        return not self.is_risk_off

    def is_warmed_up(self, samples_collected: int, book_stable: bool) -> bool:
        """ Ensure sigma stabilizes and book synchronized """
        if samples_collected < self.warmup_samples:
            return False
        if not book_stable:
            return False
        return True

    def trigger_risk_off(self, reason: str):
        """ Global Risk Off - Freezes Quoting immediately """
        if not self.is_risk_off:
            self.is_risk_off = True
            self.risk_off_reason = reason
            logging.error(f"RISK OFF TRIGGERED: {reason}")
            
    def update_inventory(self, ticker: str, delta_contracts: int):
        current = self.inventory.get(ticker, 0)
        self.inventory[ticker] = current + delta_contracts
        
        # Immediate Kill Switch for limit breach
        if abs(self.inventory[ticker]) > self.max_inventory:
            self.trigger_risk_off(f"Max inventory breached on {ticker}: {self.inventory[ticker]}")
