import time
import logging
from typing import Dict

class RiskEngine:
    """ Guardrail #2: Spot Heartbeat / Stale Data Kills
        Guardrail #3: Warm-up gate
        Inventory Limits
    """
    def __init__(
        self,
        max_inventory: int = 1000,
        spot_heartbeat_ms: int = 2000,
        kalshi_heartbeat_ms: int = 30000,
        auto_recover_ms: int = 10000,
        warmup_samples: int = 300,
    ):
        self.max_inventory = max_inventory
        self.spot_heartbeat_ms = spot_heartbeat_ms
        self.kalshi_heartbeat_ms = kalshi_heartbeat_ms
        self.auto_recover_ms = auto_recover_ms
        self.warmup_samples = warmup_samples
        
        self.last_spot_ts = 0
        self.last_kalshi_ts = 0
        self.inventory: Dict[str, int] = {}
        self.is_risk_off = False
        self.risk_off_reason = ""
        self.risk_off_since_ts = 0
        self.risk_off_recoverable = False
        self.healthy_since_ts = 0
    
    def check_heartbeat(self, current_ts: int) -> bool:
        """ Returns False if heartbeat failed, triggering Risk Off """
        spot_stale = self.last_spot_ts <= 0 or (current_ts - self.last_spot_ts) > self.spot_heartbeat_ms
        kalshi_stale = self.last_kalshi_ts <= 0 or (current_ts - self.last_kalshi_ts) > self.kalshi_heartbeat_ms

        if spot_stale or kalshi_stale:
            self.healthy_since_ts = 0
            if spot_stale:
                self.trigger_risk_off("Spot feed stale / Clock jumped", recoverable=True, current_ts=current_ts)
            else:
                self.trigger_risk_off("Kalshi feed stale / No deltas-trades observed", recoverable=True, current_ts=current_ts)
            return False

        if self.healthy_since_ts <= 0:
            self.healthy_since_ts = current_ts

        if self.is_risk_off:
            if self.can_recover(current_ts):
                self.clear_risk_off("Feeds healthy for recovery window")
            else:
                return False

        return True

    def is_warmed_up(self, samples_collected: int, book_stable: bool) -> bool:
        """ Ensure sigma stabilizes and book synchronized """
        if samples_collected < self.warmup_samples:
            return False
        if not book_stable:
            return False
        return True

    def can_recover(self, current_ts: int) -> bool:
        if not self.is_risk_off or not self.risk_off_recoverable:
            return False
        if self.healthy_since_ts <= 0:
            return False
        return (current_ts - self.healthy_since_ts) >= self.auto_recover_ms

    def clear_risk_off(self, reason: str):
        if not self.is_risk_off:
            return
        self.is_risk_off = False
        self.risk_off_reason = ""
        self.risk_off_recoverable = False
        self.risk_off_since_ts = 0
        logging.warning(f"RISK OFF CLEARED: {reason}")

    def trigger_risk_off(self, reason: str, recoverable: bool = True, current_ts: int | None = None):
        """ Global Risk Off - Freezes Quoting immediately """
        ts = int(current_ts if current_ts is not None else (time.time() * 1000))
        if not self.is_risk_off:
            self.is_risk_off = True
            self.risk_off_reason = reason
            self.risk_off_since_ts = ts
            self.risk_off_recoverable = recoverable
            logging.error(f"RISK OFF TRIGGERED: {reason}")
            return

        if (not recoverable) and self.risk_off_recoverable:
            self.risk_off_reason = reason
            self.risk_off_since_ts = ts
            self.risk_off_recoverable = False
            logging.error(f"RISK OFF ESCALATED: {reason}")
             
    def update_inventory(self, ticker: str, delta_contracts: int):
        current = self.inventory.get(ticker, 0)
        self.inventory[ticker] = current + delta_contracts
        
        # Immediate Kill Switch for limit breach
        if abs(self.inventory[ticker]) > self.max_inventory:
            self.trigger_risk_off(
                f"Max inventory breached on {ticker}: {self.inventory[ticker]}",
                recoverable=False,
            )
