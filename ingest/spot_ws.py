import asyncio
import time
import random
import logging
from core.types import SpotEvent

class MockSpotFeed:
    """
    Simulates a high-frequency BTC Spot feed via WebSockets.
    This drives the Fair Value Engine and is the primary heartbeat monitored by the Risk Engine.
    """
    def __init__(self, start_price: float, event_queue: asyncio.Queue, interval_ms: int = 1000):
        self.price = start_price
        self.event_queue = event_queue
        self.interval_ms = interval_ms
        self.is_running = False

    async def start(self):
        self.is_running = True
        logging.info("Starting Mock BTC Spot Feed")
        while self.is_running:
            # Simulate a realistic micro-structure random walk
            self.price *= (1 + random.gauss(0, 0.0001))
            
            ts = int(time.time() * 1000)
            event = SpotEvent(
                exchange_ts=ts,
                ingest_ts=ts,
                ticker="BTC",
                price=self.price
            )
            
            await self.event_queue.put(event)
            await asyncio.sleep(self.interval_ms / 1000.0)
