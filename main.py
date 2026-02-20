import asyncio
import logging
import os
from pathlib import Path
from statistics import median

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None
from engine.simulator import EngineLoop
from data.store import BufferedParquetWriter
from ingest.kalshi_ws import KalshiWSConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _merge_no_proxy(existing: str, additions: list[str]) -> str:
    items = []
    seen = set()
    for raw in (existing or "").split(",") + additions:
        val = raw.strip()
        if not val:
            continue
        key = val.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(val)
    return ",".join(items)


async def main():
    if load_dotenv:
        if Path(".env").exists():
            load_dotenv(dotenv_path=".env", override=False)
        if Path(".env.local").exists():
            load_dotenv(dotenv_path=".env.local", override=False)
    elif Path(".env").exists() or Path(".env.local").exists():
        logging.warning("Found .env file(s), but python-dotenv is not installed; env files were not auto-loaded.")

    # Bypass system proxies for Kalshi if requested
    if os.getenv("KALSHI_DISABLE_PROXY", "true").lower() in {"1", "true", "yes", "on"}:
        bypass_hosts = ["demo-api.kalshi.co", "api.elections.kalshi.com", "localhost", "127.0.0.1"]
        merged = _merge_no_proxy(os.getenv("NO_PROXY", ""), bypass_hosts)
        os.environ["NO_PROXY"] = merged
        os.environ["no_proxy"] = merged

    # 1. Tickers to subscribe to
    tickers_env = os.getenv("KALSHI_TICKERS", "")
    tickers = [t.strip() for t in tickers_env.split(",") if t.strip()] if tickers_env else [
        "BTC-24DEC31-T100000",
        "BTC-24DEC31-T110000",
        "BTC-24DEC31-T120000",
    ]

    # 2. Initialize Parquet storage buffer
    data_store = BufferedParquetWriter(data_dir="./data_warehouse", flush_interval_sec=2.0)
    
    # 3. Initialize the deterministic Engine Loop
    engine = EngineLoop(data_store=data_store, tickers=tickers)
    engine.risk.warmup_samples = int(os.getenv("WARMUP_SAMPLES", str(engine.risk.warmup_samples)))
    
    # 4. Initialize Kalshi WS Consumer
    kalshi_env = os.getenv("KALSHI_ENV", "demo").lower()
    ws_uri = (
        "wss://api.elections.kalshi.com/trade-api/ws/v2"
        if kalshi_env == "prod"
        else "wss://demo-api.kalshi.co/trade-api/ws/v2"
    )
    rest_base_url = (
        "https://api.elections.kalshi.com"
        if kalshi_env == "prod"
        else "https://demo-api.kalshi.co"
    )

    kalshi_ws = KalshiWSConsumer(
        uri=ws_uri,
        rest_base_url=rest_base_url,
        tickers=tickers,
        event_queue=engine.event_queue,
        mode=os.getenv("KALSHI_MODE", "auto"),
        api_key_id=os.getenv("KALSHI_API_KEY_ID"),
        private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
        private_key_pem=os.getenv("KALSHI_PRIVATE_KEY_PEM"),
        private_key_password=os.getenv("KALSHI_PRIVATE_KEY_PASSWORD"),
    )
    
    # Mocking Spot Feed for now
    from ingest.spot_ws import MockSpotFeed

    strike_values = list(engine.ticker_to_strike.values())
    default_spot_start = median(strike_values) if strike_values else 110000.0
    spot_start_price = float(os.getenv("MOCK_SPOT_START", str(default_spot_start)))
    spot_interval_ms = int(os.getenv("MOCK_SPOT_INTERVAL_MS", "1000"))
    spot_ws = MockSpotFeed(
        start_price=spot_start_price,
        event_queue=engine.event_queue,
        interval_ms=spot_interval_ms,
    )

    use_mock_spot = os.getenv("ENABLE_MOCK_SPOT", "true").lower() in {"1", "true", "yes", "on"}

    logging.info("Starting Kalshi PM MVP Architecture")
    logging.info("Kalshi mode=%s env=%s tickers=%s", os.getenv("KALSHI_MODE", "auto"), kalshi_env, tickers)
    logging.info("Risk warmup_samples=%s mock_spot_start=%s", engine.risk.warmup_samples, spot_start_price)

    # Run everything concurrently
    tasks = [
        engine.start(),
        kalshi_ws.start(),
    ]
    if use_mock_spot:
        tasks.append(spot_ws.start())
    else:
        logging.warning("Mock spot feed is disabled. Warmup gate may block quoting unless an external spot feed is wired.")

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down MVP")
