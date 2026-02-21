import asyncio
import json
import logging
import os
from datetime import datetime, timezone
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


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _safe_write_runtime_status(path: str, payload: dict):
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(out_path)


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

    # 1. Tickers to subscribe to
    tickers_env = os.getenv("KALSHI_TICKERS", "")
    manual_tickers = [t.strip() for t in tickers_env.split(",") if t.strip()] if tickers_env else []
    tickers = list(manual_tickers)
    discovery_used = False
    discovered_close_ts_ms = None
    discovered_ref_spot = None

    if _env_flag("KALSHI_DISCOVER_TICKERS", True):
        try:
            from ingest.discovery import discover_kxbtc_tickers

            ref_spot = None
            ref_spot_raw = os.getenv("KALSHI_DISCOVERY_SPOT")
            if ref_spot_raw:
                ref_spot = float(ref_spot_raw)
            else:
                mock_spot_raw = os.getenv("MOCK_SPOT_START")
                if mock_spot_raw:
                    ref_spot = float(mock_spot_raw)

            result = await asyncio.to_thread(
                discover_kxbtc_tickers,
                rest_base_url=rest_base_url,
                series_ticker=os.getenv("KALSHI_DISCOVERY_SERIES", "KXBTC"),
                desired_count=int(os.getenv("KALSHI_DISCOVERY_COUNT", "3")),
                min_close_min=int(os.getenv("KALSHI_DISCOVERY_MIN_CLOSE_MIN", "30")),
                max_close_min=int(os.getenv("KALSHI_DISCOVERY_MAX_CLOSE_MIN", "180")),
                reference_spot=ref_spot,
                max_pages=int(os.getenv("KALSHI_DISCOVERY_MAX_PAGES", "6")),
            )
            if result.tickers:
                tickers = result.tickers
                discovery_used = True
                discovered_close_ts_ms = result.close_ts_ms
                discovered_ref_spot = result.reference_spot
                close_iso = (
                    datetime.fromtimestamp(result.close_ts_ms / 1000.0, tz=timezone.utc).isoformat()
                    if result.close_ts_ms
                    else "unknown"
                )
                logging.info(
                    "Auto-discovery selected close=%s ref_spot=%s tickers=%s (candidates=%s)",
                    close_iso,
                    f"{result.reference_spot:.2f}" if result.reference_spot else "n/a",
                    tickers,
                    result.total_candidates,
                )
                for m in result.selected:
                    logging.info(
                        "Selected ticker=%s strike=%.2f activity=%.2f",
                        m.ticker,
                        m.strike,
                        m.activity_score,
                    )
            else:
                logging.warning("Auto-discovery returned no tickers; using manual KALSHI_TICKERS fallback.")
        except Exception as e:
            logging.warning("Auto-discovery failed (%s); using manual KALSHI_TICKERS fallback.", e)

    if not tickers:
        raise RuntimeError(
            "No Kalshi tickers available. Set KALSHI_TICKERS or enable KALSHI_DISCOVER_TICKERS "
            "with reachable Kalshi market data."
        )

    # 2. Initialize Parquet storage buffer
    data_store = BufferedParquetWriter(data_dir="./data_warehouse", flush_interval_sec=2.0)
    
    # 3. Initialize the deterministic Engine Loop
    engine = EngineLoop(data_store=data_store, tickers=tickers)
    engine.risk.warmup_samples = int(os.getenv("WARMUP_SAMPLES", str(engine.risk.warmup_samples)))
    engine.risk.kalshi_heartbeat_ms = int(os.getenv("KALSHI_HEARTBEAT_MS", str(engine.risk.kalshi_heartbeat_ms)))
    engine.min_quote_tte_ms = int(os.getenv("MIN_QUOTE_TTE_MS", str(engine.min_quote_tte_ms)))
    
    # 4. Initialize Kalshi WS Consumer

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

    use_mock_spot = _env_flag("ENABLE_MOCK_SPOT", True)

    logging.info("Starting Kalshi PM MVP Architecture")
    logging.info(
        "Kalshi mode=%s env=%s tickers=%s (discovery_used=%s)",
        os.getenv("KALSHI_MODE", "auto"),
        kalshi_env,
        tickers,
        discovery_used,
    )
    logging.info(
        "Risk warmup_samples=%s kalshi_heartbeat_ms=%s min_quote_tte_ms=%s mock_spot_start=%s",
        engine.risk.warmup_samples,
        engine.risk.kalshi_heartbeat_ms,
        engine.min_quote_tte_ms,
        spot_start_price,
    )

    runtime_status = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "kalshi_env": kalshi_env,
        "kalshi_mode": os.getenv("KALSHI_MODE", "auto"),
        "tickers": tickers,
        "discovery_used": discovery_used,
        "discovered_close_ts_ms": discovered_close_ts_ms,
        "discovered_close_iso": (
            datetime.fromtimestamp(discovered_close_ts_ms / 1000.0, tz=timezone.utc).isoformat()
            if discovered_close_ts_ms
            else None
        ),
        "discovered_reference_spot": discovered_ref_spot,
        "warmup_samples": engine.risk.warmup_samples,
        "kalshi_heartbeat_ms": engine.risk.kalshi_heartbeat_ms,
        "min_quote_tte_ms": engine.min_quote_tte_ms,
        "mock_spot_enabled": use_mock_spot,
        "mock_spot_start": spot_start_price if use_mock_spot else None,
        "discovery_window_min_close_min": int(os.getenv("KALSHI_DISCOVERY_MIN_CLOSE_MIN", "30")),
        "discovery_window_max_close_min": int(os.getenv("KALSHI_DISCOVERY_MAX_CLOSE_MIN", "180")),
    }
    _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)

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
