import asyncio
import json
import logging
import os
import time
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
    discover_kxbtc_tickers = None

    if _env_flag("KALSHI_DISCOVER_TICKERS", True):
        try:
            from ingest.discovery import discover_kxbtc_tickers, discover_tickers
        except Exception as e:
            logging.warning("Auto-discovery module unavailable (%s); using manual KALSHI_TICKERS fallback.", e)

    async def _run_discovery(reference_spot: float | None = None):
        if discover_kxbtc_tickers is None:
            return None
        ref_spot = reference_spot
        if ref_spot is None:
            ref_spot_raw = os.getenv("KALSHI_DISCOVERY_SPOT")
            if ref_spot_raw:
                ref_spot = float(ref_spot_raw)
            else:
                mock_spot_raw = os.getenv("MOCK_SPOT_START")
                if mock_spot_raw:
                    ref_spot = float(mock_spot_raw)

        # Multi-family + runway-aware discovery
        families_raw = os.getenv("KALSHI_DISCOVERY_FAMILIES", os.getenv("KALSHI_DISCOVERY_SERIES", "KXBTC"))
        families = [f.strip() for f in families_raw.split(",") if f.strip()]
        min_runway_sec = int(os.getenv("BASELINE_MIN_RUNWAY_SEC", "0"))
        max_close_min = int(os.getenv("KALSHI_DISCOVERY_MAX_CLOSE_MIN", "180"))
        # If runway requires more than max_close, bump max_close up
        if min_runway_sec > 0:
            needed_max = int(min_runway_sec / 60) + 60  # runway + 1h buffer
            max_close_min = max(max_close_min, needed_max)

        try:
            return await asyncio.to_thread(
                discover_tickers,
                rest_base_url=rest_base_url,
                series_tickers=families,
                desired_count=int(os.getenv("KALSHI_DISCOVERY_COUNT", "3")),
                min_close_min=int(os.getenv("KALSHI_DISCOVERY_MIN_CLOSE_MIN", "30")),
                max_close_min=max_close_min,
                min_runway_sec=min_runway_sec,
                reference_spot=ref_spot,
                max_pages=int(os.getenv("KALSHI_DISCOVERY_MAX_PAGES", "6")),
            )
        except Exception:
            # Fallback to legacy single-series
            return await asyncio.to_thread(
                discover_kxbtc_tickers,
                rest_base_url=rest_base_url,
                series_ticker=os.getenv("KALSHI_DISCOVERY_SERIES", "KXBTC"),
                desired_count=int(os.getenv("KALSHI_DISCOVERY_COUNT", "3")),
                min_close_min=int(os.getenv("KALSHI_DISCOVERY_MIN_CLOSE_MIN", "30")),
                max_close_min=int(os.getenv("KALSHI_DISCOVERY_MAX_CLOSE_MIN", "180")),
                reference_spot=ref_spot,
                max_pages=int(os.getenv("KALSHI_DISCOVERY_MAX_PAGES", "6")),
            )

    if discover_kxbtc_tickers is not None:
        try:
            result = await _run_discovery()
            if result and result.tickers:
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

    # If using manual tickers and no close_ts yet, try to fetch it via REST
    if tickers and not discovered_close_ts_ms:
        try:
            import requests as _req
            for t in tickers[:1]:  # Only need one ticker to get the ladder close time
                resp = _req.get(f"{rest_base_url}/trade-api/v2/markets/{t}", timeout=10)
                if resp.status_code == 200:
                    mkt = resp.json().get("market", {})
                    from ingest.discovery import _parse_time_to_ms
                    close_ms = _parse_time_to_ms(mkt.get("close_time") or mkt.get("close_ts"))
                    if close_ms > 0:
                        discovered_close_ts_ms = close_ms
                        close_iso = datetime.fromtimestamp(close_ms / 1000.0, tz=timezone.utc).isoformat()
                        logging.info("Fetched close_ts for manual ticker %s: %s", t, close_iso)
                break
        except Exception as e:
            logging.warning("Could not fetch close_ts for manual tickers: %s", e)

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
    engine.risk.spot_heartbeat_ms = int(os.getenv("SPOT_HEARTBEAT_MS", str(engine.risk.spot_heartbeat_ms)))
    engine.risk.kalshi_heartbeat_ms = int(os.getenv("KALSHI_HEARTBEAT_MS", str(engine.risk.kalshi_heartbeat_ms)))
    engine.risk.auto_recover_ms = int(os.getenv("RISK_AUTO_RECOVER_MS", str(engine.risk.auto_recover_ms)))
    engine.min_quote_tte_ms = int(os.getenv("MIN_QUOTE_TTE_MS", str(engine.min_quote_tte_ms)))
    if os.getenv("EWMA_DECAY_FACTOR"):
        engine.quant.decay_factor = float(os.getenv("EWMA_DECAY_FACTOR"))
    engine.quant.half_life_seconds = float(os.getenv("VOL_HALF_LIFE_SEC", str(engine.quant.half_life_seconds)))
    engine.edge_horizon_ms = int(os.getenv("EDGE_HORIZON_MS", str(engine.edge_horizon_ms)))
    engine.edge_tolerance_ms = int(os.getenv("EDGE_TOLERANCE_MS", str(engine.edge_tolerance_ms)))
    engine.edge_slippage_buffer_cents = float(os.getenv("EDGE_SLIPPAGE_BUFFER_CENTS", str(engine.edge_slippage_buffer_cents)))
    engine.edge_governor_window = int(os.getenv("SPREAD_GOV_WINDOW_FILLS", str(engine.edge_governor_window)))
    engine.edge_governor_min_fills = int(os.getenv("SPREAD_GOV_MIN_FILLS", str(engine.edge_governor_min_fills)))
    engine.edge_governor_cooldown_ms = int(os.getenv("SPREAD_GOV_COOLDOWN_MS", str(engine.edge_governor_cooldown_ms)))
    engine.edge_governor_bootstrap_samples = int(
        os.getenv("SPREAD_GOV_BOOTSTRAP_SAMPLES", str(engine.edge_governor_bootstrap_samples))
    )
    engine.edge_governor_tighten_lcb_cents = float(
        os.getenv("SPREAD_GOV_TIGHTEN_LCB_CENTS", str(engine.edge_governor_tighten_lcb_cents))
    )
    engine.edge_governor_widen_ucb_cents = float(
        os.getenv("SPREAD_GOV_WIDEN_UCB_CENTS", str(engine.edge_governor_widen_ucb_cents))
    )
    engine.edge_governor_hysteresis_cents = float(
        os.getenv("SPREAD_GOV_HYSTERESIS_CENTS", str(engine.edge_governor_hysteresis_cents))
    )
    engine.governor_toxicity_spike = float(os.getenv("SPREAD_GOV_TOXICITY_SPIKE", str(engine.governor_toxicity_spike)))
    engine.governor_toxicity_low = float(os.getenv("SPREAD_GOV_TOXICITY_LOW", str(engine.governor_toxicity_low)))
    engine.governor_step_up = float(os.getenv("SPREAD_GOV_STEP_UP", str(engine.governor_step_up)))
    engine.governor_step_down = float(os.getenv("SPREAD_GOV_STEP_DOWN", str(engine.governor_step_down)))
    engine.governor_size_down_streak = int(os.getenv("SPREAD_GOV_SIZE_DOWN_STREAK", str(engine.governor_size_down_streak)))
    engine.governor_size_up_streak = int(os.getenv("SPREAD_GOV_SIZE_UP_STREAK", str(engine.governor_size_up_streak)))
    engine.governor_spread_mult_min = float(os.getenv("SPREAD_GOV_MULT_MIN", str(engine.governor_spread_mult_min)))
    engine.governor_spread_mult_max = float(os.getenv("SPREAD_GOV_MULT_MAX", str(engine.governor_spread_mult_max)))
    engine.quoter.vol_spread_mult = float(os.getenv("VOL_SPREAD_MULT", str(engine.quoter.vol_spread_mult)))
    engine.base_quote_size = int(os.getenv("QUOTE_SIZE_BASE", str(engine.base_quote_size)))
    engine.quote_size = int(os.getenv("QUOTE_SIZE_INIT", str(engine.base_quote_size)))
    engine.min_quote_size = int(os.getenv("QUOTE_SIZE_MIN", str(engine.min_quote_size)))
    engine.max_quote_size = int(os.getenv("QUOTE_SIZE_MAX", str(engine.max_quote_size)))
    engine.quote_size = max(engine.min_quote_size, min(engine.max_quote_size, engine.quote_size))
    engine.market_min_trades_per_hour = int(os.getenv("MARKET_MIN_TRADES_PER_HOUR", str(engine.market_min_trades_per_hour)))
    engine.market_min_top_depth = int(os.getenv("MARKET_MIN_TOP_DEPTH", str(engine.market_min_top_depth)))
    engine.market_max_book_age_ms = int(os.getenv("MARKET_MAX_BOOK_AGE_MS", str(engine.market_max_book_age_ms)))
    engine.exclude_last_minutes = int(os.getenv("EXCLUDE_LAST_MINUTES", str(engine.exclude_last_minutes)))
    engine.profit_kpi_min_fills = int(os.getenv("PROFIT_KPI_MIN_FILLS", str(engine.profit_kpi_min_fills)))
    engine.market_rank_enabled = _env_flag("MARKET_RANK_ENABLED", engine.market_rank_enabled)
    engine.market_rank_interval_ms = int(os.getenv("MARKET_RANK_INTERVAL_MS", str(engine.market_rank_interval_ms)))
    engine.market_rank_window_ms = int(os.getenv("MARKET_RANK_WINDOW_MS", str(engine.market_rank_window_ms)))
    engine.market_rank_min_fills = int(os.getenv("MARKET_RANK_MIN_FILLS", str(engine.market_rank_min_fills)))
    
    # Inject fallback close_ts for weekend/dead environments where WS Metadata doesn't stream
    if discovered_close_ts_ms > 0:
        for t in tickers:
            engine.ticker_close_ts[t] = discovered_close_ts_ms
    engine.market_rank_top_n = int(os.getenv("MARKET_RANK_TOP_N", str(engine.market_rank_top_n)))
    engine.market_rank_toxicity_weight = float(os.getenv("MARKET_RANK_TOXICITY_WEIGHT", str(engine.market_rank_toxicity_weight)))
    engine.market_rank_ci_weight = float(os.getenv("MARKET_RANK_CI_WEIGHT", str(engine.market_rank_ci_weight)))

    engine.realism_mode = os.getenv("PAPER_REALISM_MODE", engine.realism_mode).strip().lower()
    if engine.realism_mode == "pessimistic":
        default_latency = 300
        default_cancel_lag = 300
        default_haircut = 0.60
    elif engine.realism_mode == "off":
        default_latency = 0
        default_cancel_lag = 0
        default_haircut = 1.00
    else:
        default_latency = 100
        default_cancel_lag = 100
        default_haircut = 0.80
    engine.realism_order_latency_ms = int(os.getenv("PAPER_REALISM_ORDER_LATENCY_MS", str(default_latency)))
    engine.realism_cancel_lag_ms = int(os.getenv("PAPER_REALISM_CANCEL_LAG_MS", str(default_cancel_lag)))
    engine.realism_queue_haircut = float(os.getenv("PAPER_REALISM_QUEUE_HAIRCUT", str(default_haircut)))
    
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
        "Risk warmup_samples=%s kalshi_heartbeat_ms=%s auto_recover_ms=%s min_quote_tte_ms=%s "
        "vol_half_life_sec=%s ewma_decay_factor=%s spread_mult=%.2f quote_size=%s realism=%s rank=%s mock_spot_start=%s",
        engine.risk.warmup_samples,
        engine.risk.kalshi_heartbeat_ms,
        engine.risk.auto_recover_ms,
        engine.min_quote_tte_ms,
        engine.quant.half_life_seconds,
        engine.quant.decay_factor,
        engine.quoter.vol_spread_mult,
        engine.quote_size,
        engine.realism_mode,
        engine.market_rank_enabled,
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
        "vol_half_life_sec": engine.quant.half_life_seconds,
        "ewma_decay_factor": engine.quant.decay_factor,
        "vol_spread_mult": engine.quoter.vol_spread_mult,
        "quote_size": engine.quote_size,
        "edge_horizon_ms": engine.edge_horizon_ms,
        "edge_tolerance_ms": engine.edge_tolerance_ms,
        "edge_slippage_buffer_cents": engine.edge_slippage_buffer_cents,
        "spread_governor_window_fills": engine.edge_governor_window,
        "spread_governor_min_fills": engine.edge_governor_min_fills,
        "spread_governor_cooldown_ms": engine.edge_governor_cooldown_ms,
        "spread_governor_bootstrap_samples": engine.edge_governor_bootstrap_samples,
        "spread_governor_tighten_lcb_cents": engine.edge_governor_tighten_lcb_cents,
        "spread_governor_widen_ucb_cents": engine.edge_governor_widen_ucb_cents,
        "spread_governor_hysteresis_cents": engine.edge_governor_hysteresis_cents,
        "spread_governor_toxicity_spike": engine.governor_toxicity_spike,
        "spread_governor_toxicity_low": engine.governor_toxicity_low,
        "spread_governor_step_up": engine.governor_step_up,
        "spread_governor_step_down": engine.governor_step_down,
        "spread_governor_size_down_streak": engine.governor_size_down_streak,
        "spread_governor_size_up_streak": engine.governor_size_up_streak,
        "profit_kpi_min_fills": engine.profit_kpi_min_fills,
        "spread_governor_mult_min": engine.governor_spread_mult_min,
        "spread_governor_mult_max": engine.governor_spread_mult_max,
        "market_min_trades_per_hour": engine.market_min_trades_per_hour,
        "market_min_top_depth": engine.market_min_top_depth,
        "market_max_book_age_ms": engine.market_max_book_age_ms,
        "exclude_last_minutes": engine.exclude_last_minutes,
        "market_rank_enabled": engine.market_rank_enabled,
        "market_rank_interval_ms": engine.market_rank_interval_ms,
        "market_rank_window_ms": engine.market_rank_window_ms,
        "market_rank_min_fills": engine.market_rank_min_fills,
        "market_rank_top_n": engine.market_rank_top_n,
        "market_rank_toxicity_weight": engine.market_rank_toxicity_weight,
        "market_rank_ci_weight": engine.market_rank_ci_weight,
        "paper_realism_mode": engine.realism_mode,
        "paper_realism_order_latency_ms": engine.realism_order_latency_ms,
        "paper_realism_cancel_lag_ms": engine.realism_cancel_lag_ms,
        "paper_realism_queue_haircut": engine.realism_queue_haircut,
        "health_stale_sec": int(
            os.getenv("HEALTH_STALE_SEC", str(max(1, (engine.risk.kalshi_heartbeat_ms + 999) // 1000)))
        ),
        "risk_auto_recover_ms": engine.risk.auto_recover_ms,
        "min_quote_tte_ms": engine.min_quote_tte_ms,
        "mock_spot_enabled": use_mock_spot,
        "mock_spot_start": spot_start_price if use_mock_spot else None,
        "discovery_window_min_close_min": int(os.getenv("KALSHI_DISCOVERY_MIN_CLOSE_MIN", "30")),
        "discovery_window_max_close_min": int(os.getenv("KALSHI_DISCOVERY_MAX_CLOSE_MIN", "180")),
        "last_spot": None,
        "is_risk_off": False,
        "risk_off_reason": None,
        "last_kalshi_ts": 0,
    }
    _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)

    async def _status_update_loop():
        while True:
            await asyncio.sleep(5)
            try:
                runtime_status["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
                runtime_status["last_spot"] = engine.quant.last_spot if engine.quant.last_spot > 0 else None
                runtime_status["is_risk_off"] = engine.risk.is_risk_off
                runtime_status["risk_off_reason"] = engine.risk.risk_off_reason if engine.risk.is_risk_off else None
                runtime_status["last_kalshi_ts"] = engine.risk.last_kalshi_ts
                
                # Add throughput metrics
                now_ms = int(time.time() * 1000)
                recent_trades = engine.recent_trade_count(window_ms=60000, current_ts=now_ms)
                runtime_status["trades_per_min"] = recent_trades
                
                # Add orderbook status for debugging stalls
                ob_status = {}
                for t in tickers:
                    ob = engine.orderbooks.get(t)
                    if ob:
                        bbo = ob.get_yes_bbo()
                        ob_status[t] = {
                            "is_valid": ob.is_valid,
                            "yes_bid": bbo.bid,
                            "yes_ask": bbo.ask,
                            "mid": bbo.mid,
                            "last_update": engine.last_book_update_ts.get(t, 0)
                        }
                runtime_status["orderbooks"] = ob_status
                
                # Add persistence metrics
                runtime_status["write_counts"] = dict(data_store.write_counts)
                
                _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)
            except Exception as e:
                logging.error(f"Error in status update loop: {e}")

    research_fixed_ladder = _env_flag("RESEARCH_FIXED_LADDER", False)
    failover_enabled = (
        _env_flag("DEAD_MARKET_FAILOVER_ENABLED", True)
        and (discover_kxbtc_tickers is not None)
        and (not research_fixed_ladder)
    )
    failover_check_sec = int(os.getenv("DEAD_MARKET_CHECK_SEC", "30"))
    failover_window_sec = int(os.getenv("DEAD_MARKET_WINDOW_SEC", "120"))
    failover_min_trades = int(os.getenv("DEAD_MARKET_MIN_TRADES", "1"))
    failover_cooldown_sec = int(os.getenv("DEAD_MARKET_COOLDOWN_SEC", "120"))
    runtime_status["dead_market_failover_enabled"] = failover_enabled
    runtime_status["research_fixed_ladder"] = research_fixed_ladder
    runtime_status["dead_market_check_sec"] = failover_check_sec
    runtime_status["dead_market_window_sec"] = failover_window_sec
    runtime_status["dead_market_min_trades"] = failover_min_trades
    runtime_status["dead_market_cooldown_sec"] = failover_cooldown_sec

    # Ensure engine has metadata to quote even if WS MarketMetadataEvents are delayed/missing (REST fallback)
    if discovered_close_ts_ms:
        for t in tickers:
            engine.ticker_close_ts[t] = int(discovered_close_ts_ms)
            if t not in engine.ticker_to_strike:
                strike, _ = engine._parse_contract_from_ticker(t)
                if strike:
                    engine.ticker_to_strike[t] = strike

    _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)

    async def _rotate_tickers(new_tickers: list[str], reason: str, result=None):
        nonlocal tickers, discovered_close_ts_ms, discovered_ref_spot
        if not new_tickers:
            return
        if set(new_tickers) == set(tickers):
            return

        logging.warning("Rotating tickers due to %s: old=%s new=%s", reason, tickers, new_tickers)
        tickers = list(new_tickers)
        engine.set_active_tickers(tickers)
        await kalshi_ws.update_tickers(tickers)

        if result is not None:
            discovered_close_ts_ms = result.close_ts_ms
            discovered_ref_spot = result.reference_spot
            runtime_status["discovery_used"] = True
            runtime_status["discovered_close_ts_ms"] = discovered_close_ts_ms
            runtime_status["discovered_close_iso"] = (
                datetime.fromtimestamp(discovered_close_ts_ms / 1000.0, tz=timezone.utc).isoformat()
                if discovered_close_ts_ms
                else None
            )
            runtime_status["discovered_reference_spot"] = discovered_ref_spot

        runtime_status["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        runtime_status["tickers"] = tickers
        _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)

    async def _dead_market_failover_loop():
        if not failover_enabled:
            return

        last_failover_ms = 0
        while True:
            await asyncio.sleep(max(5, failover_check_sec))
            now_ms = int(time.time() * 1000)
            recent_trades = engine.recent_trade_count(
                window_ms=max(1, failover_window_sec) * 1000,
                current_ts=now_ms,
                tickers=tickers,
            )
            if recent_trades >= failover_min_trades:
                continue
            if (now_ms - last_failover_ms) < (failover_cooldown_sec * 1000):
                continue

            logging.warning(
                "Dead-market failover trigger: trades=%s threshold=%s window_sec=%s tickers=%s",
                recent_trades,
                failover_min_trades,
                failover_window_sec,
                tickers,
            )
            ref_spot = engine.quant.last_spot if engine.quant.last_spot > 0 else None
            try:
                result = await _run_discovery(reference_spot=ref_spot)
            except Exception as e:
                logging.warning("Dead-market failover discovery failed: %s", e)
                continue
            if not result or not result.tickers:
                logging.warning("Dead-market failover: no replacement tickers returned.")
                continue

            await _rotate_tickers(
                new_tickers=result.tickers,
                reason=f"dead-market ({recent_trades} trades/{failover_window_sec}s)",
                result=result,
            )
            last_failover_ms = now_ms

    # Run everything concurrently
    tasks = [
        engine.start(),
        kalshi_ws.start(),
        _status_update_loop(),
    ]
    if use_mock_spot:
        tasks.append(spot_ws.start())
    else:
        logging.warning("Mock spot feed is disabled. Warmup gate may block quoting unless an external spot feed is wired.")
    if failover_enabled:
        tasks.append(_dead_market_failover_loop())
    elif _env_flag("DEAD_MARKET_FAILOVER_ENABLED", True):
        if research_fixed_ladder:
            logging.info("Dead-market failover disabled because RESEARCH_FIXED_LADDER=true.")
        else:
            logging.warning("Dead-market failover enabled but discovery is unavailable.")

    # Auto-pause/re-arm loop for fixed-ladder mode
    rearm_tte_threshold_sec = int(os.getenv("REARM_TTE_THRESHOLD_SEC", "2700"))  # 45 min default
    rearm_check_sec = int(os.getenv("REARM_CHECK_SEC", "120"))

    async def _auto_pause_rearm_loop():
        nonlocal tickers, discovered_close_ts_ms, discovered_ref_spot
        paused = False
        while True:
            await asyncio.sleep(60)
            now_ms = int(time.time() * 1000)

            # Check TTE of current ladder — use discovered close or engine metadata
            effective_close_ms = discovered_close_ts_ms
            if (not effective_close_ms or effective_close_ms <= 0) and hasattr(engine, 'market_meta'):
                # Try to get earliest close from engine's tracked metadata
                close_times = []
                for t in tickers:
                    meta = engine.market_meta.get(t)
                    if meta and hasattr(meta, 'close_ts') and meta.close_ts and meta.close_ts > 0:
                        close_times.append(meta.close_ts)
                if close_times:
                    effective_close_ms = min(close_times)

            if effective_close_ms and effective_close_ms > 0:
                tte_sec = (effective_close_ms - now_ms) / 1000.0
            else:
                tte_sec = float("inf")

            if tte_sec < rearm_tte_threshold_sec and not paused:
                logging.warning(
                    "AUTO-PAUSE: TTE=%.0fs < threshold=%ds. Pausing quoting, keeping ingest alive.",
                    tte_sec, rearm_tte_threshold_sec,
                )
                engine.risk.trigger_risk_off("auto-pause: TTE below threshold", recoverable=True)
                paused = True
                runtime_status["auto_paused"] = True
                runtime_status["auto_pause_reason"] = f"TTE={tte_sec:.0f}s < {rearm_tte_threshold_sec}s"
                _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)

            if paused:
                # Periodically try to re-arm with a new ladder
                await asyncio.sleep(max(30, rearm_check_sec))
                ref_spot = engine.quant.last_spot if engine.quant.last_spot > 0 else None
                try:
                    result = await _run_discovery(reference_spot=ref_spot)
                except Exception as e:
                    logging.warning("Re-arm discovery failed: %s", e)
                    continue
                if result and result.tickers and set(result.tickers) != set(tickers):
                    logging.info(
                        "RE-ARM: Found new ladder with runway. tickers=%s close=%s",
                        result.tickers,
                        datetime.fromtimestamp(result.close_ts_ms / 1000.0, tz=timezone.utc).isoformat()
                        if result.close_ts_ms else "unknown",
                    )
                    await _rotate_tickers(
                        new_tickers=result.tickers,
                        reason="auto-rearm (found ladder with runway)",
                        result=result,
                    )
                    engine.risk.clear_risk_off("auto-rearm")
                    paused = False
                    runtime_status["auto_paused"] = False
                    runtime_status["auto_pause_reason"] = None
                    _safe_write_runtime_status("data_warehouse/runtime/status.json", runtime_status)
                else:
                    logging.info("Re-arm: no new ladder with runway found. Still paused.")

    if research_fixed_ladder:
        tasks.append(_auto_pause_rearm_loop())

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down MVP")
