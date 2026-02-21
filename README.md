# Quant Demo: Kalshi PM MM MVP

Event-driven Python MVP for a prediction-market maker simulation:
- Ingest market data events
- Maintain local orderbook and volatility state
- Generate inventory-aware quotes
- Simulate queue-ahead fills
- Persist events to Parquet
- Visualize activity in Streamlit

## Repo Layout
- `main.py`: process entrypoint, starts engine + ingest feeds
- `core/`: shared types and PAVA utility
- `ingest/`: Kalshi ingest adapter + mock spot feed
- `engine/`: orderbook, quant, quoter, risk, deterministic simulator loop
- `data/store.py`: buffered parquet writer
- `dashboard/app.py`: Streamlit dashboard over parquet data

## Runtime Behavior
- Kalshi consumer supports `KALSHI_MODE=real|auto|synthetic`.
- `real`: requires API auth env vars and streams real snapshots/WS updates.
- `auto`: uses real mode when creds exist, otherwise falls back to synthetic snapshots/trades.
- `synthetic`: no external Kalshi dependency; emits synthetic snapshots/trades only.
- Mock BTC spot feed is enabled by default and drives heartbeat/warmup.

## Environment
- `KALSHI_MODE` (default: `auto`)
- `KALSHI_ENV` (`demo` or `prod`, default: `demo`)
- `KALSHI_DISCOVER_TICKERS` (`true`/`false`, default: `true`)
- `KALSHI_DISCOVERY_SERIES` (default: `KXBTC`)
- `KALSHI_DISCOVERY_COUNT` (default: `3`)
- `KALSHI_DISCOVERY_MIN_CLOSE_MIN` (default: `30`)
- `KALSHI_DISCOVERY_MAX_CLOSE_MIN` (default: `180`)
- `KALSHI_DISCOVERY_MAX_PAGES` (default: `6`)
- `KALSHI_DISCOVERY_SPOT` (optional reference spot for ATM selection)
- `KALSHI_TICKERS` (manual fallback comma-separated tickers)
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH` or `KALSHI_PRIVATE_KEY_PEM`
- `KALSHI_PRIVATE_KEY_PASSWORD` (optional)
- `ENABLE_MOCK_SPOT` (`true`/`false`, default: `true`)
- `MOCK_SPOT_START` (default: `110000`)
- `MOCK_SPOT_INTERVAL_MS` (default: `1000`)
- `WARMUP_SAMPLES` (default: `300`)
- `KALSHI_HEARTBEAT_MS` (default: `30000`)
- `RISK_AUTO_RECOVER_MS` (default: `10000`, auto-clear recoverable risk-off after healthy feeds)
- `MIN_QUOTE_TTE_MS` (default: `180000`, 3 minutes)
- `DEAD_MARKET_FAILOVER_ENABLED` (default: `true`)
- `DEAD_MARKET_CHECK_SEC` (default: `30`)
- `DEAD_MARKET_WINDOW_SEC` (default: `120`)
- `DEAD_MARKET_MIN_TRADES` (default: `1`)
- `DEAD_MARKET_COOLDOWN_SEC` (default: `120`)

Note: avoid expired tickers (e.g. `BTC-24DEC31-*` are expired as of 2026-02-20). Discovery selects currently active `KXBTC` ladders automatically.

## Setup
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run
Terminal 1:
```bash
# Optional for real Kalshi:
# set KALSHI_MODE=real
# set KALSHI_ENV=demo
# set KALSHI_API_KEY_ID=...
# set KALSHI_PRIVATE_KEY_PATH=C:\path\to\kalshi-private-key.pem
python main.py
```

Terminal 2:
```bash
streamlit run dashboard/app.py
```

## What To Expect
- If discovery is enabled, startup logs will print `Auto-discovery selected ...` with close time and chosen tickers.
- Logs should show either:
  - `Fetched orderbook snapshot ...`, or
  - `Seeded synthetic snapshot ... (auth-bypass mode)`
- Spot/trade/fill parquet files are written under `data_warehouse/`.
- Warmup gate is enabled by default:
  - `warmup_samples = 300` with 1 second spot interval is about 5 minutes before quoting starts.

## Data Paths
- `data_warehouse/spot/...`
- `data_warehouse/trade/...`
- `data_warehouse/paper_fill/...`
- `data_warehouse/orderbook_delta/...`
- `data_warehouse/lifecycle/...`
- `data_warehouse/runtime/status.json` (startup run config used by dashboard health panel)
