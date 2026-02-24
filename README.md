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
- `HEALTH_STALE_SEC` (optional dashboard threshold; defaults to `ceil(KALSHI_HEARTBEAT_MS/1000)`)
- `VOL_HALF_LIFE_SEC` (default: `300`, EWMA half-life in seconds for volatility smoothing)
- `EWMA_DECAY_FACTOR` (optional fixed lambda override; if set, half-life setting is ignored)
- `RISK_AUTO_RECOVER_MS` (default: `10000`, auto-clear recoverable risk-off after healthy feeds)
- `MIN_QUOTE_TTE_MS` (default: `180000`, 3 minutes)
- `EXCLUDE_LAST_MINUTES` (default: `3`, extra no-quote buffer near expiry)
- `PROFIT_KPI_MIN_FILLS` (default: `50`, minimum samples before profitability pass/fail)
- `VOL_SPREAD_MULT` (default: `5.0`, initial spread multiplier)
- `QUOTE_SIZE_BASE`, `QUOTE_SIZE_INIT`, `QUOTE_SIZE_MIN`, `QUOTE_SIZE_MAX`
- `EDGE_HORIZON_MS` (default: `10000`, horizon for fee-adjusted edge metric)
- `EDGE_TOLERANCE_MS` (default: `2000`, sampling tolerance around horizon)
- `EDGE_SLIPPAGE_BUFFER_CENTS` (default: `0.20`)
- `SPREAD_GOV_WINDOW_FILLS` (default: `30`)
- `SPREAD_GOV_MIN_FILLS` (default: `50`, minimum samples before any tightening/widening)
- `SPREAD_GOV_COOLDOWN_MS` (default: `5000`)
- `SPREAD_GOV_BOOTSTRAP_SAMPLES` (default: `300`)
- `SPREAD_GOV_TIGHTEN_LCB_CENTS` (default: `0.20`, tighten only if lower CI bound exceeds this)
- `SPREAD_GOV_WIDEN_UCB_CENTS` (default: `0.0`, widen if upper CI bound is below this)
- `SPREAD_GOV_HYSTERESIS_CENTS` (default: `0.05`)
- `SPREAD_GOV_TOXICITY_SPIKE` (default: `1.0`)
- `SPREAD_GOV_TOXICITY_LOW` (default: `0.25`)
- `SPREAD_GOV_STEP_UP`, `SPREAD_GOV_STEP_DOWN`
- `SPREAD_GOV_SIZE_DOWN_STREAK`, `SPREAD_GOV_SIZE_UP_STREAK`
- `SPREAD_GOV_MULT_MIN`, `SPREAD_GOV_MULT_MAX`
- `MARKET_MIN_TRADES_PER_HOUR` (default: `0`)
- `MARKET_MIN_TOP_DEPTH` (default: `5`)
- `MARKET_MAX_BOOK_AGE_MS` (default: `20000`)
- `MARKET_RANK_ENABLED` (default: `false`, rank by edge/hour minus toxicity/CI penalty)
- `MARKET_RANK_INTERVAL_MS`, `MARKET_RANK_WINDOW_MS`, `MARKET_RANK_MIN_FILLS`
- `MARKET_RANK_TOP_N`, `MARKET_RANK_TOXICITY_WEIGHT`, `MARKET_RANK_CI_WEIGHT`
- `PAPER_REALISM_MODE` (`off|base|pessimistic`)
- `PAPER_REALISM_ORDER_LATENCY_MS`, `PAPER_REALISM_CANCEL_LAG_MS`, `PAPER_REALISM_QUEUE_HAIRCUT`
- `DEAD_MARKET_FAILOVER_ENABLED` (default: `true`)
- `DEAD_MARKET_CHECK_SEC` (default: `30`)
- `DEAD_MARKET_WINDOW_SEC` (default: `120`)
- `DEAD_MARKET_MIN_TRADES` (default: `1`)
- `DEAD_MARKET_COOLDOWN_SEC` (default: `120`)
- `RESEARCH_FIXED_LADDER` (default: `false`, disables dead-market ticker rotation)

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
- `data_warehouse/quote_audit/...`
- `data_warehouse/orderbook_delta/...`
- `data_warehouse/lifecycle/...`
- `data_warehouse/market_meta/...`
- `data_warehouse/edge_metric/...`
- `data_warehouse/runtime/status.json` (startup run config used by dashboard health panel)

## Research Utilities
- `research/markouts.py`: reusable exchange-timestamp markout computation from fills + orderbook deltas.
- `research/loaders.py`: parquet channel loaders for offline analysis scripts.
- `research/replay.py`: deterministic offline replay + quoter parameter sweep.
- `research/compare_runs.py`: compare persisted replay/sweep runs (metrics/config/grid deltas).

`paper_fill` rows now include quote-context fields for research:
- `queue_ahead_at_fill`
- `time_since_quote_ms`
- `fair_prob_at_quote`
- `sigma_at_quote`
- `tte_ms_at_quote`

### Replay Examples
Single replay summary:
```bash
python -m research.replay --base-dir data_warehouse --warmup-samples 10 --kalshi-heartbeat-ms 120000 --spot-heartbeat-ms 120000
```

Bounded replay with ticker filter:
```bash
python -m research.replay --tickers KXBTC-26FEB2117-B68750 --start-ts-ms 1771705800000 --end-ts-ms 1771705877000
```

Parameter sweep (writes CSV):
```bash
python -m research.replay --tickers KXBTC-26FEB2117-B68750 --start-ts-ms 1771705800000 --end-ts-ms 1771705877000 --sweep vol_spread_mult=4,5,6 --sweep min_quote_lifetime_ms=500,1000 --out-csv research_sweep.csv
```

Persist replay artifacts by run-id:
```bash
python -m research.replay --tickers KXBTC-26FEB2117-B68750 --start-ts-ms 1771705800000 --end-ts-ms 1771705877000 --persist --run-root research_runs --run-id demo_replay_01
```

Compare two replay runs:
```bash
python -m research.compare_runs --run-root research_runs --base-run demo_replay_01 --candidate-run demo_replay_02
```

Compare two sweep runs:
```bash
python -m research.compare_runs --run-root research_runs --base-run demo_sweep_01 --candidate-run demo_sweep_02 --score markout_10s_total_usd
```
