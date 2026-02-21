# Repository Guidelines

## Project Structure & Module Organization
This is an event-driven Python MVP for Kalshi market making.

- `main.py`: app entrypoint; wires ingest, engine loop, and runtime config.
- `core/`: shared domain types (events, enums) and utilities.
- `ingest/`: Kalshi websocket/REST ingestion and ticker discovery logic.
- `engine/`: orderbook reconstruction, quant model, quoter, risk engine, simulator loop.
- `data/store.py`: buffered Parquet writer with partitioned output.
- `dashboard/app.py`: Streamlit dashboard (DuckDB + Parquet reads).
- `data_warehouse/`: runtime artifacts (spot, trades, orderbook deltas, paper fills, status).

## Build, Test, and Development Commands
- `python -m venv .venv && .venv\Scripts\activate`: create/activate local environment.
- `pip install -r requirements.txt`: install dependencies.
- `python main.py`: start engine + ingestion pipeline.
- `streamlit run dashboard/app.py`: launch dashboard on `localhost:8501`.
- `python -m compileall main.py engine ingest dashboard data core`: quick syntax/import sanity check.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation.
- Use `snake_case` for functions/variables/files; `PascalCase` for classes/dataclasses.
- Keep modules focused (ingest vs engine vs storage vs UI boundaries).
- Prefer explicit typed dataclasses for events (`*Event` suffix) and enums for side/state fields.
- Keep logging actionable and concise; include ticker/context in operational logs.

## Testing Guidelines
There is no formal test framework in this repo yet. Until `pytest` tests are added:

- Run `python -m compileall ...` before opening a PR.
- Smoke test both processes:
  1. `python main.py`
  2. `streamlit run dashboard/app.py`
- Validate that new changes do not break Parquet reads/writes in `data_warehouse/`.
- For strategy/risk changes, include a short reproducible scenario in the PR description.

## Commit & Pull Request Guidelines
- Current history uses short imperative summaries (example: `Initial Kalshi PM MVP Architecture`).
- Keep commits focused by concern (ingest, engine, dashboard, docs).
- PRs should include:
  - What changed and why
  - Config/env changes (`.env.example`, README)
  - Manual validation steps and outcomes
  - Dashboard screenshot(s) for UI-impacting updates

## Security & Configuration Tips
- Never commit secrets from `.env` or private key files (`*.pem`).
- Keep credentials in local env only (`KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`).
- Prefer demo mode (`KALSHI_ENV=demo`) when testing new behavior.
