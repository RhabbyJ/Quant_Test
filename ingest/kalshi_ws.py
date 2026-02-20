import asyncio
import base64
import json
import logging
import random
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional
from urllib.parse import urlparse

import requests
import websockets

from core.types import LifecycleEvent, OrderbookDeltaEvent, OrderbookSnapshotEvent, Side, TradeEvent


class KalshiWSConsumer:
    """
    Real Kalshi websocket + snapshot consumer with optional synthetic fallback.

    Modes:
    - "real": require API credentials and stream real data only.
    - "auto": use real data when credentials are configured, else synthetic mode.
    - "synthetic": always use synthetic snapshot/trade generation.
    """

    def __init__(
        self,
        uri: str,
        tickers: List[str],
        event_queue: asyncio.Queue,
        mode: str = "auto",
        rest_base_url: Optional[str] = None,
        api_key_id: Optional[str] = None,
        private_key_path: Optional[str] = None,
        private_key_pem: Optional[str] = None,
        private_key_password: Optional[str] = None,
        reconnect_initial_sec: float = 1.0,
        reconnect_max_sec: float = 30.0,
    ):
        self.uri = uri
        self.tickers = tickers
        self.event_queue = event_queue
        self.mode = (mode or "auto").lower()
        self.is_running = False

        self.reconnect_initial_sec = reconnect_initial_sec
        self.reconnect_max_sec = reconnect_max_sec

        self.api_key_id = (api_key_id or "").strip() or None
        self.private_key = self._load_private_key(private_key_path, private_key_pem, private_key_password)

        self.rest_base_url = rest_base_url or self._derive_rest_base_from_ws(uri)
        self._warned_fractional_contracts = False

    @staticmethod
    def _derive_rest_base_from_ws(ws_uri: str) -> str:
        parsed = urlparse(ws_uri)
        if not parsed.scheme or not parsed.netloc:
            return "https://demo-api.kalshi.co"
        rest_scheme = "https" if parsed.scheme in ("ws", "wss") else parsed.scheme
        return f"{rest_scheme}://{parsed.netloc}"

    def _load_private_key(self, path: Optional[str], pem_text: Optional[str], password: Optional[str]):
        raw_pem = None
        if pem_text:
            raw_pem = pem_text
        elif path:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw_pem = f.read()
            except OSError as e:
                raise RuntimeError(f"Unable to read Kalshi private key at {path}: {e}") from e

        if not raw_pem:
            return None

        try:
            from cryptography.hazmat.primitives import serialization
        except ImportError as e:
            raise RuntimeError("Missing dependency: cryptography. Install with `pip install cryptography`.") from e

        pwd = password.encode("utf-8") if password else None
        try:
            return serialization.load_pem_private_key(raw_pem.encode("utf-8"), password=pwd)
        except Exception as e:
            raise RuntimeError(f"Failed to parse Kalshi private key PEM: {e}") from e

    def _auth_enabled(self) -> bool:
        return bool(self.api_key_id and self.private_key)

    def _build_signed_headers(self, method: str, path: str) -> dict[str, str]:
        if not self._auth_enabled():
            return {}

        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except ImportError as e:
            raise RuntimeError("Missing dependency: cryptography. Install with `pip install cryptography`.") from e

        ts = str(int(time.time() * 1000))
        path_no_query = path.split("?", 1)[0]
        message = f"{ts}{method.upper()}{path_no_query}".encode("utf-8")

        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        }

    @staticmethod
    def _parse_exchange_ts_ms(raw_ts: Any, default_ms: int) -> int:
        if raw_ts is None:
            return default_ms

        if isinstance(raw_ts, (int, float)):
            if raw_ts < 10_000_000_000:
                return int(raw_ts * 1000)
            return int(raw_ts)

        if isinstance(raw_ts, str):
            s = raw_ts.strip()
            if not s:
                return default_ms
            if s.isdigit():
                val = int(s)
                if val < 10_000_000_000:
                    return val * 1000
                return val
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                return default_ms

        return default_ms

    def _to_int_contract(self, raw: Any) -> int:
        if raw is None:
            return 0

        if isinstance(raw, bool):
            return int(raw)

        if isinstance(raw, int):
            return raw

        if isinstance(raw, float):
            return int(raw)

        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return 0
            try:
                dec = Decimal(s)
                if dec != dec.to_integral_value() and not self._warned_fractional_contracts:
                    logging.warning("Fractional contracts detected from Kalshi feed; truncating to int for MVP simulator.")
                    self._warned_fractional_contracts = True
                return int(dec)
            except (InvalidOperation, ValueError):
                return 0

        try:
            return int(raw)
        except Exception:
            return 0

    def _get_contract_size(self, payload: dict[str, Any], int_key: str, fp_key: str) -> int:
        if int_key in payload and payload[int_key] is not None:
            return self._to_int_contract(payload[int_key])
        return self._to_int_contract(payload.get(fp_key))

    def _get_price_cents(self, payload: dict[str, Any]) -> int:
        for key in ("price", "yes_price", "yesPrice"):
            if key in payload and payload[key] is not None:
                try:
                    return int(payload[key])
                except (TypeError, ValueError):
                    continue
        return 0

    def _get_side(self, payload: dict[str, Any]) -> Side:
        raw = (payload.get("side") or payload.get("taker_side") or "yes").lower()
        return Side.YES_BID if raw == "yes" else Side.NO_BID

    def _normalize_levels(self, levels: Any):
        normalized = []
        if not levels:
            return normalized

        for level in levels:
            price = None
            size = None
            if isinstance(level, dict):
                price = level.get("price")
                size = level.get("size", level.get("quantity", level.get("qty")))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = level[0]
                size = level[1]

            if price is None or size is None:
                continue

            try:
                price_i = int(price)
            except (TypeError, ValueError):
                continue

            size_i = self._to_int_contract(size)
            if size_i > 0 and 0 <= price_i <= 100:
                normalized.append((price_i, size_i))

        return normalized

    async def _enqueue_snapshot(self, ticker: str, yes_bids, no_bids, ts_ms: int):
        event = OrderbookSnapshotEvent(
            exchange_ts=ts_ms,
            ingest_ts=ts_ms,
            ticker=ticker,
            yes_bids=yes_bids,
            no_bids=no_bids,
        )
        await self.event_queue.put(event)

    def _build_snapshot_headers(self, ticker: str) -> dict[str, str]:
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        return self._build_signed_headers("GET", path)

    async def _fetch_snapshot(self, ticker: str) -> bool:
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        url = f"{self.rest_base_url}{path}"
        ingest_ts = int(time.time() * 1000)
        headers = self._build_snapshot_headers(ticker) if self._auth_enabled() else {}

        try:
            resp = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            if resp.status_code != 200:
                logging.error("Snapshot fetch failed for %s: %s %s", ticker, resp.status_code, resp.text)
                return False

            body = resp.json()
            book = body.get("orderbook", body.get("book", {}))
            yes_bids = self._normalize_levels(book.get("yes"))
            no_bids = self._normalize_levels(book.get("no"))
            ts_ms = self._parse_exchange_ts_ms(book.get("ts") or body.get("ts"), ingest_ts)
            await self._enqueue_snapshot(ticker, yes_bids, no_bids, ts_ms)
            logging.info("Fetched orderbook snapshot for %s", ticker)
            return True
        except Exception as e:
            logging.error("Error fetching snapshot for %s: %s", ticker, e)
            return False

    async def _emit_synthetic_snapshot(self, ticker: str):
        ts = int(time.time() * 1000)
        yes_bids = [(49, 50), (48, 30), (47, 20)]
        no_bids = [(49, 50), (48, 30), (47, 20)]
        await self._enqueue_snapshot(ticker, yes_bids, no_bids, ts)
        logging.warning("Seeded synthetic snapshot for %s", ticker)

    async def _run_synthetic(self):
        for ticker in self.tickers:
            await self._emit_synthetic_snapshot(ticker)

        logging.warning("Running Kalshi synthetic mode: emitting synthetic trades.")
        while self.is_running:
            await asyncio.sleep(1.0)
            ts = int(time.time() * 1000)
            for ticker in self.tickers:
                trade = TradeEvent(
                    exchange_ts=ts,
                    ingest_ts=ts,
                    ticker=ticker,
                    price_cents=random.randint(45, 55),
                    size=random.randint(1, 5),
                    side=Side.YES_BID,
                    seq=None,
                )
                await self.event_queue.put(trade)

    async def _subscribe(self, websocket):
        sub_market_data = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": self.tickers,
            },
        }
        await websocket.send(json.dumps(sub_market_data))

        # market_lifecycle_v2 does not support market_ticker filters.
        sub_lifecycle = {
            "id": 2,
            "cmd": "subscribe",
            "params": {
                "channels": ["market_lifecycle_v2"],
            },
        }
        await websocket.send(json.dumps(sub_lifecycle))

    async def _bootstrap_snapshots(self):
        successes = 0
        for ticker in self.tickers:
            if await self._fetch_snapshot(ticker):
                successes += 1

        if successes == 0:
            logging.warning("No Kalshi snapshots loaded during bootstrap.")

    async def _run_real_stream(self):
        ws_path = urlparse(self.uri).path or "/trade-api/ws/v2"
        retry_delay = self.reconnect_initial_sec

        while self.is_running:
            try:
                await self._bootstrap_snapshots()

                headers = self._build_signed_headers("GET", ws_path)
                if not headers:
                    logging.warning("Connecting to Kalshi websocket without auth headers.")

                async with websockets.connect(
                    self.uri,
                    additional_headers=headers or None,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=8 * 1024 * 1024,
                ) as websocket:
                    logging.info("Connected to Kalshi websocket at %s", self.uri)
                    await self._subscribe(websocket)
                    retry_delay = self.reconnect_initial_sec

                    async for message in websocket:
                        if not self.is_running:
                            break
                        await self._handle_message(message)

            except Exception as e:
                logging.error("Kalshi WS stream error: %s", e)
                if not self.is_running:
                    break
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.reconnect_max_sec)

    async def start(self):
        self.is_running = True

        if self.mode not in {"real", "auto", "synthetic"}:
            raise ValueError(f"Unknown Kalshi mode: {self.mode}")

        if self.mode == "synthetic":
            await self._run_synthetic()
            return

        if self.mode == "real" and not self._auth_enabled():
            raise RuntimeError(
                "KALSHI_MODE=real requires auth credentials. "
                "Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM."
            )

        if self.mode == "auto" and not self._auth_enabled():
            logging.warning("No Kalshi API credentials found. Falling back to synthetic mode.")
            await self._run_synthetic()
            return

        await self._run_real_stream()

    async def _handle_message(self, msg: str):
        ingest_ts = int(time.time() * 1000)
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            return

        msg_type = data.get("type")
        payload = data.get("msg") if isinstance(data.get("msg"), dict) else data

        if msg_type == "subscribed" or msg_type == "ok":
            return

        if msg_type == "error":
            code = payload.get("code")
            message = payload.get("msg") or payload.get("message")
            logging.error("Kalshi websocket error [%s]: %s", code, message)
            return

        if msg_type == "orderbook_snapshot":
            ticker = payload.get("market_ticker") or payload.get("ticker") or "UNKNOWN"
            ts_ms = self._parse_exchange_ts_ms(payload.get("ts"), ingest_ts)
            yes_bids = self._normalize_levels(payload.get("yes"))
            no_bids = self._normalize_levels(payload.get("no"))
            await self._enqueue_snapshot(ticker, yes_bids, no_bids, ts_ms)
            return

        if msg_type == "orderbook_delta":
            ts_ms = self._parse_exchange_ts_ms(payload.get("ts"), ingest_ts)
            event = OrderbookDeltaEvent(
                exchange_ts=ts_ms,
                ingest_ts=ingest_ts,
                ticker=payload.get("market_ticker", "UNKNOWN"),
                price_cents=self._get_price_cents(payload),
                size=self._get_contract_size(payload, "delta", "delta_fp"),
                side=self._get_side(payload),
                seq=data.get("seq"),
            )
            await self.event_queue.put(event)
            return

        if msg_type == "trade":
            ts_ms = self._parse_exchange_ts_ms(payload.get("ts"), ingest_ts)
            event = TradeEvent(
                exchange_ts=ts_ms,
                ingest_ts=ingest_ts,
                ticker=payload.get("market_ticker", "UNKNOWN"),
                price_cents=self._get_price_cents(payload),
                size=self._get_contract_size(payload, "count", "count_fp"),
                side=self._get_side(payload),
                seq=data.get("seq"),
            )
            await self.event_queue.put(event)
            return

        if msg_type == "market_lifecycle_v2":
            ticker = payload.get("market_ticker")
            if not ticker:
                return

            ts_ms = self._parse_exchange_ts_ms(
                payload.get("ts") or payload.get("updated_ts") or payload.get("close_ts"),
                ingest_ts,
            )
            raw_status = str(payload.get("event_type") or payload.get("status") or "unknown").lower()
            if raw_status in {"market_open", "open"}:
                status = "open"
            elif raw_status in {"market_close", "closed", "close"}:
                status = "closed"
            else:
                status = raw_status
            event = LifecycleEvent(
                exchange_ts=ts_ms,
                ingest_ts=ingest_ts,
                ticker=ticker,
                status=str(status),
                seq=data.get("seq"),
            )
            await self.event_queue.put(event)
