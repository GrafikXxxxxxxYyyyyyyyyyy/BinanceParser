# live_collector.py
import asyncio
import json
import time
import logging
from typing import Dict, Any, Optional
from collections import defaultdict

import pandas as pd
from aiohttp import ClientSession
from websockets import connect, ConnectionClosed

from orderbook import OrderBook
from buffer import InMemoryBuffer

logger = logging.getLogger("LiveCollector")


class LiveCollector:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        retention_seconds: int = 30,
        orderbook_levels: int = 1000,
        orderbook_snapshot_interval_sec: float = 0.2,
        open_interest_fetch_interval_sec: float = 1.0,
    ):
        self.symbol = symbol.upper()
        self.retention_ms = retention_seconds * 1000
        self.orderbook_levels = orderbook_levels
        self.orderbook_snapshot_interval_sec = orderbook_snapshot_interval_sec
        self.open_interest_fetch_interval_sec = open_interest_fetch_interval_sec

        self.book = OrderBook(self.symbol)
        self._buffers: Dict[str, InMemoryBuffer] = defaultdict(
            lambda: InMemoryBuffer("unknown", self.retention_ms)
        )
        self.session: Optional[ClientSession] = None
        self.running = False
        self.contract_size = 1.0
        self._book_synced = False
        self._last_depth_exchange_ts = 0
        self._last_snapshot_local_ts = 0
        self._reinit_in_progress = False

        # Инициализируем все потоки согласно storage.py
        stream_types = [
            "orderbook_snapshots", "aggTrades", "rawTrades",
            "markPrice", "bookTicker"
        ]
        for st in stream_types:
            self._buffers[st] = InMemoryBuffer(st, self.retention_ms)

    def _buffer(self, stream_type: str, data: Dict[str, Any]):
        self._buffers[stream_type].add(data)

    async def fetch_exchange_info(self):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with self.session.get(url) as resp:
            data = await resp.json()
            symbol_info = next(s for s in data["symbols"] if s["symbol"] == self.symbol)
            filters = {f["filterType"]: f for f in symbol_info["filters"]}
            lot_size = filters.get("LOT_SIZE", {})
            self.contract_size = float(lot_size.get("minQty", 1.0))
            logger.info(f"✅ Exchange info loaded. Contract size: {self.contract_size}")

    async def fetch_depth_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000"
        async with self.session.get(url) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.error(f"❌ Snapshot fetch failed: {resp.status}")
                return None

    def _trigger_orderbook_resync(self):
        if not self.running or not self._book_synced or self._reinit_in_progress:
            return
        logger.info("🔄 Triggering OrderBook resync due to desync")
        self._book_synced = False
        asyncio.create_task(self._delayed_reinit_orderbook())

    async def _delayed_reinit_orderbook(self):
        if self._reinit_in_progress:
            return
        self._reinit_in_progress = True
        try:
            await asyncio.sleep(0.5)
            await self._reinit_orderbook_after_disconnect()
        finally:
            self._reinit_in_progress = False

    async def _reinit_orderbook_after_disconnect(self):
        if not self.running:
            return
        logger.info("🔄 OrderBook reinitialization STARTED")
        self._book_synced = False
        for attempt in range(5):
            if not self.running:
                return
            try:
                snapshot = await self.fetch_depth_snapshot()
                if snapshot:
                    self.book.apply_snapshot(snapshot)
                    stream_url = f"wss://fstream.binance.com/ws/{self.symbol.lower()}@depth@100ms"
                    asyncio.create_task(self.websocket_reader(stream_url, self._depth_handler))
                    logger.info(f"📸 Snapshot refetched (lastUpdateId={self.book.last_update_id})")
                    return
            except Exception as e:
                logger.warning(f"⚠️ Snapshot attempt {attempt+1}/5 failed: {e}")
            delay = min(2.0 * (2 ** attempt), 60.0)
            await asyncio.sleep(delay)
        logger.error("❌ Reinit failed after 5 attempts — continuing without orderbook")

    async def _proper_orderbook_init(self):
        logger.info("🔍 Starting order book initialization (@depth@100ms)...")
        snapshot = await self.fetch_depth_snapshot()
        if not snapshot:
            raise RuntimeError("❌ Failed to get initial snapshot")
        self.book.apply_snapshot(snapshot)
        logger.info(f"📸 Snapshot received (lastUpdateId={self.book.last_update_id})")

        stream_url = f"wss://fstream.binance.com/ws/{self.symbol.lower()}@depth@100ms"
        asyncio.create_task(self.websocket_reader(stream_url, self._depth_handler))

        for _ in range(20):
            if self._book_synced:
                return
            await asyncio.sleep(0.1)
        logger.warning("⚠️ OrderBook sync slow — but continuing")

    def _process_depth_diff(self, msg: Dict[str, Any]):
        self._last_depth_exchange_ts = msg["E"]
        if self._book_synced:
            self._try_save_snapshot(exchange_ts=msg["E"])

    def _try_save_snapshot(self, exchange_ts: int):
        current_ts = int(time.time() * 1000)
        if current_ts - self._last_snapshot_local_ts >= int(self.orderbook_snapshot_interval_sec * 1000):
            self._save_orderbook_snapshot(exchange_ts)
            self._last_snapshot_local_ts = current_ts

    def _save_orderbook_snapshot(self, exchange_ts: int):
        try:
            bids, asks = self.book.get_top_n(self.orderbook_levels)

            records = []
            for level, (price, qty) in enumerate(bids[:1000]):
                records.append({
                    "exchange_ts": exchange_ts,
                    "lastUpdateId": self.book.last_update_id,
                    "side": "bid",
                    "level": level,
                    "price": price,
                    "qty": qty
                })
            for level, (price, qty) in enumerate(asks[:1000]):
                records.append({
                    "exchange_ts": exchange_ts,
                    "lastUpdateId": self.book.last_update_id,
                    "side": "ask",
                    "level": level,
                    "price": price,
                    "qty": qty
                })

            for rec in records:
                self._buffer("orderbook_snapshots", rec)

            logger.debug(f"📸 Saved in-memory orderbook snapshot @ {exchange_ts} ({len(records)} records)")
        except Exception as e:
            logger.error(f"💥 Failed to save orderbook snapshot: {e}", exc_info=True)

    async def _depth_handler(self, msg: dict):
        if not self._book_synced:
            if msg["u"] >= self.book.last_update_id:
                logger.info(f"✅ OrderBook SYNCED (@100ms) with u={msg['u']}")
                self.book.apply_diff(msg)
                self._book_synced = True
                self._process_depth_diff(msg)
            else:
                logger.debug(f"🔄 Skipping outdated diff: u={msg['u']} < {self.book.last_update_id}")
            return

        self.book.apply_diff(msg)
        self._process_depth_diff(msg)

    def process_agg_trade(self, msg: Dict[str, Any]):
        self._buffer("aggTrades", {
            "tradeId": msg["a"],
            "price": float(msg["p"]),
            "qty": float(msg["q"]),
            "isBuyerMaker": msg["m"],
            "exchange_ts": msg["T"]
        })

    def process_raw_trade(self, msg: Dict[str, Any]):
        self._buffer("rawTrades", {
            "id": msg["t"],
            "price": float(msg["p"]),
            "qty": float(msg["q"]),
            "isBuyerMaker": msg["m"],
            "exchange_ts": msg["T"]
        })

    def process_mark_price(self, msg: Dict[str, Any]):
        self._buffer("markPrice", {
            "markPrice": float(msg["p"]),
            "indexPrice": float(msg["i"]),
            "fundingRate": float(msg["r"]),
            "nextFundingTime": msg["T"],
            "exchange_ts": msg["E"]
        })

    def process_book_ticker(self, msg: Dict[str, Any]):
        self._buffer("bookTicker", {
            "bestBid": float(msg["b"]), "bestBidQty": float(msg["B"]),
            "bestAsk": float(msg["a"]), "bestAskQty": float(msg["A"]),
            "exchange_ts": msg["E"]
        })

    async def websocket_reader(self, stream_url: str, handler):
        attempt = 0
        max_reconnect_attempts = 10
        reconnect_delay_base = 1.0
        while self.running and attempt < max_reconnect_attempts:
            try:
                async with connect(stream_url) as ws:
                    logger.info(f"🔌 Connected to {stream_url}")
                    attempt = 0
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            data = json.loads(msg)
                            if asyncio.iscoroutinefunction(handler):
                                await handler(data)
                            else:
                                handler(data)
                        except asyncio.TimeoutError:
                            await ws.ping()
            except (ConnectionClosed, OSError) as e:
                attempt += 1
                delay = min(reconnect_delay_base * (2 ** (attempt - 1)), 60.0)
                logger.warning(f"⚠️ WS error: {e}. Reconnect {attempt}/{max_reconnect_attempts} in {delay:.1f}s")
                if "depth" in stream_url:
                    logger.info("🔄 Depth stream disconnected. Reinitializing...")
                    self._trigger_orderbook_resync()
                await asyncio.sleep(delay)
        logger.error(f"❌ Max reconnect attempts reached for {stream_url}")

    async def periodic_orderbook_snapshot(self):
        while self.running:
            try:
                if self._book_synced and self._last_depth_exchange_ts != 0:
                    self._try_save_snapshot(exchange_ts=self._last_depth_exchange_ts)
            except Exception as e:
                logger.error(f"💥 Periodic snapshot error: {e}", exc_info=True)
            await asyncio.sleep(self.orderbook_snapshot_interval_sec)

    def get_dataframe(self, stream_type: str) -> pd.DataFrame:
        """Возвращает актуальный pd.DataFrame с последними данными (до 5 минут)."""
        if stream_type not in self._buffers:
            return pd.DataFrame()
        return self._buffers[stream_type].to_dataframe()

    def is_orderbook_synced(self) -> bool:
        return self._book_synced

    async def start(self):
        self.session = ClientSession()
        self.running = True

        await self.fetch_exchange_info()
        await self._proper_orderbook_init()

        streams = [
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@aggTrade", self.process_agg_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@trade", self.process_raw_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@markPrice@1s", self.process_mark_price),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@bookTicker", self.process_book_ticker),
        ]

        tasks = [
            asyncio.create_task(self.periodic_orderbook_snapshot()),
        ]
        tasks += [
            asyncio.create_task(self.websocket_reader(url, handler))
            for url, handler in streams
        ]

        await asyncio.gather(*tasks)

    async def stop(self):
        self.running = False
        if self.session and not self.session.closed:
            await self.session.close()
        logger.info("⏹️ LiveCollector stopped.")