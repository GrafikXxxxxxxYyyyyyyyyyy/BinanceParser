import asyncio
import json
import time
import logging
from collections import deque
from typing import Dict, Any
from aiohttp import ClientSession
from websockets import connect, ConnectionClosed
from orderbook import OrderBook
from storage import DataStorage
from config import CollectorConfig

logger = logging.getLogger("Collector")

class BinanceFuturesCollector:
    def __init__(self, config: CollectorConfig):
        self.cfg = config
        self.symbol = config.symbol.lower()
        self.book = OrderBook(config.symbol)
        self.storage = DataStorage(config.data_dir, config.symbol)
        self.session = None
        self.running = False
        self.last_snapshot_ts = 0

        self._depth_buffer = deque()
        self._snapshot_received = False
        self._book_synced = False
        self._depth_buffer_lock = asyncio.Lock()

    async def fetch_exchange_info(self):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with self.session.get(url) as resp:
            data = await resp.json()
            symbol_info = next(s for s in data["symbols"] if s["symbol"] == self.cfg.symbol)
            self.filters = symbol_info["filters"]
            logger.info("✅ Exchange info loaded")

    async def fetch_depth_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.cfg.symbol}&limit=1000"
        async with self.session.get(url) as resp:
            if resp.status != 200:
                logger.error(f"❌ Failed to fetch snapshot: {resp.status}")
                return None
            return await resp.json()

    async def fetch_funding_history(self):
        """Сохраняем историю финансирования (для симуляций)."""
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={self.cfg.symbol}&limit=1000"
        async with self.session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data:
                    self.storage.buffer("funding_history", {
                        "fundingTime": item["fundingTime"],
                        "fundingRate": float(item["fundingRate"]),
                        "markPrice": float(item.get("markPrice", 0))
                    })
                logger.info("✅ Funding history fetched")

    async def _depth_handler(self, msg: dict):
        if not self._snapshot_received:
            async with self._depth_buffer_lock:
                self._depth_buffer.append(msg)
            return

        if not self._book_synced:
            async with self._depth_buffer_lock:
                if msg["U"] <= self.book.last_update_id + 1 and msg["u"] >= self.book.last_update_id:
                    logger.info("✅ Valid first diff found! Applying buffer...")
                    applying = False
                    for buffered_msg in list(self._depth_buffer):
                        if buffered_msg["U"] == msg["U"] and buffered_msg["u"] == msg["u"]:
                            applying = True
                        if applying:
                            self.book.apply_diff(buffered_msg)
                    self._depth_buffer.clear()
                    self._book_synced = True
                    logger.info("🎯 OrderBook IDEALLY SYNCED ✅")
                else:
                    self._depth_buffer.append(msg)
            return

        # Normal mode
        self.book.apply_diff(msg)
        self._process_depth_diff(msg)

    def _process_depth_diff(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self.storage.buffer("depthDiffs", {
            "U": msg["U"],
            "u": msg["u"],
            "bids": json.dumps(msg["b"]),
            "asks": json.dumps(msg["a"]),
            "exchange_ts": msg["E"],
            "local_recv_ts": local_recv
        })

    async def _proper_orderbook_init(self):
        logger.info("🔍 Starting IDEAL order book initialization...")
        # 1. СРАЗУ запускаем WebSocket и буферизацию
        stream_url = f"wss://fstream.binance.com/ws/{self.symbol}@depth"
        asyncio.create_task(self.websocket_reader(stream_url, self._depth_handler))
        # 2. Минимальная задержка для подключения (100 мс)
        await asyncio.sleep(0.1)
        # 3. Получаем snapshot
        snapshot = await self.fetch_depth_snapshot()
        if not snapshot:
            raise RuntimeError("❌ Failed to get initial snapshot")
        self.book.apply_snapshot(snapshot)
        self._snapshot_received = True
        self.last_snapshot_ts = time.time()
        logger.info("📸 Snapshot received, seeking valid diff in buffer...")

        # 4. Ждём до 2 секунд официальной синхронизации
        for _ in range(20):
            if self._book_synced:
                return
            await asyncio.sleep(0.1)

        # 5. BEST-EFFORT FALLBACK (один раз!)
        async with self._depth_buffer_lock:
            for msg in self._depth_buffer:
                if msg["u"] >= self.book.last_update_id:
                    logger.warning("⚠️ Official sync failed — applying best-effort first diff (one-time)")
                    self.book.apply_diff(msg)
                    self._book_synced = True
                    self._depth_buffer.clear()
                    logger.info("🎯 OrderBook SYNCED (best-effort, stable thereafter) ✅")
                    return
        logger.error("💥 No valid diff found — order book disabled")

    async def schedule_snapshot(self):
        while self.running:
            await asyncio.sleep(self.cfg.snapshot_interval_minutes * 60)
            if self._book_synced:
                snapshot = await self.fetch_depth_snapshot()
                if snapshot:
                    self.book.apply_snapshot(snapshot)
                    self._snapshot_received = True
                    self._book_synced = False
                    self._depth_buffer.clear()
                    self.last_snapshot_ts = time.time()
                    logger.info("🔄 Periodic snapshot applied for correction")

    async def periodic_orderbook_snapshot(self):
        logger.info("✅ OrderBook snapshot task STARTED (100ms interval)")
        while self.running:
            try:
                if self._book_synced:
                    local_ts = int(time.time() * 1000)
                    bids, asks = self.book.get_top_n(self.cfg.orderbook_levels)
                    if bids and asks:
                        self.storage.buffer("orderbook_snapshots", {
                            "exchange_ts": local_ts,
                            "local_recv_ts": local_ts,
                            "bids": json.dumps(bids),
                            "asks": json.dumps(asks)
                        })
            except Exception as e:
                logger.error(f"💥 Error in orderbook snapshot: {e}")
            await asyncio.sleep(self.cfg.orderbook_snapshot_interval_sec)

    # --- Обработчики данных ---
    def process_agg_trade(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self.storage.buffer("aggTrades", {
            "tradeId": msg["a"],
            "price": float(msg["p"]),
            "qty": float(msg["q"]),
            "isBuyerMaker": msg["m"],
            "exchange_ts": msg["T"],
            "local_recv_ts": local_recv
        })

    def process_mark_price(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self.storage.buffer("markPrice", {
            "markPrice": float(msg["p"]),
            "indexPrice": float(msg["i"]),
            "fundingRate": float(msg["r"]),
            "nextFundingTime": msg["T"],
            "exchange_ts": msg["E"],
            "local_recv_ts": local_recv
        })

    def process_liquidation(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        o = msg["o"]
        self.storage.buffer("liquidations", {
            "side": o["S"],
            "price": float(o["p"]),
            "qty": float(o["q"]),
            "exchange_ts": o["T"],
            "local_recv_ts": local_recv
        })

    def process_kline(self, msg: Dict[str, Any]):
        k = msg["k"]
        if not k["x"]:  # только закрытые свечи
            return
        local_recv = int(time.time() * 1000)
        self.storage.buffer("klines_1s", {
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "quoteVolume": float(k["q"]),
            "exchange_ts": k["t"],
            "local_recv_ts": local_recv
        })

    def process_book_ticker(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self.storage.buffer("bookTicker", {
            "bestBid": float(msg["b"]),
            "bestBidQty": float(msg["B"]),
            "bestAsk": float(msg["a"]),
            "bestAskQty": float(msg["A"]),
            "exchange_ts": msg["E"],
            "local_recv_ts": local_recv
        })

    async def websocket_reader(self, stream_url: str, handler):
        attempt = 0
        while self.running and attempt < self.cfg.max_reconnect_attempts:
            try:
                async with connect(stream_url) as ws:
                    logger.info(f"🔌 Connected to {stream_url}")
                    attempt = 0
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            data = json.loads(msg)
                            await handler(data)
                        except asyncio.TimeoutError:
                            await ws.ping()
            except (ConnectionClosed, OSError) as e:
                attempt += 1
                delay = self.cfg.reconnect_delay_base * (2 ** (attempt - 1))
                logger.warning(f"⚠️ WS error: {e}. Reconnect {attempt}/{self.cfg.max_reconnect_attempts} in {delay:.1f}s")
                await asyncio.sleep(delay)
        logger.error(f"❌ Max reconnect attempts reached for {stream_url}")

    async def handle_agg_trade(self, msg):
        self.process_agg_trade(msg)

    async def handle_mark_price(self, msg):
        self.process_mark_price(msg)

    async def handle_liquidation(self, msg):
        self.process_liquidation(msg)

    async def handle_kline(self, msg):
        self.process_kline(msg)

    async def handle_book_ticker(self, msg):
        self.process_book_ticker(msg)

    async def start(self):
        self.session = ClientSession()
        self.running = True
        await self.fetch_exchange_info()
        await self.fetch_funding_history()  # важно для 10x!

        await self._proper_orderbook_init()

        # Все остальные стримы
        streams = [
            (f"wss://fstream.binance.com/ws/{self.symbol}@aggTrade", self.handle_agg_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol}@markPrice@1s", self.handle_mark_price),
            (f"wss://fstream.binance.com/ws/!forceOrder@arr", self.handle_liquidation),
            (f"wss://fstream.binance.com/ws/{self.symbol}@kline_1s", self.handle_kline),
            (f"wss://fstream.binance.com/ws/{self.symbol}@bookTicker", self.handle_book_ticker),
        ]

        tasks = [
            self.schedule_snapshot(),
            self.periodic_orderbook_snapshot(),
        ]
        tasks += [self.websocket_reader(url, handler) for url, handler in streams]

        async def periodic_flush():
            while self.running:
                await asyncio.sleep(10)
                self.storage.flush_all()
        tasks.append(periodic_flush())

        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()
        self.storage.flush_all()