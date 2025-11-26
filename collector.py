import asyncio
import json
import time
import logging
from collections import deque
from typing import Dict, Any
import pandas as pd  # требуется для storage
from aiohttp import ClientSession
from websockets import connect, ConnectionClosed
from orderbook import OrderBook
from storage import DataStorage
from config import CollectorConfig

logger = logging.getLogger("Collector")

class BinanceFuturesCollector:
    def __init__(self, config: CollectorConfig):
        self.cfg = config
        self.symbol = config.symbol.upper()
        self.book = OrderBook(self.symbol)
        self.storage = DataStorage(config.data_dir, self.symbol)
        self.session = None
        self.running = False
        self.last_snapshot_ts = 0

        self._depth_buffer = deque(maxlen=10000)  # ограниченный буфер
        self._snapshot_received = False
        self._book_synced = False
        self._depth_buffer_lock = asyncio.Lock()
        self.contract_size = 1.0  # будет определено из exchangeInfo

    async def fetch_exchange_info(self):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with self.session.get(url) as resp:
            data = await resp.json()
            symbol_info = next(s for s in data["symbols"] if s["symbol"] == self.symbol)
            filters = {f["filterType"]: f for f in symbol_info["filters"]}
            lot_size = filters.get("LOT_SIZE", {})
            self.contract_size = float(lot_size.get("minQty", 1.0))  # для BTCUSDT = 1
            logger.info(f"✅ Exchange info loaded. Contract size: {self.contract_size}")

    async def fetch_depth_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000"
        async with self.session.get(url) as resp:
            if resp.status != 200:
                logger.error(f"❌ Failed to fetch snapshot: {resp.status}")
                return None
            return await resp.json()

    async def fetch_open_interest(self):
        # Получаем Open Interest
        oi_url = "https://fapi.binance.com/futures/data/openInterestHist"
        params = {"symbol": self.symbol, "period": "5m", "limit": 1}
        # Получаем текущую цену (mark price)
        mark_url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={self.symbol}"

        try:
            async with self.session.get(oi_url, params=params) as oi_resp:
                if oi_resp.status != 200:
                    logger.warning(f"OI fetch failed: {oi_resp.status}")
                    return
                oi_data = await oi_resp.json()
                if not oi_data:
                    return
                oi_item = oi_data[0]

            async with self.session.get(mark_url) as mark_resp:
                if mark_resp.status != 200:
                    logger.warning(f"Mark price fetch failed: {mark_resp.status}")
                    return
                mark_data = await mark_resp.json()
                mark_price = float(mark_data.get("markPrice", 0))

            open_interest = float(oi_item["sumOpenInterest"])
            value_usd = open_interest * mark_price * self.contract_size

            self.storage.buffer("openInterest", {
                "timestamp": int(oi_item["timestamp"]),
                "openInterest": open_interest,
                "valueUSD": value_usd
            })
        except Exception as e:
            logger.error(f"Open interest fetch error: {e}")

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

        self.book.apply_diff(msg)
        self._process_depth_diff(msg)

    def _process_depth_diff(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self.storage.buffer("depthDiffs", {
            "U": msg["U"],
            "u": msg["u"],
            "bids": msg["b"],
            "asks": msg["a"],
            "exchange_ts": msg["E"],
            "local_recv_ts": local_recv
        })

    async def _proper_orderbook_init(self):
        logger.info("🔍 Starting IDEAL order book initialization...")
        stream_url = f"wss://fstream.binance.com/ws/{self.symbol.lower()}@depth"
        asyncio.create_task(self.websocket_reader(stream_url, self._depth_handler))
        await asyncio.sleep(0.1)
        snapshot = await self.fetch_depth_snapshot()
        if not snapshot:
            raise RuntimeError("❌ Failed to get initial snapshot")
        self.book.apply_snapshot(snapshot)
        self._snapshot_received = True
        self.last_snapshot_ts = time.time()
        logger.info("📸 Snapshot received")

        for _ in range(30):
            if self._book_synced:
                return
            await asyncio.sleep(0.1)

        async with self._depth_buffer_lock:
            for msg in self._depth_buffer:
                if msg["u"] >= self.book.last_update_id:
                    logger.warning("⚠️ Best-effort sync applied")
                    self.book.apply_diff(msg)
                    self._book_synced = True
                    self._depth_buffer.clear()
                    return
        logger.error("💥 Sync failed — disabling orderbook snapshots")

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
                    logger.info("🔄 Periodic snapshot applied")

    async def periodic_orderbook_snapshot(self):
        logger.info("✅ OrderBook snapshot task STARTED (20Hz)")
        while self.running:
            try:
                if self._book_synced:
                    local_ts = int(time.time() * 1000)
                    bids, asks = self.book.get_top_n(self.cfg.orderbook_levels)
                    if bids and asks:
                        self.storage.buffer("orderbook_snapshots", {
                            "exchange_ts": local_ts,
                            "local_recv_ts": local_ts,
                            "bids": bids,
                            "asks": asks
                        })
            except Exception as e:
                logger.error(f"💥 Orderbook snapshot error: {e}")
            await asyncio.sleep(self.cfg.orderbook_snapshot_interval_sec)

    async def periodic_open_interest(self):
        logger.info("✅ Open Interest fetcher STARTED (1Hz)")
        while self.running:
            await self.fetch_open_interest()
            await asyncio.sleep(self.cfg.open_interest_fetch_interval_sec)

    # --- Processors ---
    def process_agg_trade(self, msg: Dict[str, Any]):
        self.storage.buffer("aggTrades", {
            "tradeId": msg["a"],
            "price": float(msg["p"]),
            "qty": float(msg["q"]),
            "isBuyerMaker": msg["m"],
            "exchange_ts": msg["T"],
            "local_recv_ts": int(time.time() * 1000)
        })

    def process_raw_trade(self, msg: Dict[str, Any]):
        self.storage.buffer("rawTrades", {
            "id": msg["t"],
            "price": float(msg["p"]),
            "qty": float(msg["q"]),
            "isBuyerMaker": msg["m"],
            "exchange_ts": msg["T"],
            "local_recv_ts": int(time.time() * 1000)
        })

    def process_mark_price(self, msg: Dict[str, Any]):
        self.storage.buffer("markPrice", {
            "markPrice": float(msg["p"]),
            "indexPrice": float(msg["i"]),
            "fundingRate": float(msg["r"]),
            "nextFundingTime": msg["T"],
            "exchange_ts": msg["E"],
            "local_recv_ts": int(time.time() * 1000)
        })

    def process_liquidation(self, msg: Dict[str, Any]):
        o = msg["o"]
        if o["s"] != self.symbol:
            return  # фильтрация по символу!
        self.storage.buffer("liquidations", {
            "symbol": o["s"],
            "side": o["S"],
            "price": float(o["p"]),
            "qty": float(o["q"]),
            "exchange_ts": o["T"],
            "local_recv_ts": int(time.time() * 1000)
        })

    def process_kline(self, msg: Dict[str, Any]):
        k = msg["k"]
        if not k["x"]:
            return
        self.storage.buffer("klines_1s", {
            "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]), "close": float(k["c"]),
            "volume": float(k["v"]), "quoteVolume": float(k["q"]),
            "exchange_ts": k["t"],
            "local_recv_ts": int(time.time() * 1000)
        })

    def process_book_ticker(self, msg: Dict[str, Any]):
        self.storage.buffer("bookTicker", {
            "bestBid": float(msg["b"]), "bestBidQty": float(msg["B"]),
            "bestAsk": float(msg["a"]), "bestAskQty": float(msg["A"]),
            "exchange_ts": msg["E"],
            "local_recv_ts": int(time.time() * 1000)
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
                            if asyncio.iscoroutinefunction(handler):
                                await handler(data)
                            else:
                                handler(data)
                        except asyncio.TimeoutError:
                            await ws.ping()
            except (ConnectionClosed, OSError) as e:
                attempt += 1
                delay = min(self.cfg.reconnect_delay_base * (2 ** (attempt - 1)), 60.0)
                logger.warning(f"⚠️ WS error: {e}. Reconnect {attempt}/{self.cfg.max_reconnect_attempts} in {delay:.1f}s")
                await asyncio.sleep(delay)
        logger.error(f"❌ Max reconnect attempts reached for {stream_url}")

    # Async handlers
    async def handle_agg_trade(self, msg): self.process_agg_trade(msg)
    async def handle_raw_trade(self, msg): self.process_raw_trade(msg)
    async def handle_mark_price(self, msg): self.process_mark_price(msg)
    async def handle_liquidation(self, msg): self.process_liquidation(msg)
    async def handle_kline(self, msg): self.process_kline(msg)
    async def handle_book_ticker(self, msg): self.process_book_ticker(msg)

    async def periodic_flush(self):
        while self.running:
            await asyncio.sleep(self.cfg.buffer_flush_interval_sec)
            self.storage.flush_all()

    async def start(self):
        self.session = ClientSession()
        self.running = True
        await self.fetch_exchange_info()

        await self._proper_orderbook_init()

        streams = [
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@aggTrade", self.handle_agg_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@trade", self.handle_raw_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@markPrice@1s", self.handle_mark_price),
            ("wss://fstream.binance.com/ws/!forceOrder@arr", self.handle_liquidation),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@kline_1s", self.handle_kline),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@bookTicker", self.handle_book_ticker),
        ]

        tasks = [
            self.schedule_snapshot(),
            self.periodic_orderbook_snapshot(),
            self.periodic_open_interest(),
            self.periodic_flush(),
        ]
        tasks += [self.websocket_reader(url, handler) for url, handler in streams]

        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()
        self.storage.close_all()
        logger.info("⏹️  Collector stopped and data flushed.")