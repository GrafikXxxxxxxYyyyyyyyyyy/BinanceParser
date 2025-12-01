import asyncio
import json
import time
import logging
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
        self.symbol = config.symbol.upper()
        self.book = OrderBook(self.symbol)
        self.storage = DataStorage(config.data_dir, self.symbol)
        self.session = None
        self.running = False
        self.contract_size = 1.0
        self._book_synced = False
        self._last_depth_exchange_ts = 0
        self._last_snapshot_local_ts = 0  # время последнего снапшота (в ms)
        self._reinit_in_progress = False  # 🔒 защита от параллельных восстановлений

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
            elif resp.status in (418, 429):
                logger.warning(f"⚠️ Rate limit hit (HTTP {resp.status}). IP likely banned. Pausing reinit.")
                return None  # не пытаемся повторять
            else:
                logger.error(f"❌ Failed to fetch snapshot: {resp.status}")
                return None

    async def fetch_open_interest(self):
        """Получает текущий открытый интерес через правильный эндпоинт."""
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": self.symbol}
        mark_url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={self.symbol}"

        try:
            async with self.session.get(url, params=params) as oi_resp:
                if oi_resp.status != 200:
                    logger.warning(f"OI fetch failed: {oi_resp.status}")
                    return
                oi_data = await oi_resp.json()
                open_interest = float(oi_data["openInterest"])

            async with self.session.get(mark_url) as mark_resp:
                if mark_resp.status != 200:
                    logger.warning(f"Mark price fetch failed: {mark_resp.status}")
                    return
                mark_data = await mark_resp.json()
                mark_price = float(mark_data.get("markPrice", 0))

            value_usd = open_interest * mark_price * self.contract_size

            self.storage.buffer("openInterest", {
                "exchange_ts": int(time.time() * 1000),
                "local_recv_ts": int(time.time() * 1000),
                "openInterest": open_interest,
                "valueUSD": value_usd
            })
        except Exception as e:
            logger.error(f"Open interest fetch error: {e}", exc_info=True)

    def _trigger_orderbook_resync(self):
        """Запускает ресинхронизацию ТОЛЬКО если стакан был синхронизирован и восстановление не идёт."""
        if not self.running or not self._book_synced or self._reinit_in_progress:
            return
        logger.info("🔄 Triggering OrderBook resync due to desync")
        self._book_synced = False
        asyncio.create_task(self._delayed_reinit_orderbook())

    async def _delayed_reinit_orderbook(self):
        """Откладывает реинициализацию на 0.5 сек, чтобы избежать шторма запросов."""
        if self._reinit_in_progress:
            return
        self._reinit_in_progress = True
        try:
            await asyncio.sleep(0.5)  # дать сети стабилизироваться
            await self._reinit_orderbook_after_disconnect()
        finally:
            self._reinit_in_progress = False

    async def _reinit_orderbook_after_disconnect(self):
        """Пытается восстановить стакан. Вызывается только один раз за раз."""
        if not self.running:
            return

        logger.info("🔄 OrderBook reinitialization STARTED")
        self._book_synced = False

        base_delay = 2.0
        for attempt in range(5):
            if not self.running:
                return
            try:
                snapshot = await self.fetch_depth_snapshot()
                if snapshot:
                    self.book.apply_snapshot(snapshot)
                    logger.info(f"📸 Snapshot refetched (lastUpdateId={self.book.last_update_id})")
                    stream_url = f"wss://fstream.binance.com/ws/{self.symbol.lower()}@depth@100ms"
                    asyncio.create_task(self.websocket_reader(stream_url, self._depth_handler))
                    return
            except Exception as e:
                logger.warning(f"⚠️ Snapshot attempt {attempt+1}/5 failed: {e}")
            
            # Экспоненциальная задержка
            delay = min(base_delay * (2 ** attempt), 60.0)
            logger.info(f"  ⏳ Waiting {delay:.1f}s before next snapshot attempt...")
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
        logger.warning("⚠️ OrderBook sync slow — but continuing (data will resume)")

    async def _recover_from_wal(self):
        logger.info("🔍 Recovering data from WAL files...")
        recovered_count = 0
        for stream_type in self.storage._schemas.keys():
            from storage import WALLogger
            wal_logger = WALLogger(self.cfg.data_dir, self.symbol, stream_type)
            records = list(wal_logger.read_all())
            if not records:
                continue
            logger.info(f"  → Found {len(records)} records in {stream_type} WAL")
            for entry in records:
                self.storage.buffer(stream_type, entry["data"])
            recovered_count += len(records)

        if recovered_count:
            logger.info(f"✅ Recovered {recovered_count} records from WAL")
            self.storage.flush_all()
        else:
            logger.info("📭 No WAL files found — clean start")

    async def periodic_orderbook_snapshot(self):
        """FALLBACK: сохраняет снапшот каждые N мс, если стакан не обновлялся."""
        logger.info("✅ Periodic orderbook snapshot task STARTED (fallback)")
        while self.running:
            try:
                if self._book_synced and self._last_depth_exchange_ts != 0:
                    local_ts = int(time.time() * 1000)
                    self._try_save_snapshot(
                        exchange_ts=self._last_depth_exchange_ts,
                        local_ts=local_ts
                    )
            except Exception as e:
                logger.error(f"💥 Periodic snapshot error: {e}", exc_info=True)
            await asyncio.sleep(self.cfg.orderbook_snapshot_interval_sec)

    async def periodic_open_interest(self):
        logger.info("✅ Open Interest fetcher STARTED (1Hz)")
        while self.running:
            await self.fetch_open_interest()
            await asyncio.sleep(self.cfg.open_interest_fetch_interval_sec)

    def _process_depth_diff(self, msg: Dict[str, Any]):
        local_recv = int(time.time() * 1000)
        self._last_depth_exchange_ts = msg["E"]
        self.storage.buffer("depthDiffs", {
            "U": msg["U"],
            "u": msg["u"],
            "bids": msg["b"],
            "asks": msg["a"],
            "exchange_ts": msg["E"],
            "local_recv_ts": local_recv
        })

        if self._book_synced:
            self._try_save_snapshot(exchange_ts=msg["E"], local_ts=local_recv)

    def _try_save_snapshot(self, exchange_ts: int, local_ts: int):
        interval_ms = int(self.cfg.orderbook_snapshot_interval_sec * 1000)
        if local_ts - self._last_snapshot_local_ts >= interval_ms:
            self._save_orderbook_snapshot(exchange_ts, local_ts)

    def _save_orderbook_snapshot(self, exchange_ts: int, local_ts: int):
        try:
            bids, asks = self.book.get_top_n(self.cfg.orderbook_levels)
            if not bids or not asks:
                return
            self.storage.buffer("orderbook_snapshots", {
                "exchange_ts": exchange_ts,
                "local_recv_ts": local_ts,
                "bids": bids,
                "asks": asks,
                "lastUpdateId": self.book.last_update_id
            })
            self._last_snapshot_local_ts = local_ts
            logger.debug(f"📸 Saved orderbook snapshot @ {exchange_ts}")
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
            return
        self.storage.buffer("liquidations", {
            "symbol": o["s"],
            "side": o["S"],
            "price": float(o["p"]),
            "qty": float(o["q"]),
            "exchange_ts": o["T"],
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
                if "depth" in stream_url:
                    logger.info("🔄 Depth stream disconnected. Reinitializing...")
                    self._trigger_orderbook_resync()
                await asyncio.sleep(delay)
        logger.error(f"❌ Max reconnect attempts reached for {stream_url}")

    async def validate_orderbook(self):
        logger.info("✅ OrderBook validator STARTED (every 30s)")
        while self.running:
            try:
                if self._book_synced:
                    snapshot = await self.fetch_depth_snapshot()
                    if not snapshot:
                        logger.warning("⚠️ Validation: failed to fetch snapshot")
                        continue

                    local_bids, local_asks = self.book.get_top_n(10)
                    if not local_bids or not local_asks:
                        continue

                    rest_bids = [(float(p), float(q)) for p, q in snapshot["bids"][:10]]
                    rest_asks = [(float(p), float(q)) for p, q in snapshot["asks"][:10]]

                    if not rest_bids or not rest_asks:
                        continue

                    local_best_bid = local_bids[0][0]
                    rest_best_bid = rest_bids[0][0]
                    local_best_ask = local_asks[0][0]
                    rest_best_ask = rest_asks[0][0]

                    bid_diff = abs(local_best_bid - rest_best_bid) / rest_best_bid
                    ask_diff = abs(local_best_ask - rest_best_ask) / rest_best_ask

                    logger.debug(f"🔍 Validation: local bid={local_best_bid}, REST bid={rest_best_bid}, diff={bid_diff:.6f}")
                    logger.debug(f"🔍 Validation: local ask={local_best_ask}, REST ask={rest_best_ask}, diff={ask_diff:.6f}")

                    if bid_diff > 0.0005 or ask_diff > 0.0005:
                        logger.critical(
                            f"💥 OrderBook VALIDATION FAILED! "
                            f"Bid diff: {bid_diff:.4%}, Ask diff: {ask_diff:.4%} — triggering resync"
                        )
                        self._book_synced = False
                        self._trigger_orderbook_resync()
                    else:
                        logger.info("✅ OrderBook VALIDATION PASSED")
                else:
                    logger.debug("⏸️ Skipping validation — orderbook not synced")
            except Exception as e:
                logger.error(f"💥 OrderBook validation error: {e}", exc_info=True)
            await asyncio.sleep(300)

    async def handle_agg_trade(self, msg): self.process_agg_trade(msg)
    async def handle_raw_trade(self, msg): self.process_raw_trade(msg)
    async def handle_mark_price(self, msg): self.process_mark_price(msg)
    async def handle_liquidation(self, msg): self.process_liquidation(msg)
    async def handle_book_ticker(self, msg): self.process_book_ticker(msg)

    async def periodic_flush(self):
        while self.running:
            await asyncio.sleep(self.cfg.buffer_flush_interval_sec)
            self.storage.flush_all()
            for stream_type, buffer in list(self.storage._buffers.items()):
                if len(buffer) >= self.cfg.buffer_flush_max_records:
                    logger.info(f"📤 Forced flush for '{stream_type}' (size: {len(buffer)})")
                    try:
                        self.storage.flush_stream(stream_type)
                    except Exception as e:
                        logger.error(f"💥 Flush failed for '{stream_type}': {e}")

    async def safe_task(self, task_factory, task_name: str):
        while self.running:
            try:
                coro = task_factory()
                if not asyncio.iscoroutine(coro):
                    raise ValueError(f"task_factory for '{task_name}' did not return a coroutine")
                await coro
            except asyncio.CancelledError:
                logger.info(f"⏹️ Task '{task_name}' cancelled.")
                break
            except Exception as e:
                logger.critical(f"💥 CRITICAL: Task '{task_name}' crashed! Restarting in 1s. Error: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def start(self):
        self.session = ClientSession()
        self.running = True

        await self._recover_from_wal()
        await self.fetch_exchange_info()
        await self._proper_orderbook_init()

        streams = [
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@aggTrade", self.handle_agg_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@trade", self.handle_raw_trade),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@markPrice@1s", self.handle_mark_price),
            ("wss://fstream.binance.com/ws/!forceOrder@arr", self.handle_liquidation),
            (f"wss://fstream.binance.com/ws/{self.symbol.lower()}@bookTicker", self.handle_book_ticker),
        ]

        from functools import partial

        tasks = [
            self.safe_task(self.periodic_orderbook_snapshot, "orderbook_snapshot"),
            self.safe_task(self.periodic_open_interest, "open_interest"),
            self.safe_task(self.periodic_flush, "periodic_flush"),
            self.safe_task(self.validate_orderbook, "orderbook_validator"),
        ]
        tasks += [
            self.safe_task(partial(self.websocket_reader, url, handler), f"ws_{i}")
            for i, (url, handler) in enumerate(streams)
        ]

        await asyncio.gather(*tasks)

    async def stop(self):
        self.running = False
        if self.session and not self.session.closed:
            await self.session.close()
        self.storage.close_all()
        logger.info("⏹️ Collector stopped and data flushed.")