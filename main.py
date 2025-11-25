import asyncio
import logging
from collector import BinanceFuturesCollector
from config import CollectorConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

async def main():
    config = CollectorConfig(
        symbol="BTCUSDT",
        use_full_depth_stream=True,  # критично!
        data_dir="./data",
        snapshot_interval_minutes=10,
        orderbook_snapshot_interval_sec=0.1,  # 100 мс
        orderbook_levels=50  # глубина для ML
    )
    collector = BinanceFuturesCollector(config)
    try:
        await collector.start()
    except KeyboardInterrupt:
        print("🛑 Stopping...")
        await collector.stop()

if __name__ == "__main__":
    asyncio.run(main())