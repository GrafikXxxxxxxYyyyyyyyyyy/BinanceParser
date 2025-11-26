import asyncio
import logging
import sys
from collector import BinanceFuturesCollector
from config import CollectorConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

async def main():
    if len(sys.argv) != 2:
        print("Usage: python3 main.py <SYMBOL>")
        return
    symbol = sys.argv[1].upper()
    config = CollectorConfig(
        symbol=symbol,
        use_full_depth_stream=True,
        data_dir=f"./data_{symbol}",
        snapshot_interval_minutes=10,
        orderbook_snapshot_interval_sec=0.1,
        orderbook_levels=50
    )
    collector = BinanceFuturesCollector(config)
    try:
        await collector.start()
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping {symbol}...")
    finally:
        if collector:
            await collector.stop()

if __name__ == "__main__":
    asyncio.run(main())