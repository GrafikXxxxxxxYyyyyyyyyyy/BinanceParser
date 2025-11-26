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
        data_dir=f"./data_{symbol}",
        orderbook_levels=100,
        orderbook_snapshot_interval_sec=0.05,  # 20 Гц
        open_interest_fetch_interval_sec=1.0,
        buffer_flush_interval_sec=2.0
    )
    collector = BinanceFuturesCollector(config)
    try:
        await collector.start()
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping {symbol} collector...")
    finally:
        if collector:
            await collector.stop()

if __name__ == "__main__":
    asyncio.run(main())