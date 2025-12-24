import asyncio
import logging
import sys
from collector import BinanceFuturesCollector
from config import CollectorConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("collector.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

async def main():
    if len(sys.argv) != 2:
        print("Usage: python3 main.py <SYMBOL>")
        sys.exit(1)
    symbol = sys.argv[1].upper()
    config = CollectorConfig(
        symbol=symbol,
        data_dir=f"./data_{symbol}",
        orderbook_levels=1000,
        orderbook_snapshot_interval_sec=0.2,
        open_interest_fetch_interval_sec=1.0,
        buffer_flush_interval_sec=2.0
    )
    collector = BinanceFuturesCollector(config)
    try:
        await collector.start()
    except KeyboardInterrupt:
        print(f"\n🛑 Received SIGINT. Stopping {symbol} collector...")
    except Exception as e:
        logging.critical(f"💥 UNEXPECTED CRASH: {e}", exc_info=True)
    finally:
        if collector:
            await collector.stop()
        print("✅ Collector shut down cleanly.")

if __name__ == "__main__":
    asyncio.run(main())