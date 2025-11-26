import os
import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from typing import Dict, Any

# Настройка логгера
logger = logging.getLogger("Storage")

class DataStorage:
    def __init__(self, data_dir: str, symbol: str):
        self.data_dir = data_dir
        self.symbol = symbol
        os.makedirs(data_dir, exist_ok=True)
        self._current_hour: Dict[str, str] = {}
        self._buffers: Dict[str, list] = {}
        self._writers: Dict[str, pq.ParquetWriter] = {}
        self._schemas = self._define_schemas()

    def _define_schemas(self) -> Dict[str, pa.Schema]:
        return {
            "depthDiffs": pa.schema([
                ("U", pa.int64()), ("u", pa.int64()),
                ("bids", pa.string()), ("asks", pa.string()),
                ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "orderbook_snapshots": pa.schema([
                ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64()),
                ("bids", pa.string()), ("asks", pa.string())
            ]),
            "aggTrades": pa.schema([
                ("tradeId", pa.int64()), ("price", pa.float64()), ("qty", pa.float64()),
                ("isBuyerMaker", pa.bool_()), ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "rawTrades": pa.schema([
                ("id", pa.int64()), ("price", pa.float64()), ("qty", pa.float64()),
                ("isBuyerMaker", pa.bool_()), ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "markPrice": pa.schema([
                ("markPrice", pa.float64()), ("indexPrice", pa.float64()), ("fundingRate", pa.float64()),
                ("nextFundingTime", pa.int64()), ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "liquidations": pa.schema([
                ("symbol", pa.string()), ("side", pa.string()), ("price", pa.float64()),
                ("qty", pa.float64()), ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "klines_1s": pa.schema([
                ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
                ("close", pa.float64()), ("volume", pa.float64()), ("quoteVolume", pa.float64()),
                ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "bookTicker": pa.schema([
                ("bestBid", pa.float64()), ("bestBidQty", pa.float64()),
                ("bestAsk", pa.float64()), ("bestAskQty", pa.float64()),
                ("exchange_ts", pa.int64()), ("local_recv_ts", pa.int64())
            ]),
            "openInterest": pa.schema([
                ("timestamp", pa.int64()), ("openInterest", pa.float64()), ("valueUSD", pa.float64())
            ])
        }

    def _get_current_hour(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H")

    def _get_path(self, stream_type: str, hour: str) -> str:
        return os.path.join(self.data_dir, f"{self.symbol}_{stream_type}_{hour}.parquet")

    def buffer(self, stream_type: str, data: Dict[str, Any]):
        if stream_type not in self._buffers:
            self._buffers[stream_type] = []
        safe_data = {}
        for k, v in data.items():
            if isinstance(v, (list, dict)):
                safe_data[k] = json.dumps(v)
            else:
                safe_data[k] = v
        self._buffers[stream_type].append(safe_data)

    def _rotate_writer_if_needed(self, stream_type: str):
        current_hour = self._get_current_hour()
        if self._current_hour.get(stream_type) != current_hour:
            self._close_writer(stream_type)
            self._current_hour[stream_type] = current_hour
            path = self._get_path(stream_type, current_hour)
            self._writers[stream_type] = pq.ParquetWriter(
                path, self._schemas[stream_type]
            )

    def _close_writer(self, stream_type: str):
        if stream_type in self._writers:
            self._writers[stream_type].close()
            del self._writers[stream_type]

    def flush_stream(self, stream_type: str):
        buffer = self._buffers.get(stream_type)
        if not buffer:
            return
        try:
            self._rotate_writer_if_needed(stream_type)
            df = pd.DataFrame(buffer)
            table = pa.Table.from_pandas(df, schema=self._schemas[stream_type])
            self._writers[stream_type].write_table(table)
        except Exception as e:
            logger.error(f"Flush error for {stream_type}: {e}")
        finally:
            self._buffers[stream_type].clear()

    def flush_all(self):
        for stream in list(self._buffers.keys()):
            if self._buffers[stream]:
                self.flush_stream(stream)

    def close_all(self):
        self.flush_all()
        for stream in list(self._writers.keys()):
            self._close_writer(stream)