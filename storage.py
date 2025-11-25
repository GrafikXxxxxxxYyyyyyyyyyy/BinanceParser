import pandas as pd
import os
import json

class DataStorage:
    def __init__(self, data_dir: str, symbol: str):
        self.data_dir = data_dir
        self.symbol = symbol
        os.makedirs(data_dir, exist_ok=True)
        self._buffers = {}

    def _get_path(self, stream_type: str):
        return os.path.join(self.data_dir, f"{self.symbol}_{stream_type}.parquet")

    def buffer(self, stream_type: str, data: dict):
        if stream_type not in self._buffers:
            self._buffers[stream_type] = []
        self._buffers[stream_type].append(data)

    def flush_all(self):
        for stream_type, records in self._buffers.items():
            if not records:
                continue
            df = pd.DataFrame(records)
            path = self._get_path(stream_type)
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                df = pd.concat([existing, df], ignore_index=True)
            df.to_parquet(path, index=False)
            self._buffers[stream_type] = []
            print(f"Flushed {len(records)} records to {stream_type}")