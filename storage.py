import pandas as pd
import os
import time
from datetime import datetime

class DataStorage:
    def __init__(self, data_dir: str, symbol: str):
        self.data_dir = data_dir
        self.symbol = symbol
        os.makedirs(data_dir, exist_ok=True)
        self._current_hour = None
        self._buffers = {}
        self._file_handles = {}  # не используем, но можно для streaming

    def _get_current_hour(self) -> str:
        """Возвращает строку вида '20251124_15' (UTC)"""
        return datetime.utcnow().strftime("%Y%m%d_%H")

    def _get_path(self, stream_type: str, hour: str) -> str:
        return os.path.join(self.data_dir, f"{self.symbol}_{stream_type}_{hour}.parquet")

    def buffer(self, stream_type: str, data: dict):
        current = self._get_current_hour()
        if self._current_hour != current:
            # Смена часа — сбросить всё старое
            self.flush_all()
            self._current_hour = current
            self._buffers = {}

        if stream_type not in self._buffers:
            self._buffers[stream_type] = []
        self._buffers[stream_type].append(data)

    def flush_all(self):
        if not self._buffers:
            return

        hour = self._get_current_hour() if self._current_hour is None else self._current_hour

        for stream_type, records in self._buffers.items():
            if not records:
                continue

            df = pd.DataFrame(records)
            path = self._get_path(stream_type, hour)

            # Append to existing hourly file (or create new)
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                df = pd.concat([existing, df], ignore_index=True)
            df.to_parquet(path, index=False)

        self._buffers = {}