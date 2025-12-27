# buffer.py
import json
import logging
import pandas as pd
from collections import deque
from typing import Dict, Any, Optional, List

logger = logging.getLogger("InMemoryBuffer")


class InMemoryBuffer:
    def __init__(self, stream_type: str, retention_ms: int = 300_000):  # 5 минут по умолчанию
        self.stream_type = stream_type
        self.retention_ms = retention_ms
        self._records: deque = deque()

    def add(self, data: Dict[str, Any]):
        # Сериализуем сложные типы (list/dict) в JSON — как в storage.py
        safe_data = {}
        for k, v in data.items():
            if isinstance(v, (list, dict)):
                safe_data[k] = json.dumps(v, separators=(',', ':'))
            else:
                safe_data[k] = v

        # Определяем временную метку для TTL
        ts = safe_data.get("exchange_ts")
        if ts is None:
            logger.warning(f"⚠️ No 'exchange_ts' in record for stream '{self.stream_type}': {safe_data}")
            return

        self._records.append(safe_data)

        # Удаляем устаревшие записи (старше retention_ms)
        cutoff = ts - self.retention_ms
        while self._records and self._records[0].get("exchange_ts", float('inf')) < cutoff:
            self._records.popleft()

    def to_dataframe(self) -> pd.DataFrame:
        if not self._records:
            return pd.DataFrame()
        return pd.DataFrame(list(self._records))

    def clear(self):
        self._records.clear()