from dataclasses import dataclass

@dataclass
class CollectorConfig:
    symbol: str = "BTCUSDT"
    data_dir: str = "./data"
    max_reconnect_attempts: int = 10
    reconnect_delay_base: float = 1.0
    orderbook_snapshot_interval_sec: float = 0.1
    orderbook_levels: int = 1000
    open_interest_fetch_interval_sec: float = 1.0
    buffer_flush_interval_sec: float = 2.0
    buffer_flush_max_records: int = 10000