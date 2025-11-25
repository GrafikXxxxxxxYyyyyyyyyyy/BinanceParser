from dataclasses import dataclass

@dataclass
class CollectorConfig:
    symbol: str = "BTCUSDT"
    use_full_depth_stream: bool = True  
    data_dir: str = "./data"
    snapshot_interval_minutes: int = 10 
    max_reconnect_attempts: int = 10
    reconnect_delay_base: float = 1.0
    orderbook_snapshot_interval_sec: float = 0.1 
    orderbook_levels: int = 50 