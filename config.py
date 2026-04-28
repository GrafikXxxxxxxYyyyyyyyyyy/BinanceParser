from dataclasses import dataclass

# Binance USDⓈ-M Futures WebSocket routing (legacy `…/ws/` decommissioned; use /public и /market):
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
WS_FUTURES_PUBLIC_BASE = "wss://fstream.binance.com/public/ws"
WS_FUTURES_MARKET_BASE = "wss://fstream.binance.com/market/ws"


def futures_ws_public(symbol: str, stream_rest: str) -> str:
    """High-frequency streams: depth, bookTicker, etc. stream_rest example: depth@100ms, bookTicker."""
    return f"{WS_FUTURES_PUBLIC_BASE}/{symbol.lower()}@{stream_rest}"


def futures_ws_market(symbol: str, stream_rest: str) -> str:
    """Regular market streams: aggTrade, trade, markPrice@1s, etc."""
    return f"{WS_FUTURES_MARKET_BASE}/{symbol.lower()}@{stream_rest}"


@dataclass
class CollectorConfig:
    symbol: str = "BTCUSDT"
    data_dir: str = "./data"
    max_reconnect_attempts: int = 10
    reconnect_delay_base: float = 1.0
    orderbook_snapshot_interval_sec: float = 0.1
    orderbook_levels: int = 100
    open_interest_fetch_interval_sec: float = 1.0
    buffer_flush_interval_sec: float = 2.0
    buffer_flush_max_records: int = 10000
