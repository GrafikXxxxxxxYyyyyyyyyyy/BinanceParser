import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

# Public: depth, bookTicker, и тиковые сделки `<symbol>@trade` (см. миграцию WS; @trade не на /market).
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
WS_FUTURES_PUBLIC_BASE = "wss://fstream.binance.com/public/ws"
WS_FUTURES_MARKET_BASE = "wss://fstream.binance.com/market/ws"


def futures_ws_public(symbol: str, stream_rest: str) -> str:
    """High-frequency / public: depth@100ms, bookTicker, trade (тиковые сделки на /public, не на /market)."""
    return f"{WS_FUTURES_PUBLIC_BASE}/{symbol.lower()}@{stream_rest}"


def futures_ws_market(symbol: str, stream_rest: str) -> str:
    """Regular market streams: aggTrade, markPrice@1s, etc."""
    return f"{WS_FUTURES_MARKET_BASE}/{symbol.lower()}@{stream_rest}"


def unwrap_binance_ws_payload(parsed: Any) -> Optional[Dict[str, Any]]:
    """
    Path `/ws/` sends the event dict as root. `/stream` uses `{"stream":...,"data":{...}}`
    (`data` can be JSON string). Subscribe acks `{result:null,"id":...}` yield None.
    """
    if not isinstance(parsed, dict):
        return None
    inner = parsed.get("data")
    if isinstance(inner, str):
        try:
            inner = json.loads(inner)
        except json.JSONDecodeError:
            inner = None
    if isinstance(inner, dict):
        if not inner:
            return None
        return inner
    if parsed.get("result") is None and "id" in parsed and "e" not in parsed:
        return None
    return parsed


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
