from collections import OrderedDict
import logging

logger = logging.getLogger("OrderBook")

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids = OrderedDict()
        self.asks = OrderedDict()
        self.last_update_id = -1
        self._is_synced = False

    def apply_snapshot(self, snapshot: dict):
        self.bids.clear()
        self.asks.clear()
        for price, qty in snapshot["bids"]:
            self.bids[float(price)] = float(qty)
        for price, qty in snapshot["asks"]:
            self.asks[float(price)] = float(qty)
        self.bids = OrderedDict(sorted(self.bids.items(), reverse=True))
        self.asks = OrderedDict(sorted(self.asks.items()))
        self.last_update_id = snapshot["lastUpdateId"]
        self._is_synced = False

    def apply_diff(self, diff: dict):
        for price, qty in diff["b"]:
            price_f, qty_f = float(price), float(qty)
            if qty_f == 0.0:
                self.bids.pop(price_f, None)
            else:
                self.bids[price_f] = qty_f
        self.bids = OrderedDict(sorted(self.bids.items(), reverse=True))

        for price, qty in diff["a"]:
            price_f, qty_f = float(price), float(qty)
            if qty_f == 0.0:
                self.asks.pop(price_f, None)
            else:
                self.asks[price_f] = qty_f
        self.asks = OrderedDict(sorted(self.asks.items()))
        self.last_update_id = diff["u"]
        self._is_synced = True

    def is_synced(self) -> bool:
        return self._is_synced

    def get_top_n(self, n: int = 50):
        bids = list(self.bids.items())[:n]
        asks = list(self.asks.items())[:n]
        return bids, asks