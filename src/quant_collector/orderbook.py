from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sortedcontainers import SortedDict

from .domain import number


class SequenceGap(Exception):
    pass


class OrderBook:
    """Keep the complete subscribed depth; truncate only the exported state."""

    def __init__(self):
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.sequence = None
        self.epoch = uuid4().hex
        self.initialized = False
        self.valid = False
        self.last_export = None

    def snapshot(self, bids: List[Any], asks: List[Any], sequence: Any = None,
                 reset_epoch: bool = True):
        self.bids.clear()
        self.asks.clear()
        if reset_epoch:
            self.epoch = uuid4().hex
        self.last_export = None
        self.initialized = True
        self._apply(bids, asks)
        self.sequence = str(sequence) if sequence is not None else None

    def delta(self, bids: List[Any], asks: List[Any], sequence: Any = None,
              previous: Any = None) -> bool:
        if not self.initialized:
            raise SequenceGap("Delta arrived before snapshot")
        seq = str(sequence) if sequence is not None else None
        if seq is not None and self.sequence is not None:
            if int(seq) <= int(self.sequence):
                return False
            if previous is not None and str(previous) != self.sequence:
                self.valid = False
                raise SequenceGap("Previous sequence does not match")
        self._apply(bids, asks)
        self.sequence = seq
        return True

    def _apply(self, bids, asks):
        for target, levels in ((self.bids, bids), (self.asks, asks)):
            for level in levels:
                price, size = Decimal(str(level[0])), Decimal(str(level[1]))
                if not price.is_finite() or not size.is_finite() or price <= 0 or size < 0:
                    self.valid = False
                    raise ValueError("Invalid orderbook level")
                if size == 0:
                    target.pop(price, None)
                else:
                    target[price] = size
        self.valid = bool(self.bids and self.asks and self.bids.peekitem(-1)[0] < self.asks.peekitem(0)[0])

    def export(self, instrument, depth: int = 20) -> Dict[str, Any]:
        bid_keys = list(self.bids.islice(max(0, len(self.bids) - depth), len(self.bids)))[::-1]
        ask_keys = list(self.asks.islice(0, min(depth, len(self.asks))))
        return dict(
            bids=[dict(price=instrument.price(p), qty=instrument.qty(self.bids[p])) for p in bid_keys],
            asks=[dict(price=instrument.price(p), qty=instrument.qty(self.asks[p])) for p in ask_keys],
            book_valid=self.valid, book_epoch_id=self.epoch,
            source_sequence=self.sequence, bid_level_count=len(bid_keys), ask_level_count=len(ask_keys),
        )

    def changed_export(self, instrument, depth: int = 20) -> Optional[Dict[str, Any]]:
        state = self.export(instrument, depth)
        signature = (state["bids"], state["asks"], state["book_valid"])
        if signature == self.last_export:
            return None
        self.last_export = signature
        return state
