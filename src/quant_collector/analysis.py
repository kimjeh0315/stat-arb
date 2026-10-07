from collections import defaultdict, deque
from decimal import Decimal

from .domain import Context, number, stable_id

SECOND = 10 ** 9


def analytical_decimal(value):
    return number(value.quantize(Decimal("0.000000000000000001")))


class Analyzer:
    """Receive-time as-of joins: a late packet never changes an earlier analysis row."""

    def __init__(self, instruments, coverage, stale_seconds=15):
        self.instruments = {i.instrument_id: i for i in instruments}
        self.coverage = coverage
        self.stale_ns = stale_seconds * SECOND
        self.history = defaultdict(deque)
        self.buckets = defaultdict(lambda: defaultdict(Decimal))
        self.interrupted = set()
        self.liquidation_interrupted = set()

    def remember(self, instrument_id, field, received, value):
        history = self.history[(instrument_id, field)]
        history.append((received, value))
        # Keep a predecessor plus two seconds of history, even if a tick is delayed.
        while len(history) > 1 and history[1][0] < received - 2 * SECOND:
            history.popleft()

    def ingest(self, event):
        instrument_id = event["instrument_id"]
        received = event["received_at_ns"]
        dataset = event["dataset"]
        if dataset == "book_state_20":
            slim = {k: event.get(k) for k in ("book_state_id", "book_valid", "bid_level_count", "ask_level_count")}
            slim["bid"] = event["bids"][0]["price"] if event["bids"] else None
            slim["ask"] = event["asks"][0]["price"] if event["asks"] else None
            self.remember(instrument_id, "book", received, slim)
        elif dataset == "price_event":
            self.remember(instrument_id, event["price_type"], received, event["price"])
        elif dataset == "oi_event":
            self.remember(instrument_id, "oi", received, event["oi_base_qty"])
        elif dataset == "funding_event" and event["rate_kind"] == "indicative":
            self.remember(instrument_id, "funding", received, event)
        elif dataset == "collection_status":
            if event["state"] in ("subscribed", "disconnected", "connecting"):
                self.remember(instrument_id, "connected", received, event["state"] == "subscribed")
                if event["state"] != "subscribed":
                    self.interrupted.add((instrument_id, received // SECOND))
                    self.remember(instrument_id, "book", received, None)
            if event["state"] in ("external_liquidation_connected", "external_liquidation_disconnected"):
                self.remember(instrument_id, "liquidation_connected", received,
                              event["state"] == "external_liquidation_connected")
                if event["state"] == "external_liquidation_disconnected":
                    self.liquidation_interrupted.add((instrument_id, received // SECOND))
        elif dataset == "public_trade" and not event.get("is_snapshot"):
            bucket = self.buckets[(instrument_id, received // SECOND)]
            qty, price = Decimal(event["base_qty"]), Decimal(event["price"])
            bucket["trade_volume"] += qty
            bucket["trade_notional"] += qty * price
            bucket["taker_" + event["aggressor_side"]] += qty
            # Event-time can be historical on initial subscriptions; ticker prices take precedence
            # at subsequent receives, while no future receives are included in an earlier row.
        elif dataset == "liquidation_event":
            qty = event.get("filled_qty")
            if qty is None:
                qty = event.get("order_qty")
            if qty is not None:
                self.buckets[(instrument_id, received // SECOND)]["liquidation"] += Decimal(qty)

    def latest(self, instrument_id, field, as_of):
        for received, value in reversed(self.history[(instrument_id, field)]):
            if received <= as_of:
                return value, (as_of - received) / 10 ** 6
        return None, None

    def rows(self, as_of):
        rows = []
        for instrument_id, instrument in self.instruments.items():
            flags = []
            book, book_age = self.latest(instrument_id, "book", as_of)
            connected, _ = self.latest(instrument_id, "connected", as_of)
            valid = bool(book and book["book_valid"] and connected and book_age * 10 ** 6 <= self.stale_ns)
            if not valid:
                flags.append("book_unavailable_or_stale")
            if book and (book["bid_level_count"] < 20 or book["ask_level_count"] < 20):
                flags.append("book_depth_below_20")
            mid, spread = None, None
            if valid:
                bid, ask = Decimal(book["bid"]), Decimal(book["ask"])
                mid = number((bid + ask) / 2)
                spread = analytical_decimal((ask - bid) / Decimal(mid) * 10000)
            values = {}
            for field in ("mark", "index", "oracle", "last"):
                value, age = self.latest(instrument_id, field, as_of)
                values[field + "_price"] = value
                values[field + "_age_ms"] = age
            oi, oi_age = self.latest(instrument_id, "oi", as_of)
            funding, funding_age = self.latest(instrument_id, "funding", as_of)
            bucket_key = (instrument_id, as_of // SECOND - 1)
            bucket = self.buckets.get(bucket_key, {})
            interrupted = bucket_key in self.interrupted
            trade_connected = bool(connected and not interrupted)
            coverage = self.coverage[instrument.exchange]
            external_connected, _ = self.latest(instrument_id, "liquidation_connected", as_of)
            liq_connected = (bool(external_connected) if coverage.startswith("external")
                             else bool(connected and coverage != "unavailable_public_api"))
            liq_interrupted = (bucket_key in self.liquidation_interrupted if coverage.startswith("external")
                               else interrupted)
            if coverage == "unavailable_public_api":
                flags.append("liquidation_feed_unavailable")
            if interrupted or not connected:
                flags.append("feed_gap")
            if liq_interrupted:
                flags.append("liquidation_feed_gap")
            fields = dict(
                as_of_ns=as_of, book_state_id=book["book_state_id"] if book else None,
                mid_price=mid, spread_bps=spread, oi_base_qty=oi, oi_age_ms=oi_age,
                book_age_ms=book_age, funding_age_ms=funding_age,
                funding_rate=funding["rate_decimal"] if funding else None,
                funding_interval_hours=funding["interval_hours"] if funding else None,
                next_funding_at_ns=funding["settlement_at_ns"] if funding else None,
                trade_volume_1s=number(bucket.get("trade_volume", 0)) if trade_connected else None,
                trade_notional_1s=analytical_decimal(Decimal(bucket.get("trade_notional", 0))) if trade_connected else None,
                taker_buy_volume_1s=number(bucket.get("taker_buy", 0)) if trade_connected else None,
                taker_sell_volume_1s=number(bucket.get("taker_sell", 0)) if trade_connected else None,
                reported_liquidation_volume_1s=number(bucket.get("liquidation", 0)) if liq_connected and not liq_interrupted else None,
                liquidation_coverage=coverage, trade_feed_connected=trade_connected,
                liquidation_feed_connected=liq_connected,
                **values,
            )
            ctx = Context(instrument.exchange, "analysis", "", as_of, 0)
            event = ctx.event("analysis_state_1s", instrument, as_of, **fields)
            event["event_id"] = stable_id(instrument_id, "analysis", as_of)
            event["quality_flags"] = flags
            event["raw_id"] = None
            rows.append(event)
        for key in list(self.buckets):
            if key[1] < as_of // SECOND - 2:
                del self.buckets[key]
        self.interrupted = {key for key in self.interrupted if key[1] >= as_of // SECOND - 2}
        self.liquidation_interrupted = {key for key in self.liquidation_interrupted
                                       if key[1] >= as_of // SECOND - 2}
        return rows
