from decimal import Decimal

from .base import Adapter
from ..orderbook import SequenceGap


class GrvtAdapter(Adapter):
    name = "grvt"
    ws_url = "wss://market-data.grvt.io/ws/full"
    rest_url = "https://market-data.grvt.io/full/v1"

    def __init__(self, *args):
        super().__init__(*args)
        self.snapshot_remaining = {}
        self.snapshot_sequence = {}

    async def discover(self):
        data = await self.http.post(self.rest_url + "/all_instruments",
                                    {"is_active": True, "kinds": ["PERPETUAL"]})
        result = []
        for row in data["result"]:
            if row["quote"] not in self.config.quote_assets or "ORDERBOOK" not in row["venues"]:
                continue
            if row.get("asset_class", "CRYPTO") != "CRYPTO":
                continue
            item = self.universe.instrument(
                self.name, row["instrument"], row["base"], row["quote"], row["quote"],
                tick_size=row["tick_size"], qty_step=str(Decimal(10) ** -int(row["base_decimals"])),
                min_qty=row["min_size"], funding_interval_hours=str(row["funding_interval_hours"]))
            if item:
                result.append(item)
        return result

    def reset(self, symbols):
        super().reset(symbols)
        for symbol in symbols:
            self.snapshot_remaining.pop(symbol, None)
            self.snapshot_sequence.pop(symbol, None)

    def subscriptions(self, symbols):
        return [{"jsonrpc": "2.0", "method": "subscribe", "id": index,
                 "params": {"stream": stream, "selectors": [s + suffix for s in symbols]}}
                for index, (stream, suffix) in enumerate(
                    (("v1.book.d", "@50"), ("v1.ticker.s", "@500"), ("v1.trade", "@50")), 1)]

    def handle(self, message, ctx):
        if message.get("error") or message.get("code"):
            raise ValueError("GRVT subscription error")
        if "result" in message:
            result = message["result"]
            if result.get("stream") == "v1.book.d":
                for index, (selector, count) in enumerate(zip(result.get("subs", []), result.get("num_snapshots", []))):
                    symbol = selector.split("@")[0]
                    self.snapshot_remaining[symbol] = int(count)
                    latest = result.get("latest_sequence_number", [])
                    first = result.get("first_sequence_number", [])
                    if index < len(latest):
                        self.snapshot_sequence[symbol] = str(latest[index])
                    elif index < len(first):
                        self.snapshot_sequence[symbol] = str(int(first[index]) - 1)
            return []
        stream = message.get("stream", "")
        if "feed" not in message:
            return []
        data = message["feed"]
        if isinstance(data, list):
            events = []
            for row in data:
                events += self.handle(dict(message, feed=row), ctx)
            return events
        symbol = data.get("instrument", message.get("selector", "").split("@")[0])
        instrument = self.instruments.get(symbol)
        if instrument is None:
            return []
        stamp = int(data["event_time"]) if data.get("event_time") is not None else None
        if stream == "v1.book.d":
            bids = [[r["price"], r["size"]] for r in data.get("bids", [])]
            asks = [[r["price"], r["size"]] for r in data.get("asks", [])]
            book = self.books[symbol]
            remaining = self.snapshot_remaining.get(symbol)
            seq = message.get("sequence_number")
            if not book.initialized:
                if remaining is None:
                    raise SequenceGap("GRVT snapshot acknowledgement missing")
                book.snapshot(bids, asks, seq)
            elif remaining and remaining > 0:
                book._apply(bids, asks)
                book.sequence = str(seq)
            else:
                if not book.delta(bids, asks, seq, message.get("prev_sequence_number")):
                    return []
            if remaining and remaining > 0:
                self.snapshot_remaining[symbol] = remaining - 1
                if remaining > 1:
                    book.valid = False
                    return []
                # GRVT startup snapshot packets use sequence=0. The acknowledgement
                # supplies the predecessor needed to validate the first live delta.
                if str(seq) == "0" and symbol in self.snapshot_sequence:
                    book.sequence = self.snapshot_sequence[symbol]
            return self.book_event(symbol, ctx, stamp)
        if stream == "v1.trade":
            trade = self.trade(instrument, ctx, stamp, data["trade_id"], data["price"],
                               data["size"], "buy" if data["is_taker_buyer"] else "sell",
                               snapshot=bool(message.get("is_snapshot", False) or
                                             str(message.get("sequence_number")) == "0"))
            return [trade] + self.prices(instrument, ctx, stamp,
                                        {"last": data["price"], "mark": data.get("mark_price"),
                                         "index": data.get("index_price")})
        if stream == "v1.ticker.s":
            funding = self.funding(
                instrument, ctx, stamp, data.get("funding_rate"),
                int(data["next_funding_time"]) if data.get("next_funding_time") else None,
                divisor="100" if self.config.grvt_live_funding_unit == "percentage_points" else "1000000")
            for event in funding:
                event["quality_flags"].append("grvt_live_funding_unit_configured")
            return self.prices(instrument, ctx, stamp,
                               {"mark": data.get("mark_price"), "index": data.get("index_price"),
                                "last": data.get("last_price")}) + self.oi(
                instrument, ctx, stamp, data.get("open_interest")) + funding
        return []

    async def poll_funding(self, instrument, since_ns):
        return await self.http.post(self.rest_url + "/funding",
                                    {"instrument": instrument.native_symbol, "start_time": str(since_ns),
                                     "limit": 1000, "agg_type": "FUNDING_INTERVAL"})

    def parse_funding(self, instrument, message, ctx):
        events = []
        for row in message["result"]:
            stamp = int(row["funding_time"])
            events += self.funding(instrument, ctx, stamp, row["funding_rate"], stamp,
                                   "settled", divisor="100", interval=row.get("funding_interval_hours"))
        return events
