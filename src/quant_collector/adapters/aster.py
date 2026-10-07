import asyncio
import time

from .base import Adapter, LOG
from ..domain import ns_from_ms, stable_id
from ..orderbook import SequenceGap


class AsterAdapter(Adapter):
    name = "aster"
    ws_url = "wss://fstream.asterdex.com/ws"
    rest_url = "https://fapi.asterdex.com"
    liquidation_coverage = "latest_per_symbol_1000ms"

    def __init__(self, *args):
        super().__init__(*args)
        self.pending = set()

    async def discover(self):
        data = await self.http.get(self.rest_url + "/fapi/v1/exchangeInfo")
        funding = await self.http.get(self.rest_url + "/fapi/v1/fundingInfo")
        intervals = {r["symbol"]: str(r["fundingIntervalHours"]) for r in funding}
        result = []
        for row in data["symbols"]:
            if row["status"] != "TRADING" or row["contractType"] != "PERPETUAL":
                continue
            if row["quoteAsset"] not in self.config.quote_assets:
                continue
            filters = {f["filterType"]: f for f in row["filters"]}
            item = self.universe.instrument(
                self.name, row["symbol"], row["baseAsset"], row["quoteAsset"], row["marginAsset"],
                tick_size=filters["PRICE_FILTER"]["tickSize"],
                qty_step=filters["LOT_SIZE"]["stepSize"], min_qty=filters["LOT_SIZE"]["minQty"],
                funding_interval_hours=intervals.get(row["symbol"]))
            if item:
                result.append(item)
        return result

    def subscriptions(self, symbols):
        return [{"method": "SUBSCRIBE", "id": 1,
                 "params": [symbol.lower() + "@" + channel
                            for symbol in symbols for channel in
                            ("depth@100ms", "aggTrade", "markPrice@1s", "forceOrder")]}]

    def reset(self, symbols):
        super().reset(symbols)
        self.pending.difference_update(symbols)

    async def prepare(self, symbols, sink, session_id):
        # The WS transport buffers updates while REST snapshots are fetched.
        for symbol in symbols:
            data = await self.http.get(self.rest_url + "/fapi/v1/depth",
                                       params={"symbol": symbol, "limit": 1000})
            def seed(body, ctx, symbol=symbol):
                self.books[symbol].snapshot(body["bids"], body["asks"], body["lastUpdateId"])
                self.books[symbol].valid = False
                self.pending.add(symbol)
                return []
            await sink.rest(self, "book_snapshot", symbol, data, seed, session_id)

    def handle(self, message, ctx):
        if message.get("code") is not None:
            raise ValueError("Aster subscription error")
        data = message.get("data", message)
        kind = data.get("e")
        symbol = data.get("s") or data.get("o", {}).get("s")
        instrument = self.instruments.get(symbol)
        if instrument is None:
            return []
        stamp = ns_from_ms(data.get("T", data.get("E")))
        if kind == "depthUpdate":
            book = self.books[symbol]
            if int(data["u"]) < int(book.sequence):
                return []
            if symbol in self.pending:
                # Accept the snapshot update itself or the immediately following range.
                if not int(data["U"]) <= int(book.sequence) + 1 <= int(data["u"]) + 1:
                    raise SequenceGap("Aster snapshot/delta bridge missing")
                book._apply(data["b"], data["a"])
                book.sequence = str(data["u"])
                self.pending.remove(symbol)
            elif not book.delta(data["b"], data["a"], data["u"], data["pu"]):
                return []
            return self.book_event(symbol, ctx, stamp)
        if kind == "aggTrade":
            return [self.trade(instrument, ctx, stamp, data["a"], data["p"], data["q"],
                               "sell" if data["m"] else "buy", aggregated=True)] + self.prices(
                                   instrument, ctx, stamp, {"last": data["p"]})
        if kind == "markPriceUpdate":
            return self.prices(instrument, ctx, ns_from_ms(data.get("E")),
                               {"mark": data.get("p"), "index": data.get("i")}) + self.funding(
                instrument, ctx, ns_from_ms(data.get("E")), data.get("r"), ns_from_ms(data.get("T")))
        if kind == "forceOrder":
            order = data["o"]
            event = ctx.event("liquidation_event", instrument, ns_from_ms(order["T"]),
                              order_side=order["S"].lower(),
                              liquidated_position_side="long" if order["S"] == "SELL" else "short",
                              order_qty=instrument.qty(order["q"]), filled_qty=instrument.qty(order["z"]),
                              reported_price=instrument.price(order["ap"]), price_kind="average_fill",
                              coverage_type=self.liquidation_coverage)
            event["event_id"] = stable_id(instrument.instrument_id, "liquidation", order)
            return [event]
        return []

    async def oi_loop(self, sink, stop):
        while not stop.is_set():
            for symbol, instrument in self.instruments.items():
                if stop.is_set():
                    return
                try:
                    data = await self.http.get(self.rest_url + "/fapi/v1/openInterest",
                                               params={"symbol": symbol})
                    await sink.rest(self, "open_interest", symbol, data,
                                    lambda body, ctx, i=instrument: self.oi(
                                        i, ctx, ns_from_ms(body.get("time")), body["openInterest"]))
                except Exception as exc:
                    LOG.warning("Aster OI failed for %s: %s", symbol, type(exc).__name__)
                    await sink.status(self.name, "rest", "oi_poll_failed", [symbol], type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.aster_oi_poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def poll_funding(self, instrument, since_ns):
        return await self.http.get(self.rest_url + "/fapi/v1/fundingRate",
                                   params={"symbol": instrument.native_symbol,
                                           "startTime": since_ns // 10 ** 6, "limit": 1000})

    def parse_funding(self, instrument, message, ctx):
        events = []
        for row in message:
            stamp = ns_from_ms(row["fundingTime"])
            events += self.funding(instrument, ctx, stamp, row["fundingRate"], stamp, "settled")
        return events
