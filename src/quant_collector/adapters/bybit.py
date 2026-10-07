from decimal import Decimal

from .base import Adapter
from ..domain import ns_from_ms, stable_id


class BybitAdapter(Adapter):
    name = "bybit"
    ws_url = "wss://stream.bybit.com/v5/public/linear"
    ws_ping = {"op": "ping"}
    liquidation_coverage = "exchange_all_liquidation"
    rest_url = "https://api.bybit.com"

    async def discover(self):
        result, cursor = [], ""
        while True:
            data = await self.http.get(self.rest_url + "/v5/market/instruments-info",
                                       params={"category": "linear", "limit": 1000, "cursor": cursor})
            for row in data["result"]["list"]:
                if row["status"] != "Trading" or row["contractType"] != "LinearPerpetual":
                    continue
                if row["quoteCoin"] not in self.config.quote_assets:
                    continue
                lot = row["lotSizeFilter"]
                item = self.universe.instrument(
                    self.name, row["symbol"], row["baseCoin"], row["quoteCoin"], row["settleCoin"],
                    tick_size=row["priceFilter"]["tickSize"], qty_step=lot["qtyStep"],
                    min_qty=lot["minOrderQty"],
                    funding_interval_hours=str(Decimal(str(row["fundingInterval"])) / 60))
                if item:
                    result.append(item)
            cursor = data["result"].get("nextPageCursor", "")
            if not cursor:
                return result

    def subscriptions(self, symbols):
        return [{"op": "subscribe", "args": ["%s.%s" % (channel, symbol)
                 for symbol in symbols for channel in
                 ("orderbook.50", "publicTrade", "tickers", "allLiquidation")]}]

    def handle(self, message, ctx):
        if message.get("success") is False:
            raise ValueError("Subscription rejected")
        topic = message.get("topic", "")
        if not topic:
            return []
        symbol = topic.split(".")[-1]
        instrument = self.instruments.get(symbol)
        if not instrument:
            return []
        data = message["data"]
        if topic.startswith("orderbook."):
            book = self.books[symbol]
            if message["type"] == "snapshot" or data.get("u") == 1:
                book.snapshot(data["b"], data["a"], data["u"])
            elif not book.delta(data["b"], data["a"], data["u"]):
                return []
            # Bybit u is monotonic, not guaranteed to increase by exactly one.
            return self.book_event(symbol, ctx, ns_from_ms(data.get("cts", message.get("cts", message["ts"]))))
        if topic.startswith("publicTrade."):
            return [self.trade(instrument, ctx, ns_from_ms(t["T"]), t["i"],
                               t["p"], t["v"], t["S"].lower()) for t in data]
        if topic.startswith("tickers."):
            timestamp = ns_from_ms(message.get("ts"))
            events = self.prices(instrument, ctx, timestamp,
                                 {k: data.get(v) for k, v in
                                  (("mark", "markPrice"), ("index", "indexPrice"), ("last", "lastPrice"))})
            events += self.oi(instrument, ctx, timestamp, data.get("openInterest"))
            events += self.funding(instrument, ctx, timestamp, data.get("fundingRate"),
                                   ns_from_ms(data.get("nextFundingTime")))
            return events
        if topic.startswith("allLiquidation."):
            events = []
            for item in data:
                event = ctx.event("liquidation_event", instrument, ns_from_ms(item["T"]),
                                  order_side="sell" if item["S"] == "Buy" else "buy",
                                  liquidated_position_side="long" if item["S"] == "Buy" else "short",
                                  order_qty=instrument.qty(item["v"]), filled_qty=None,
                                  reported_price=instrument.price(item["p"]), price_kind="bankruptcy",
                                  coverage_type=self.liquidation_coverage)
                event["event_id"] = stable_id(instrument.instrument_id, "liquidation", item)
                events.append(event)
            return events
        return []

    async def poll_funding(self, instrument, since_ns):
        return await self.http.get(self.rest_url + "/v5/market/funding/history",
                                   params={"category": "linear", "symbol": instrument.native_symbol,
                                           "startTime": since_ns // 10 ** 6,
                                           "endTime": __import__("time").time_ns() // 10 ** 6, "limit": 200})

    def parse_funding(self, instrument, message, ctx):
        events = []
        for row in message["result"]["list"]:
            stamp = ns_from_ms(row["fundingRateTimestamp"])
            events += self.funding(instrument, ctx, stamp, row["fundingRate"], stamp, "settled")
        return events
