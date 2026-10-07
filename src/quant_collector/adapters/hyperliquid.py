from decimal import Decimal

from .base import Adapter
from ..domain import ns_from_ms


class HyperliquidAdapter(Adapter):
    name = "hyperliquid"
    ws_url = "wss://api.hyperliquid.xyz/ws"
    rest_url = "https://api.hyperliquid.xyz/info"
    ws_ping = {"method": "ping"}

    async def discover(self):
        data = await self.http.post(self.rest_url, {"type": "meta"})
        result = []
        for row in data["universe"]:
            if row.get("isDelisted"):
                continue
            item = self.universe.instrument(
                self.name, row["name"], row["name"], "USDC", "USDC",
                qty_step=str(Decimal(10) ** -int(row["szDecimals"])), funding_interval_hours="1")
            if item:
                result.append(item)
        return result

    def subscriptions(self, symbols):
        return [{"method": "subscribe", "subscription": dict(type=channel, coin=symbol, **extra)}
                for symbol in symbols for channel, extra in
                (("l2Book", {"fast": False}), ("trades", {}), ("activeAssetCtx", {}))]

    def handle(self, message, ctx):
        channel = message.get("channel", "")
        if channel == "error":
            raise ValueError("Hyperliquid subscription error")
        data = message.get("data", {})
        if channel == "trades":
            events = []
            for row in data:
                instrument = self.instruments.get(row["coin"])
                if instrument:
                    stamp = ns_from_ms(row["time"])
                    events.append(self.trade(instrument, ctx, stamp, row["tid"], row["px"],
                                             row["sz"], "buy" if row["side"] == "B" else "sell"))
                    events += self.prices(instrument, ctx, stamp, {"last": row["px"]})
            return events
        symbol = data.get("coin") if isinstance(data, dict) else None
        instrument = self.instruments.get(symbol)
        if instrument is None:
            return []
        if channel == "l2Book":
            bids, asks = data["levels"]
            self.books[symbol].snapshot([[r["px"], r["sz"]] for r in bids],
                                        [[r["px"], r["sz"]] for r in asks],
                                        reset_epoch=not self.books[symbol].initialized)
            return self.book_event(symbol, ctx, ns_from_ms(data["time"]))
        if channel == "activeAssetCtx":
            values = data["ctx"]
            # This feed has no exchange event timestamp. Do not invent one.
            next_funding = (ctx.received_at_ns // (3600 * 10 ** 9) + 1) * 3600 * 10 ** 9
            events = self.prices(instrument, ctx, None,
                                 {"mark": values.get("markPx"), "oracle": values.get("oraclePx")})
            events += self.oi(instrument, ctx, None, values.get("openInterest"))
            events += self.funding(instrument, ctx, None, values.get("funding"), next_funding)
            for event in events:
                event["quality_flags"].append("event_time_unavailable")
            return events
        return []

    async def poll_funding(self, instrument, since_ns):
        return await self.http.post(self.rest_url, {"type": "fundingHistory",
                                                  "coin": instrument.native_symbol,
                                                  "startTime": since_ns // 10 ** 6})

    def parse_funding(self, instrument, message, ctx):
        events = []
        for row in message:
            stamp = ns_from_ms(row["time"])
            events += self.funding(instrument, ctx, stamp, row["fundingRate"], stamp, "settled")
        return events
