"""Optional provider bridge. Providers must implement the documented normalized JSON contract."""
import asyncio
import json
import logging
import os
import time
from uuid import uuid4

import aiohttp

from .domain import stable_id

LOG = logging.getLogger(__name__)


class ExternalLiquidations:
    def __init__(self, spec, instruments):
        self.spec = spec
        self.name = spec["exchange"]
        self.instruments = {i.native_symbol: i for i in instruments if i.exchange == self.name}

    def handle(self, message, ctx):
        result = []
        for item in message if isinstance(message, list) else [message]:
            instrument = self.instruments.get(item.get("native_symbol"))
            if instrument is None:
                continue
            if item["order_side"] not in ("buy", "sell"):
                raise ValueError("External liquidation order_side must be buy/sell")
            event = ctx.event("liquidation_event", instrument, item.get("event_time_ns"),
                              order_side=item["order_side"],
                              liquidated_position_side=item.get("liquidated_position_side"),
                              order_qty=instrument.qty(item.get("order_qty")),
                              filled_qty=instrument.qty(item.get("filled_qty")),
                              reported_price=instrument.price(item.get("reported_price")),
                              price_kind=item["price_kind"], coverage_type=self.spec["coverage_type"])
            event["event_id"] = stable_id(self.name, "external_liquidation", item["id"])
            result.append(event)
        return result

    async def run(self, session, sink, stop):
        url = os.environ[self.spec["url_env"]]
        headers = {}
        if self.spec.get("token_env"):
            headers["Authorization"] = "Bearer " + os.environ[self.spec["token_env"]]
        delay = 1
        while not stop.is_set():
            session_id = "external-" + uuid4().hex
            try:
                async with session.ws_connect(url, headers=headers, heartbeat=20,
                                              receive_timeout=60) as ws:
                    if self.spec.get("subscribe"):
                        await ws.send_json(self.spec["subscribe"])
                    await sink.status(self.name, session_id, "external_liquidation_connected", list(self.instruments))
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            ctx = sink.context(self.name, session_id)
                            try:
                                events = self.handle(json.loads(message.data), ctx)
                            except Exception:
                                await sink.queue.put(sink.envelope(ctx, "external_liquidations", message.data, []))
                                raise
                            await sink.queue.put(sink.envelope(ctx, "external_liquidations", message.data, events))
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError("External websocket error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("External %s liquidation reconnect: %s", self.name, type(exc).__name__)
            finally:
                await sink.status(self.name, session_id, "external_liquidation_disconnected", list(self.instruments))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)
