import asyncio
import json
import logging
import random
import time
from typing import Any, Dict, List
from uuid import uuid4

import aiohttp

from ..domain import Context, number, stable_id
from ..orderbook import OrderBook

LOG = logging.getLogger(__name__)


class Adapter:
    name = ""
    ws_url = ""
    liquidation_coverage = "unavailable_public_api"
    ws_ping = None

    def __init__(self, config, universe, http):
        self.config, self.universe, self.http = config, universe, http
        self.instruments = {}
        self.books = {}
        self.session_started = {}

    async def discover(self):
        raise NotImplementedError

    def configure(self, instruments):
        self.instruments = {i.native_symbol: i for i in instruments}
        self.books = {s: OrderBook() for s in self.instruments}

    def subscriptions(self, symbols):
        raise NotImplementedError

    def handle(self, message, ctx):
        raise NotImplementedError

    def reset(self, symbols):
        for symbol in symbols:
            self.books[symbol] = OrderBook()

    def book_event(self, symbol, ctx, timestamp):
        state = self.books[symbol].export(self.instruments[symbol])
        event = ctx.event("book_state_20", self.instruments[symbol], timestamp, **state)
        event["book_state_id"] = event["event_id"]
        return [event]

    def prices(self, instrument, ctx, timestamp, values):
        return [ctx.event("price_event", instrument, timestamp,
                          price_type=kind, price=instrument.price(value))
                for kind, value in values.items() if value is not None and value != ""]

    def oi(self, instrument, ctx, timestamp, value):
        if value is None or value == "":
            return []
        return [ctx.event("oi_event", instrument, timestamp, native_value=number(value),
                          native_unit=instrument.native_base, oi_base_qty=instrument.qty(value))]

    def funding(self, instrument, ctx, timestamp, value, settlement_at,
                rate_kind="indicative", divisor="1", interval=None):
        if value is None or value == "":
            return []
        from decimal import Decimal
        rate = number(Decimal(str(value)) / Decimal(divisor))
        event = ctx.event("funding_event", instrument, timestamp, rate_kind=rate_kind,
                          rate_decimal=rate, settlement_at_ns=settlement_at,
                          interval_hours=number(interval or instrument.funding_interval_hours),
                          native_rate=number(value),
                          native_rate_unit={"1": "decimal", "100": "percentage_points",
                                            "1000000": "centibeeps"}[divisor])
        if rate_kind == "settled":
            event["event_id"] = stable_id(instrument.instrument_id, "funding", settlement_at, rate_kind)
        return [event]

    def trade(self, instrument, ctx, timestamp, trade_id, price, qty, side,
              aggregated=False, snapshot=False):
        if timestamp is not None and timestamp < self.session_started.get(ctx.session_id, 0):
            snapshot = True
        event = ctx.event("public_trade", instrument, timestamp,
                          native_trade_id=str(trade_id), price=instrument.price(price),
                          base_qty=instrument.qty(qty), aggressor_side=side,
                          is_aggregated=aggregated, is_snapshot=snapshot)
        event["event_id"] = stable_id(instrument.instrument_id, "trade", trade_id)
        return event

    async def run(self, sink, stop):
        symbols = list(self.instruments)
        size = self.config.symbols_per_connection
        chunks = [symbols[i:i + size] for i in range(0, len(symbols), size)]
        await asyncio.gather(*(self.connection(chunk, sink, stop) for chunk in chunks))

    async def prepare(self, symbols, sink, session_id):
        pass

    async def connection(self, symbols, sink, stop):
        delay = 1
        while not stop.is_set():
            session_id = uuid4().hex
            self.reset(symbols)
            await sink.status(self.name, session_id, "connecting", symbols)
            ping_task = None
            try:
                async with self.http.session.ws_connect(
                    self.ws_url, heartbeat=20, receive_timeout=60,
                    max_msg_size=32 * 1024 * 1024,
                ) as ws:
                    self.session_started[session_id] = time.time_ns()
                    await sink.status(self.name, session_id, "connected", symbols)
                    for message in self.subscriptions(symbols):
                        await ws.send_json(message)
                        await asyncio.sleep(0.15)
                    await self.prepare(symbols, sink, session_id)
                    await sink.status(self.name, session_id, "subscribed", symbols)
                    if self.ws_ping is not None:
                        ping_task = asyncio.create_task(self.ping(ws))
                    connected_at = time.monotonic()
                    async for incoming in ws:
                        if stop.is_set():
                            break
                        if incoming.type == aiohttp.WSMsgType.TEXT:
                            await sink.message(self, session_id, incoming.data)
                        elif incoming.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError("Websocket transport error")
                    if time.monotonic() - connected_at > 60:
                        delay = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("%s reconnect: %s", self.name, type(exc).__name__)
                await sink.status(self.name, session_id, "disconnected", symbols,
                                  type(exc).__name__)
            finally:
                self.session_started.pop(session_id, None)
                if ping_task:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
                await sink.status(self.name, session_id, "disconnected", symbols)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay + random.random())
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)

    async def ping(self, ws):
        while True:
            await asyncio.sleep(20)
            await ws.send_json(self.ws_ping)

    async def poll_funding(self, instrument, since_ns):
        raise NotImplementedError

    def parse_funding(self, instrument, message, ctx):
        raise NotImplementedError

    async def funding_loop(self, sink, stop):
        # Per-symbol cursors are in-memory; restart lookback safely overlaps by stable event IDs.
        since = {s: time.time_ns() - self.config.funding_lookback_hours * 3600 * 10 ** 9
                 for s in self.instruments}
        while not stop.is_set():
            for symbol, instrument in self.instruments.items():
                if stop.is_set():
                    return
                try:
                    data = await self.poll_funding(instrument, since[symbol])
                    events = await sink.rest(self, "funding_history", symbol, data,
                                             lambda body, ctx: self.parse_funding(instrument, body, ctx))
                    stamps = [e["settlement_at_ns"] for e in events if e.get("settlement_at_ns")]
                    if stamps:
                        since[symbol] = max(stamps) + 1
                except Exception as exc:
                    LOG.warning("%s funding poll failed for %s: %s", self.name, symbol,
                                type(exc).__name__)
                    await sink.status(self.name, "rest", "funding_poll_failed", [symbol],
                                      type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.funding_poll_seconds)
            except asyncio.TimeoutError:
                pass
