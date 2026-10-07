import asyncio
import json
import logging
import time
from collections import OrderedDict
from uuid import uuid4

from .domain import Context
from .orderbook import SequenceGap

LOG = logging.getLogger(__name__)


class Dedupe:
    def __init__(self, capacity=200000):
        self.capacity = capacity
        self.ids = OrderedDict()

    def accept(self, event):
        if event["dataset"] not in ("public_trade", "liquidation_event", "funding_event"):
            return True
        if event["dataset"] == "funding_event" and event["rate_kind"] != "settled":
            return True
        key = event["event_id"]
        if key in self.ids:
            self.ids.move_to_end(key)
            return False
        self.ids[key] = None
        if len(self.ids) > self.capacity:
            self.ids.popitem(last=False)
        return True


class Pipeline:
    def __init__(self, journal, analyzer, instruments, capacity):
        self.journal, self.analyzer = journal, analyzer
        self.instruments = {(i.exchange, i.native_symbol): i for i in instruments}
        self.queue = asyncio.Queue(maxsize=capacity)
        self.dedupe = Dedupe()
        self.accepting = True

    def context(self, exchange, session_id):
        return Context(exchange, session_id, uuid4().hex, time.time_ns(), time.monotonic_ns())

    def envelope(self, ctx, channel, payload, events, source="websocket"):
        return dict(raw=dict(raw_id=ctx.raw_id, exchange=ctx.exchange, session_id=ctx.session_id,
                             received_at_ns=ctx.received_at_ns, received_mono_ns=ctx.received_mono_ns,
                             channel=channel, source=source, payload=payload), events=events)

    async def message(self, adapter, session_id, payload):
        ctx = self.context(adapter.name, session_id)
        channel = "unparsed"
        try:
            message = json.loads(payload)
            channel = message.get("topic", message.get("stream", message.get("channel", "control")))
            events = adapter.handle(message, ctx)
        except Exception as exc:
            # Keep the failing packet before resetting the connection/book.
            await self.queue.put(self.envelope(ctx, channel, payload, []))
            LOG.warning("%s parser/reset: %s", adapter.name, type(exc).__name__)
            raise
        await self.queue.put(self.envelope(ctx, channel, payload, events))

    async def rest(self, adapter, channel, symbol, data, parser, session_id="rest"):
        ctx = self.context(adapter.name, session_id)
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        try:
            events = parser(data, ctx)
        except Exception:
            await self.queue.put(self.envelope(ctx, channel + ":" + symbol, payload, [], "rest"))
            raise
        await self.queue.put(self.envelope(ctx, channel + ":" + symbol, payload, events, "rest"))
        return events

    async def status(self, exchange, session_id, state, symbols, detail=""):
        if not self.accepting:
            return
        ctx = self.context(exchange, session_id)
        events = [ctx.event("collection_status", self.instruments[(exchange, s)], None,
                            state=state, detail=detail)
                  for s in symbols if (exchange, s) in self.instruments]
        await self.queue.put(self.envelope(ctx, "collector_status", json.dumps(
            dict(state=state, symbols=symbols, detail=detail)), events, "collector"))

    async def metadata(self, adapters, report):
        for adapter in adapters:
            ctx = self.context(adapter.name, "discovery")
            events = []
            for instrument in adapter.instruments.values():
                row = instrument.row()
                fields = {k: row[k] for k in ("native_symbol", "native_base", "settle_asset", "multiplier",
                                              "tick_size", "qty_step", "min_qty", "funding_interval_hours")}
                events.append(ctx.event("instrument_metadata", instrument, None, **fields))
            await self.queue.put(self.envelope(ctx, "discovery", json.dumps(report[adapter.name],
                                 ensure_ascii=False), events, "rest"))

    async def ticks(self, stop):
        previous = None
        while not stop.is_set():
            now = time.time_ns()
            target = (now // 10 ** 9 + 1) * 10 ** 9
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0, (target - now) / 10 ** 9))
                return
            except asyncio.TimeoutError:
                pass
            actual = time.time_ns() // 10 ** 9 * 10 ** 9
            if actual != previous:
                await self.queue.put({"tick_ns": actual})
                previous = actual

    async def consume(self):
        while True:
            first = await self.queue.get()
            if first is None:
                self.queue.task_done()
                return
            batch = [first]
            while len(batch) < 256 and not self.queue.empty():
                # Shutdown sentinel is only enqueued after queue.join().
                batch.append(self.queue.get_nowait())
            envelopes = []
            for item in batch:
                if "tick_ns" in item:
                    envelopes.append({"events": self.analyzer.rows(item["tick_ns"])})
                else:
                    item["events"] = [event for event in item["events"] if self.dedupe.accept(event)]
                    for event in item["events"]:
                        self.analyzer.ingest(event)
                    envelopes.append(item)
            await asyncio.to_thread(self.journal.append_many, envelopes)
            for _ in batch:
                self.queue.task_done()

    async def close(self):
        await self.queue.join()
        await self.queue.put(None)
