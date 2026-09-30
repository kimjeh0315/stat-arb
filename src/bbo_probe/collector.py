import asyncio
import contextlib
import json
import random
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, TextIO

import websockets

from .exchanges.base import ExchangeAdapter, InvalidMessage, ReconnectRequired
from .models import BBO


class QualityTracker:
    def __init__(self) -> None:
        self.counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.last_sequence: Dict[str, int] = {}
        self.last_recv_mono_ns: Dict[str, int] = {}
        self.event_ages_ms: Dict[str, List[float]] = defaultdict(list)
        self.interarrival_ms: Dict[str, List[float]] = defaultdict(list)

    def observe(self, bbo: BBO) -> None:
        stats = self.counts[bbo.exchange]
        stats["messages"] += 1
        if not bbo.valid:
            stats["invalid_or_crossed"] += 1
        if bbo.exchange_ts_ns is not None:
            age_ms = (bbo.recv_ts_ns - bbo.exchange_ts_ns) / 1_000_000
            self.event_ages_ms[bbo.exchange].append(age_ms)
            if age_ms < 0:
                stats["negative_event_age"] += 1
        stream_key = f"{bbo.exchange}:{bbo.instrument}"
        last_recv = self.last_recv_mono_ns.get(stream_key)
        if last_recv is not None:
            self.interarrival_ms[bbo.exchange].append((bbo.recv_mono_ns - last_recv) / 1_000_000)
        self.last_recv_mono_ns[stream_key] = bbo.recv_mono_ns
        if bbo.sequence is None:
            return
        # GRVT documents sequence 0 as snapshot; the following live sequence need not follow 0.
        if bbo.sequence == 0:
            stats["snapshots"] += 1
            self.last_sequence.pop(stream_key, None)
            return
        previous = self.last_sequence.get(stream_key)
        if previous is not None and bbo.sequence == previous:
            stats["duplicate_sequence"] += 1
        elif previous is not None and bbo.sequence < previous:
            stats["sequence_regression"] += 1
        if bbo.prev_sequence is not None and previous is not None and bbo.prev_sequence != previous:
            stats["sequence_gap"] += 1
        self.last_sequence[stream_key] = bbo.sequence

    @staticmethod
    def _percentile(values: List[float], fraction: float) -> float:
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(len(ordered) * fraction)))
        return round(ordered[index], 3)

    def summary(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        venues = set(self.counts) | set(self.event_ages_ms) | set(self.interarrival_ms)
        for venue in venues:
            stats: Dict[str, Any] = dict(self.counts[venue])
            ages = self.event_ages_ms[venue]
            intervals = self.interarrival_ms[venue]
            if ages:
                stats["event_age_ms"] = {
                    "p50": self._percentile(ages, 0.50),
                    "p95": self._percentile(ages, 0.95),
                    "p99": self._percentile(ages, 0.99),
                    "max": round(max(ages), 3),
                }
            if intervals:
                stats["interarrival_ms"] = {
                    "p50": self._percentile(intervals, 0.50),
                    "p95": self._percentile(intervals, 0.95),
                    "p99": self._percentile(intervals, 0.99),
                    "max": round(max(intervals), 3),
                }
            result[venue] = stats
        return result


async def _send_heartbeats(adapter: ExchangeAdapter, websocket: Any) -> None:
    interval = adapter.heartbeat_interval
    if interval is None:
        return
    while True:
        await asyncio.sleep(interval)
        message = adapter.heartbeat_message()
        if message is not None:
            await websocket.send(json.dumps(message))


async def collect_adapter(adapter: ExchangeAdapter, output: TextIO, tracker: QualityTracker,
                          stop_at: float, latest: Dict[str, BBO]) -> None:
    attempts = 0
    while time.monotonic() < stop_at:
        try:
            async with websockets.connect(adapter.url, ping_interval=20, ping_timeout=20,
                                          open_timeout=10, close_timeout=5) as ws:
                attempts = 0
                for subscription in adapter.subscription_messages():
                    await ws.send(json.dumps(subscription))
                heartbeat_task = asyncio.create_task(_send_heartbeats(adapter, ws))
                try:
                    while time.monotonic() < stop_at:
                        timeout = min(5.0, max(0.05, stop_at - time.monotonic()))
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        except asyncio.TimeoutError:
                            continue
                        recv_ts_ns, recv_mono_ns = time.time_ns(), time.monotonic_ns()
                        try:
                            bbo = adapter.parse(json.loads(raw), recv_ts_ns, recv_mono_ns)
                        except ReconnectRequired:
                            tracker.counts[adapter.name]["sequence_gap"] += 1
                            raise
                        except (InvalidMessage, KeyError, TypeError, ValueError):
                            tracker.counts[adapter.name]["invalid_messages"] += 1
                            continue
                        if bbo is None:
                            continue
                        tracker.observe(bbo)
                        latest[f"{bbo.exchange}:{bbo.instrument}"] = bbo
                        output.write(json.dumps(bbo.to_dict(), separators=(",", ":")) + "\n")
                        output.flush()
                finally:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            tracker.counts[adapter.name]["disconnects"] += 1
            tracker.counts[adapter.name][f"error:{type(exc).__name__}"] += 1
            attempts += 1
            await asyncio.sleep(min(8.0, (2 ** min(attempts, 3)) + random.random()))


async def run_probe(adapters: Iterable[ExchangeAdapter], duration: float, output_path: Path) -> dict:
    tracker = QualityTracker()
    latest: Dict[str, BBO] = {}
    stop_at = time.monotonic() + duration
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as output:
        await asyncio.gather(*(collect_adapter(a, output, tracker, stop_at, latest) for a in adapters))
    return tracker.summary()
