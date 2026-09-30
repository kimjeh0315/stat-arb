"""Combine many symbols into one public WebSocket connection per venue."""

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from ..models import BBO
from .base import ExchangeAdapter, JsonObject


@dataclass(frozen=True)
class BatchAdapter(ExchangeAdapter):
    children: Tuple[ExchangeAdapter, ...] = ()
    instrument: str = "*"

    def __post_init__(self) -> None:
        if not self.children:
            raise ValueError("BatchAdapter requires at least one child")
        names = {child.name for child in self.children}
        urls = {child.url for child in self.children}
        if len(names) != 1 or len(urls) != 1:
            raise ValueError("all batched adapters must use the same venue and URL")

    @property
    def name(self) -> str:
        return self.children[0].name

    @property
    def url(self) -> str:
        return self.children[0].url

    @property
    def heartbeat_interval(self) -> Optional[float]:
        return self.children[0].heartbeat_interval

    def heartbeat_message(self) -> Optional[JsonObject]:
        return self.children[0].heartbeat_message()

    def subscription_messages(self) -> Iterable[JsonObject]:
        messages = [message for child in self.children for message in child.subscription_messages()]
        if self.name == "bybit":
            return ({"op": "subscribe", "args": [arg for msg in messages for arg in msg["args"]]},)
        if self.name == "aster":
            return ({"method": "SUBSCRIBE", "params": [p for msg in messages for p in msg["params"]], "id": 1},)
        if self.name == "grvt":
            streams = {msg["params"]["stream"] for msg in messages}
            if len(streams) != 1:
                raise ValueError("all batched GRVT adapters must use the same stream")
            return ({
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {
                    "stream": streams.pop(),
                    "selectors": [s for msg in messages for s in msg["params"]["selectors"]],
                },
                "id": 1,
            },)
        return tuple(messages)

    def parse(self, message: JsonObject, recv_ts_ns: int, recv_mono_ns: int) -> Optional[BBO]:
        for child in self.children:
            quote = child.parse(message, recv_ts_ns, recv_mono_ns)
            if quote is not None:
                return quote
        return None
