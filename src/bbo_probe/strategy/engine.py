"""Small, dependency-free, event-driven BBO backtester."""

import json
import math
import statistics
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from ..markets import canonical_asset
from .loader import DecisionFunction
from .types import Action, HypothesisInput


@dataclass(frozen=True)
class Quote:
    venue: str
    instrument: str
    bid: float
    ask: float
    recv_ts_ns: int

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass
class Position:
    direction: int
    entry_ts_ns: int
    long_venue: str
    short_venue: str
    long_entry: float
    short_entry: float


@dataclass(frozen=True)
class Trade:
    asset: str
    left_venue: str
    right_venue: str
    direction: str
    entry_ts_ns: int
    exit_ts_ns: int
    holding_seconds: float
    gross_bps: float
    cost_bps: float
    net_bps: float
    exit_reason: str


def _quote(record: Dict[str, Any]) -> Optional[Quote]:
    if not record.get("valid", True):
        return None
    return Quote(
        venue=str(record["exchange"]),
        instrument=str(record["instrument"]),
        bid=float(record["bid_price"]),
        ask=float(record["ask_price"]),
        recv_ts_ns=int(record["recv_ts_ns"]),
    )


def _rolling_statistics(values: Iterable[float]) -> Tuple[float, float, float]:
    sample = list(values)
    mean = statistics.mean(sample)
    standard_deviation = statistics.pstdev(sample) if len(sample) >= 2 else 0.0
    deviation = sample[-1] - mean
    zscore = 0.0 if standard_deviation == 0 else deviation / standard_deviation
    return mean, standard_deviation, zscore


def _max_drawdown(values: Iterable[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def _close_trade(
    key: Tuple[str, str, str], position: Position, left: Quote, right: Quote,
    now_ns: int, fee_bps: float, reason: str,
) -> Trade:
    quotes = {left.venue: left, right.venue: right}
    long_exit = quotes[position.long_venue].bid
    short_exit = quotes[position.short_venue].ask
    long_return = long_exit / position.long_entry - 1
    short_return = position.short_entry / short_exit - 1
    gross_bps = (long_return + short_return) * 5_000
    # Four fills charged against two equal-notional legs.
    cost_bps = fee_bps * 2
    return Trade(
        asset=key[0], left_venue=key[1], right_venue=key[2],
        direction="long_left" if position.direction == 1 else "short_left",
        entry_ts_ns=position.entry_ts_ns, exit_ts_ns=now_ns,
        holding_seconds=(now_ns - position.entry_ts_ns) / 1_000_000_000,
        gross_bps=round(gross_bps, 6), cost_bps=round(cost_bps, 6),
        net_bps=round(gross_bps - cost_bps, 6), exit_reason=reason,
    )


def run_backtest(
    data_path: Path,
    venues: Sequence[str],
    assets: Sequence[str],
    decide: DecisionFunction,
    *,
    window: int = 120,
    freshness_ms: float = 1_000,
    fee_bps: float = 5.0,
) -> Dict[str, Any]:
    if len(venues) < 2:
        raise ValueError("backtest requires at least two venues")
    if window < 2 or freshness_ms <= 0 or fee_bps < 0:
        raise ValueError("invalid backtest configuration")

    wanted_venues, wanted_assets = set(venues), {asset.upper() for asset in assets}
    venue_pairs = list(combinations(venues, 2))
    latest: Dict[Tuple[str, str], Quote] = {}
    histories: Dict[Tuple[str, str, str], Deque[float]] = defaultdict(
        lambda: deque(maxlen=window)
    )
    positions: Dict[Tuple[str, str, str], Position] = {}
    last_frames: Dict[Tuple[str, str, str], Tuple[Quote, Quote]] = {}
    frame_counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
    trades: List[Trade] = []
    stale_events = 0
    processed_events = 0

    with data_path.open(encoding="utf-8") as source:
        for line in source:
            record = json.loads(line)
            quote = _quote(record)
            if quote is None or quote.venue not in wanted_venues:
                continue
            asset = canonical_asset(quote.venue, quote.instrument)
            if asset is None or asset not in wanted_assets:
                continue
            latest[(asset, quote.venue)] = quote
            processed_events += 1

            for left_venue, right_venue in venue_pairs:
                if quote.venue not in (left_venue, right_venue):
                    continue
                left = latest.get((asset, left_venue))
                right = latest.get((asset, right_venue))
                if left is None or right is None:
                    continue
                now_ns = max(left.recv_ts_ns, right.recv_ts_ns)
                if now_ns - min(left.recv_ts_ns, right.recv_ts_ns) > freshness_ms * 1_000_000:
                    stale_events += 1
                    continue

                key = (asset, left_venue, right_venue)
                frame_counts[key] += 1
                basis = math.log(left.mid / right.mid) * 10_000
                history = histories[key]
                history.append(basis)
                basis_mean, basis_std, zscore = _rolling_statistics(history)
                position = positions.get(key)
                frame = HypothesisInput(
                    asset=asset, left_venue=left_venue, right_venue=right_venue,
                    timestamp_ns=now_ns, basis_bps=basis, zscore=zscore,
                    sample_count=len(history), position=0 if position is None else position.direction,
                    holding_seconds=(
                        0.0 if position is None
                        else (now_ns - position.entry_ts_ns) / 1_000_000_000
                    ),
                    basis_mean_bps=basis_mean,
                    basis_std_bps=basis_std,
                    deviation_bps=basis - basis_mean,
                )
                action = decide(frame)
                if not isinstance(action, Action):
                    action = Action(action)

                if position is None and action in (Action.LONG_LEFT, Action.SHORT_LEFT):
                    if action == Action.LONG_LEFT:
                        position = Position(1, now_ns, left_venue, right_venue, left.ask, right.bid)
                    else:
                        position = Position(-1, now_ns, right_venue, left_venue, right.ask, left.bid)
                    positions[key] = position
                elif position is not None and action == Action.CLOSE:
                    trades.append(_close_trade(key, position, left, right, now_ns, fee_bps, "signal"))
                    del positions[key]
                last_frames[key] = (left, right)

    for key, position in positions.items():
        left, right = last_frames[key]
        now_ns = max(left.recv_ts_ns, right.recv_ts_ns)
        trades.append(_close_trade(key, position, left, right, now_ns, fee_bps, "end_of_data"))

    net_values = [trade.net_bps for trade in trades]
    grouped: Dict[Tuple[str, str, str], List[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[(trade.asset, trade.left_venue, trade.right_venue)].append(trade)
    pair_results: Dict[str, Dict[str, Any]] = {}
    for key in sorted(frame_counts):
        pair_trades = grouped[key]
        pair_net = [trade.net_bps for trade in pair_trades]
        pair_gross = [trade.gross_bps for trade in pair_trades]
        pair_name = f"{key[0]}:{key[1]}-{key[2]}"
        pair_results[pair_name] = {
            "samples": frame_counts[key],
            "trades": len(pair_trades),
            "win_rate": (
                0.0 if not pair_trades
                else round(sum(value > 0 for value in pair_net) / len(pair_trades), 6)
            ),
            "gross_bps": round(sum(pair_gross), 6),
            "net_bps": round(sum(pair_net), 6),
            "average_net_bps": (
                0.0 if not pair_trades else round(statistics.mean(pair_net), 6)
            ),
            "max_drawdown_bps": round(_max_drawdown(pair_net), 6),
        }
    return {
        "configuration": {
            "data": str(data_path), "venues": list(venues), "assets": list(assets),
            "window": window, "freshness_ms": freshness_ms, "fee_bps_per_fill": fee_bps,
        },
        "events": {"processed": processed_events, "stale_pair_events": stale_events},
        "summary": {
            "trades": len(trades),
            "win_rate": 0.0 if not trades else sum(value > 0 for value in net_values) / len(trades),
            "gross_bps": round(sum(trade.gross_bps for trade in trades), 6),
            "net_bps": round(sum(net_values), 6),
            "average_net_bps": 0.0 if not trades else round(statistics.mean(net_values), 6),
        },
        "pairs": pair_results,
        "trades": [asdict(trade) for trade in trades],
    }
