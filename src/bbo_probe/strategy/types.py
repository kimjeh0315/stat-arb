from dataclasses import dataclass
from enum import Enum


class Action(str, Enum):
    HOLD = "hold"
    LONG_LEFT = "long_left_short_right"
    SHORT_LEFT = "short_left_long_right"
    CLOSE = "close"


@dataclass(frozen=True)
class HypothesisInput:
    """Everything a hypothesis may inspect at one decision point."""

    asset: str
    left_venue: str
    right_venue: str
    timestamp_ns: int
    basis_bps: float
    zscore: float
    sample_count: int
    position: int  # 1=long left, -1=short left, 0=flat
    holding_seconds: float
    basis_mean_bps: float = 0.0
    basis_std_bps: float = 0.0
    deviation_bps: float = 0.0
