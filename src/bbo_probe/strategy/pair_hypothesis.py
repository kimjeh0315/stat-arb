"""Declarative, traceable definitions for cross-venue pair hypotheses."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Tuple

from .pair_rules import PairTradingParameters, decide_pair
from .types import Action, HypothesisInput


@dataclass(frozen=True)
class PairParameterSweep:
    windows: Tuple[int, ...] = ()
    warmup_samples: Tuple[int, ...] = ()
    entry_z: Tuple[float, ...] = ()
    min_deviation_bps: Tuple[float, ...] = ()
    exit_z: Tuple[float, ...] = ()
    stop_z: Tuple[float, ...] = ()
    max_hold_seconds: Tuple[float, ...] = ()


@dataclass(frozen=True)
class PairHypothesis:
    hypothesis_id: str
    version: int
    title: str
    thesis: str
    parameters: PairTradingParameters
    window: int
    assets: Tuple[str, ...] = ()
    venue_pairs: Tuple[Tuple[str, str], ...] = ()
    sweep: PairParameterSweep = field(default_factory=PairParameterSweep)

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9]+(?:_[a-z0-9]+)*", self.hypothesis_id):
            raise ValueError("hypothesis_id must be lowercase snake_case")
        if self.version < 1 or not self.title.strip() or not self.thesis.strip():
            raise ValueError("hypothesis version, title and thesis are required")
        if self.window < self.parameters.warmup_samples:
            raise ValueError("hypothesis window must cover its warmup samples")

    def allows(self, asset: str, left_venue: str, right_venue: str) -> bool:
        asset_allowed = not self.assets or asset in self.assets
        pair = frozenset((left_venue, right_venue))
        venue_allowed = not self.venue_pairs or any(
            pair == frozenset(candidate) for candidate in self.venue_pairs
        )
        return asset_allowed and venue_allowed

    def applies_to(self, frame: HypothesisInput) -> bool:
        return self.allows(frame.asset, frame.left_venue, frame.right_venue)

    def decide(self, frame: HypothesisInput) -> Action:
        if not self.applies_to(frame):
            return Action.HOLD
        return decide_pair(frame, self.parameters)

    def with_parameters(self, parameters: PairTradingParameters) -> "PairHypothesis":
        return replace(self, parameters=parameters)


@dataclass(frozen=True)
class LoadedPairHypothesis:
    specification: PairHypothesis
    source_path: Path
    source_sha256: str

    def with_parameters(self, parameters: PairTradingParameters) -> "LoadedPairHypothesis":
        return replace(self, specification=self.specification.with_parameters(parameters))

    def definition_manifest(self) -> Dict[str, Any]:
        spec = self.specification
        return {
            "id": spec.hypothesis_id,
            "version": spec.version,
            "title": spec.title,
            "thesis": spec.thesis,
            "source": str(self.source_path),
            "source_sha256": self.source_sha256,
            "scope": {
                "assets": list(spec.assets) if spec.assets else "all_requested",
                "venue_pairs": [list(pair) for pair in spec.venue_pairs]
                if spec.venue_pairs else "all_requested",
            },
            "default_window": spec.window,
            "sweep_defaults": asdict(spec.sweep),
        }

    def variant_manifest(self, *, window: int) -> Dict[str, Any]:
        parameters = asdict(self.specification.parameters)
        variant_payload = json.dumps(
            {"source_sha256": self.source_sha256, "parameters": parameters, "window": window},
            sort_keys=True, separators=(",", ":"),
        ).encode()
        return {
            "variant_id": hashlib.sha256(variant_payload).hexdigest()[:16],
            "window": window,
            "parameters": parameters,
        }

    def manifest(self, *, window: int) -> Dict[str, Any]:
        return {**self.definition_manifest(), "variant": self.variant_manifest(window=window)}
