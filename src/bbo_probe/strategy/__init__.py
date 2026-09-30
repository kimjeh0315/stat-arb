"""Reusable event-driven research framework."""

from .pair_rules import PairTradingParameters, decide_pair
from .types import Action, HypothesisInput

__all__ = ["Action", "HypothesisInput", "PairTradingParameters", "decide_pair"]
