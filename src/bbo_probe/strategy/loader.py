import importlib.util
from pathlib import Path
from typing import Callable

from .types import Action, HypothesisInput

DecisionFunction = Callable[[HypothesisInput], Action]


def load_hypothesis(path: Path) -> DecisionFunction:
    spec = importlib.util.spec_from_file_location("bbo_user_hypothesis", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot load hypothesis: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    decide = getattr(module, "decide", None)
    if not callable(decide):
        raise ValueError(f"{path} must define decide(frame)")
    return decide
