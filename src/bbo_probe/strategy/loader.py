import importlib.util
import hashlib
from pathlib import Path
from typing import Callable

from .pair_hypothesis import LoadedPairHypothesis, PairHypothesis
from .types import Action, HypothesisInput

DecisionFunction = Callable[[HypothesisInput], Action]


def _load_module(path: Path):
    resolved = path.resolve()
    spec = importlib.util.spec_from_file_location(
        f"bbo_user_hypothesis_{hashlib.sha256(str(resolved).encode()).hexdigest()[:12]}",
        resolved,
    )
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot load hypothesis: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, resolved


def load_pair_hypothesis(path: Path) -> LoadedPairHypothesis:
    module, resolved = _load_module(path)
    hypothesis = getattr(module, "HYPOTHESIS", None)
    if not isinstance(hypothesis, PairHypothesis):
        raise ValueError(f"{path} must export HYPOTHESIS as PairHypothesis")
    return LoadedPairHypothesis(
        specification=hypothesis,
        source_path=path,
        source_sha256=hashlib.sha256(resolved.read_bytes()).hexdigest(),
    )


def load_hypothesis(path: Path) -> DecisionFunction:
    module, _ = _load_module(path)
    structured = getattr(module, "HYPOTHESIS", None)
    if isinstance(structured, PairHypothesis):
        return structured.decide
    decide = getattr(module, "decide", None)
    if not callable(decide):
        raise ValueError(f"{path} must define decide(frame)")
    return decide
