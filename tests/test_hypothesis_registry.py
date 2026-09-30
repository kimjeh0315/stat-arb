import importlib.util
from dataclasses import replace
from pathlib import Path

from bbo_probe.strategy.loader import load_pair_hypothesis


def load_script(path: str):
    spec = importlib.util.spec_from_file_location("test_script_module", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_hypothesis_file_exports_a_traceable_specification() -> None:
    for path in sorted(Path("hypotheses").glob("*.py")):
        loaded = load_pair_hypothesis(path)
        manifest = loaded.manifest(window=loaded.specification.window)
        assert manifest["id"] == path.stem
        assert len(manifest["source_sha256"]) == 64
        assert len(manifest["variant"]["variant_id"]) == 16


def test_parameter_override_changes_variant_but_not_hypothesis_source() -> None:
    loaded = load_pair_hypothesis(Path("hypotheses/cross_venue_pair_grid.py"))
    original = loaded.manifest(window=loaded.specification.window)
    changed_parameters = replace(loaded.specification.parameters, entry_z=2.0)
    changed = loaded.with_parameters(changed_parameters).manifest(
        window=loaded.specification.window
    )
    assert changed["source_sha256"] == original["source_sha256"]
    assert changed["variant"]["variant_id"] != original["variant"]["variant_id"]


def test_sweep_defaults_come_from_the_hypothesis_file() -> None:
    module = load_script("scripts/sweep_pair_parameters.py")
    args = module.build_parser().parse_args([
        "--data", "unused.jsonl", "--venues", "bybit", "grvt", "--top", "50",
    ])
    loaded = load_pair_hypothesis(Path("hypotheses/cross_venue_pair_grid.py"))
    grid = module.parameter_grid(args, loaded.specification)
    assert len(grid) == 18
    assert {values[2] for values in grid} == {1.75, 2.0, 2.5}


def test_grid_cli_accepts_top_50_without_running_a_simulation() -> None:
    module = load_script("scripts/simulate_pair_grid.py")
    args = module.build_parser().parse_args([
        "--data", "unused.jsonl", "--venues", "bybit", "grvt", "--top", "50",
    ])
    assert args.top == 50
