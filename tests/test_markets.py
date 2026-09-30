import pytest

from bbo_probe.cli import build_parser
from bbo_probe.markets import TOP_MARKETS


def test_turnover_universe_contains_50_unique_ranked_assets() -> None:
    assert len(TOP_MARKETS) == 50
    assert [market.rank for market in TOP_MARKETS] == list(range(1, 51))
    assert len({market.asset for market in TOP_MARKETS}) == 50


def test_cli_accepts_top_50_but_not_51() -> None:
    assert build_parser().parse_args(["--top", "50"]).top == 50
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--top", "51"])
