"""Edit only this function to test a different hypothesis."""

from bbo_probe.strategy import Action, HypothesisInput


def decide(x: HypothesisInput) -> Action:
    if x.position == 0 and x.sample_count >= 60:
        if x.zscore >= 2.5:
            return Action.SHORT_LEFT
        if x.zscore <= -2.5:
            return Action.LONG_LEFT
    if x.position != 0 and (abs(x.zscore) <= 0.5 or x.holding_seconds >= 300):
        return Action.CLOSE
    return Action.HOLD
