PYTHON ?= .venv/bin/python
VENUES ?= bybit hyperliquid aster grvt
SYMBOLS ?= BTC
DURATION ?= 60
DATA ?= data/bbo.jsonl
RESULT ?= data/result.json
HYPOTHESIS ?= hypotheses/btc_bybit_grvt_pair.py
WINDOW ?= 120
FRESHNESS_MS ?= 1000
FEE_BPS ?= 5
MIN_TRADES ?= 5

.PHONY: compile test check markets collect collect-top20 collect-top50 strategy strategy-grid

compile:
	PYTHONPYCACHEPREFIX=/tmp/quant-pycache $(PYTHON) -m compileall -q src hypotheses scripts

test:
	$(PYTHON) -m pytest -q

check: compile test

markets:
	$(PYTHON) -m bbo_probe.cli --list-markets

collect: compile
	$(PYTHON) -m bbo_probe.cli --venues $(VENUES) --symbols $(SYMBOLS) --duration $(DURATION) --output $(DATA)

collect-top20: compile
	$(PYTHON) -m bbo_probe.cli --venues $(VENUES) --top 20 --duration $(DURATION) --output $(DATA)

collect-top50: compile
	$(PYTHON) -m bbo_probe.cli --venues $(VENUES) --top 50 --duration $(DURATION) --output $(DATA)

strategy: compile
	$(PYTHON) -m bbo_probe.strategy.cli --data $(DATA) --venues $(VENUES) --symbols $(SYMBOLS) --hypothesis $(HYPOTHESIS) --window $(WINDOW) --freshness-ms $(FRESHNESS_MS) --fee-bps $(FEE_BPS) --output $(RESULT)

strategy-grid: compile
	$(PYTHON) scripts/simulate_pair_grid.py --data $(DATA) --venues $(VENUES) --symbols $(SYMBOLS) --window $(WINDOW) --freshness-ms $(FRESHNESS_MS) --fee-bps $(FEE_BPS) --min-trades $(MIN_TRADES) --output $(RESULT)
