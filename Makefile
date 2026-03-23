PYTHON_VERSION ?= 3.12

.PHONY: setup run score score-auto improve

setup:
	uv sync --python $(PYTHON_VERSION)

run:
	uv run python runner.py

score:
	uv run python scorer.py $(RESULTS)

score-auto:
	uv run python scorer.py $(RESULTS) --auto

improve:
	uv run python improve.py $(RESULTS)
