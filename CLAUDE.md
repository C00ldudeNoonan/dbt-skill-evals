# dbt-skill-evals

Eval framework that measures how dbt agent skills change Claude Code's behavior on canonical dbt workflows.

## How to run

```bash
# Create the repo-local .venv
make setup

# Run all scenarios
make run

# Run a single scenario
uv run python runner.py --scenario create-staging-model

# Run a single skill-set across all scenarios
uv run python runner.py --skill-set no-skills

# Score results
make score RESULTS=results/<timestamp>

# Score with LLM grading
make score-auto RESULTS=results/<timestamp>

# Save baseline
uv run python scorer.py results/<timestamp> --save-baseline

# Generate improvement feedback for the skill
make improve RESULTS=results/<timestamp>
```

## Project structure

- `scenarios/` — Each subdirectory is a self-contained eval scenario
- `context/` — A complete dbt project (jaffle-shop variant) copied to a temp dir for each run
- `prompt.txt` — The exact prompt sent to Claude
- `skill-sets.yaml` — Defines skill-set configurations to A/B test
- `scenario.md` — Description and grading criteria
- `runner.py` — Invokes `claude -p` per scenario x skill-set, captures metrics
- `scorer.py` — Deterministic validators + optional LLM grading + baseline comparison
- `improve.py` — Converts scored runs into proposed skill updates and feedback artifacts
- `baselines/` — Version-controlled JSON snapshots for regression detection
- `results/` — Run output (gitignored)

## Conventions

- Each scenario's `context/` is a self-contained dbt project using DuckDB
- The repo expects a local `.venv/`; run `make setup` to create it with Python 3.12
- The runner executes `dbt deps && dbt seed` before each eval run
- All dbt commands use the `profiles.yml` inside the context directory
