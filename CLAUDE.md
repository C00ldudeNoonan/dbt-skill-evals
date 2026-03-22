# dbt-skill-evals

Eval framework that measures how dbt agent skills change Claude Code's behavior on canonical dbt workflows.

## How to run

```bash
# Install dependencies
uv sync

# Run all scenarios
python runner.py

# Run a single scenario
python runner.py --scenario create-staging-model

# Run a single skill-set across all scenarios
python runner.py --skill-set no-skills

# Score results
python scorer.py results/<timestamp>

# Score with LLM grading
python scorer.py results/<timestamp> --auto

# Save baseline
python scorer.py results/<timestamp> --save-baseline
```

## Project structure

- `scenarios/` — Each subdirectory is a self-contained eval scenario
  - `context/` — A complete dbt project (jaffle-shop variant) copied to a temp dir for each run
  - `prompt.txt` — The exact prompt sent to Claude
  - `skill-sets.yaml` — Defines skill-set configurations to A/B test
  - `scenario.md` — Description and grading criteria
- `runner.py` — Invokes `claude -p` per scenario x skill-set, captures metrics
- `scorer.py` — Deterministic validators + optional LLM grading + baseline comparison
- `baselines/` — Version-controlled JSON snapshots for regression detection
- `results/` — Run output (gitignored)

## Conventions

- Each scenario's `context/` is a self-contained dbt project using DuckDB
- The runner executes `dbt deps && dbt seed` before each eval run
- All dbt commands use the `profiles.yml` inside the context directory
