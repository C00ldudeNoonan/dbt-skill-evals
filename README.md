# dbt-skill-evals

A lightweight eval framework that measures how [dbt agent skills](https://github.com/dbt-labs/dbt-agent-skills) change Claude Code's behavior on canonical dbt workflows.

## Why

Agent skills are instructions that shape how AI coding agents approach tasks. But how do you know they're working? This framework runs the same dbt task with and without skills, captures structured metrics, and compares the results — giving you data on whether skills actually improve agent behavior.

Think of it as **testing for skills**: just as dbt brought testing to data transformations, every skill should have an eval.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Scenario   │     │   runner.py  │     │  scorer.py   │
│              │     │              │     │              │
│ prompt.txt   │────▶│ claude -p    │────▶│ validators   │
│ context/     │     │ (headless)   │     │ LLM grading  │
│ skill-sets   │     │              │     │ baselines    │
└──────────────┘     └──────────────┘     └──────────────┘
                            │                    │
                            ▼                    ▼
                     results/<ts>/         baselines/
                     ├── output.md         └── <scenario>/
                     ├── metadata.yaml         └── <set>.json
                     └── changes/
```

Each scenario runs Claude Code headlessly (`claude -p`) against a self-contained jaffle-shop dbt project, once without skills (baseline) and once with dbt-agent-skills installed. The scorer runs deterministic checks (naming conventions, `dbt compile` exit codes, test coverage) and optionally uses Claude as an LLM judge.

## Scenarios

| Scenario | What it tests | Key eval criteria |
|----------|--------------|-------------------|
| **create-staging-model** | Creating a new staging model from a source | Naming conventions, `source()` macro, YAML tests, `dbt compile` |
| **add-tests-to-model** | Adding tests to an untested model | Data preview before writing tests, no hallucinated values, `dbt test` |
| **add-relationships-tests** | Adding targeted foreign-key relationship tests | Correct parent refs, preserved existing tests, targeted `dbt test` |
| **refactor-hardcoded-source** | Replacing a hardcoded raw table reference with `source()` | Correct source macro, preserved transformations, targeted `dbt compile` |
| **add-source-freshness** | Adding freshness expectations to a declared source | Correct freshness thresholds, preserved source config, targeted `dbt source freshness` |
| **add-unit-test-to-model** | Adding a unit test for model transformation logic | Correct test inputs and expected rows, preserved existing tests, targeted `dbt test` |
| **debug-failing-build** | Diagnosing and fixing a build error | Root cause identification, targeted fix, no collateral changes |

## Quick Start

```bash
# Create the repo-local .venv with Python 3.12 and install dependencies
make setup

# Run all scenarios (7 scenarios x 2 skill-sets = 14 runs)
make run

# Run a single scenario
uv run python runner.py --scenario create-staging-model

# Score results
make score RESULTS=results/<timestamp>

# Score with dbt validation
uv run python scorer.py results/<timestamp> --validate-dbt

# Score with LLM grading
make score-auto RESULTS=results/<timestamp>

# Save results as baseline for future comparison
uv run python scorer.py results/<timestamp> --save-baseline

# Generate skill-improvement feedback from a scored run
make improve RESULTS=results/<timestamp>
```

## Local Setup

- This repo expects a repo-local `.venv/` and prefers that environment over any global `dbt` install.
- `make setup` runs `uv sync --python 3.12`, which creates `.venv/` in the repo and installs `dbt-duckdb` plus the Python dependencies used by the harness.
- `claude` still needs to be installed and authenticated separately.
- If you do not want to use `make`, the equivalent bootstrap command is `uv sync --python 3.12`.

## Results

> Results will be added after running evals.

## Improvement Loop

- `improve.py` turns a scored run into skill feedback artifacts under `results/<timestamp>/improvement/`
- It analyzes deterministic failures for `with-skills`, compares them to `no-skills`, extracts evidence from `raw.jsonl` and `output.md`, and writes:
- `report.md` — human-readable findings and recommendations
- `feedback.json` — machine-readable recommendations
- `skill_update_prompt.txt` — a prompt you can use to update the external dbt skill
- The loop does not edit the external skill automatically; it closes the gap between eval results and the next skill revision.

## Design Decisions

- **Lightweight over polished**: Two Python scripts (~200 lines each) over a CLI framework. Achieves the same goal with less complexity.
- **Deterministic first**: Regex checks and `dbt compile` exit codes catch most issues. LLM grading is optional and additive.
- **Version-controlled baselines**: JSON snapshots committed to git enable regression detection via `git diff` (inspired by Dagster's eval approach).
- **Self-contained contexts**: Each scenario ships a complete dbt project (jaffle-shop + DuckDB). No external warehouse needed.
- **A/B by default**: Every scenario compares no-skills vs. with-skills, producing a comparison table that shows exactly what skills change.

## Extending

To add a new scenario:

1. Create `scenarios/<name>/` with `context/`, `prompt.txt`, `scenario.md`, `skill-sets.yaml`
2. Add a validator function in `scorer.py`
3. Run `uv run python runner.py --scenario <name>`
