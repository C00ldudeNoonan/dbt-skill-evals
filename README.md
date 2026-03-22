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
| **debug-failing-build** | Diagnosing and fixing a build error | Root cause identification, targeted fix, no collateral changes |

## Quick Start

```bash
# Install dependencies
uv sync

# Run all scenarios (3 scenarios x 2 skill-sets = 6 runs)
python runner.py

# Run a single scenario
python runner.py --scenario create-staging-model

# Score results
python scorer.py results/<timestamp>

# Score with dbt validation
python scorer.py results/<timestamp> --validate-dbt

# Score with LLM grading
python scorer.py results/<timestamp> --auto

# Save results as baseline for future comparison
python scorer.py results/<timestamp> --save-baseline
```

## Results

> Results will be added after running evals.

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
3. Run `python runner.py --scenario <name>`
