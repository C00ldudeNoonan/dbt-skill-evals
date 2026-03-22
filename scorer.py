"""Scorer: deterministic validators + optional LLM grading + baseline comparison."""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path


SCENARIOS_DIR = Path(__file__).parent / "scenarios"
BASELINES_DIR = Path(__file__).parent / "baselines"

# --- Deterministic validators per scenario ---


def validate_create_staging_model(changes: dict[str, str], output_text: str) -> dict:
    checks = {}

    # Check stg_supplies.sql exists
    sql_matches = [k for k in changes if "stg_supplies" in k and k.endswith(".sql")]
    checks["sql_file_created"] = len(sql_matches) > 0

    if sql_matches:
        content = changes[sql_matches[0]]
        checks["uses_source_macro"] = bool(
            re.search(r"source\s*\(\s*['\"]ecom['\"]\s*,\s*['\"]raw_supplies['\"]\s*\)", content)
        )
        checks["uses_cents_to_dollars"] = "cents_to_dollars" in content
        checks["uses_surrogate_key"] = "generate_surrogate_key" in content

    # Check YAML file exists with tests
    yml_matches = [k for k in changes if "stg_supplies" in k and k.endswith(".yml")]
    checks["yaml_file_created"] = len(yml_matches) > 0

    if yml_matches:
        yml_content = changes[yml_matches[0]]
        checks["has_not_null_test"] = "not_null" in yml_content
        checks["has_unique_test"] = "unique" in yml_content

    return checks


def validate_add_tests_to_model(changes: dict[str, str], output_text: str) -> dict:
    checks = {}

    # Check stg_orders.yml was modified
    yml_matches = [k for k in changes if "stg_orders" in k and k.endswith(".yml")]
    checks["yml_modified"] = len(yml_matches) > 0

    if yml_matches:
        yml_content = changes[yml_matches[0]]
        checks["has_not_null_test"] = "not_null" in yml_content
        checks["has_unique_test"] = "unique" in yml_content
        checks["has_expression_test"] = "expression_is_true" in yml_content

    # Check if agent previewed data before writing tests
    checks["used_dbt_show"] = "dbt show" in output_text

    # Check for hallucinated accepted_values (rough heuristic)
    if yml_matches:
        yml_content = changes[yml_matches[0]]
        checks["no_accepted_values_hallucination"] = "accepted_values" not in yml_content

    return checks


def validate_debug_failing_build(changes: dict[str, str], output_text: str) -> dict:
    checks = {}

    # Check stg_orders.sql was modified
    sql_matches = [k for k in changes if "stg_orders" in k and k.endswith(".sql")]
    checks["sql_file_modified"] = len(sql_matches) > 0

    if sql_matches:
        content = changes[sql_matches[0]]
        # The fix should change "customers" back to "customer"
        checks["bug_fixed"] = bool(re.search(r"\bcustomer\b\s+as\s+customer_id", content))
        checks["typo_removed"] = "customers as customer_id" not in content

    # Check no other SQL files were modified (targeted fix)
    other_sql = [k for k in changes if k.endswith(".sql") and "stg_orders" not in k]
    checks["no_collateral_changes"] = len(other_sql) == 0

    return checks


VALIDATORS = {
    "create-staging-model": validate_create_staging_model,
    "add-tests-to-model": validate_add_tests_to_model,
    "debug-failing-build": validate_debug_failing_build,
}


def validate_with_dbt(scenario_name: str, changes: dict[str, str], context_dir: Path) -> dict:
    """Run dbt commands to validate the changes."""
    checks = {}

    # Create temp dir with original context + changes overlaid
    temp_dir = Path(tempfile.mkdtemp(prefix="dbt-validate-"))
    try:
        shutil.copytree(context_dir, temp_dir, dirs_exist_ok=True)

        # Apply changes
        for rel_path, content in changes.items():
            out_path = temp_dir / rel_path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(content)

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(temp_dir)

        # Install deps and seed
        subprocess.run(
            ["dbt", "deps"], cwd=temp_dir, capture_output=True, env=env, timeout=120,
        )
        subprocess.run(
            ["dbt", "seed"], cwd=temp_dir, capture_output=True, env=env, timeout=120,
        )

        # Run scenario-specific dbt command
        if scenario_name == "create-staging-model":
            r = subprocess.run(
                ["dbt", "compile", "--select", "stg_supplies"],
                cwd=temp_dir, capture_output=True, text=True, env=env, timeout=120,
            )
            checks["dbt_compile_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_compile_error"] = r.stderr[:300]

        elif scenario_name == "add-tests-to-model":
            r = subprocess.run(
                ["dbt", "test", "--select", "stg_orders"],
                cwd=temp_dir, capture_output=True, text=True, env=env, timeout=120,
            )
            checks["dbt_test_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_test_error"] = r.stderr[:300]

        elif scenario_name == "debug-failing-build":
            r = subprocess.run(
                ["dbt", "build"],
                cwd=temp_dir, capture_output=True, text=True, env=env, timeout=300,
            )
            checks["dbt_build_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_build_error"] = r.stderr[:300]

    except FileNotFoundError:
        checks["dbt_available"] = False
    except Exception as e:
        checks["dbt_validation_error"] = str(e)[:200]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return checks


# --- LLM grading ---


def llm_grade(scenario_md: str, output_text: str, metadata: dict) -> dict:
    """Use Claude as an LLM judge to grade the run."""
    grading_prompt = f"""You are grading an AI agent's performance on a dbt task.

## Scenario
{scenario_md}

## Agent Output
{output_text[:5000]}

## Metadata
- Duration: {metadata.get('duration_ms', 0)}ms
- Cost: ${metadata.get('total_cost_usd', 0):.4f}
- Tools used: {metadata.get('tools_used', [])}

## Instructions
Grade the agent on three dimensions. Respond with ONLY valid YAML:

```yaml
task_completion:
  score: <1-5>
  notes: "<brief explanation>"
tool_usage:
  rating: "<appropriate|partial|inappropriate>"
  notes: "<brief explanation>"
solution_quality:
  score: <1-5>
  notes: "<brief explanation>"
overall_success: <true|false>
```"""

    try:
        r = subprocess.run(
            ["claude", "--print", "-p", grading_prompt, "--max-turns", "1"],
            capture_output=True, text=True, timeout=120,
        )
        # Extract YAML from response
        output = r.stdout.strip()
        yaml_match = re.search(r"```yaml\s*\n(.*?)```", output, re.DOTALL)
        if yaml_match:
            return yaml.safe_load(yaml_match.group(1))
        # Try parsing entire output as YAML
        return yaml.safe_load(output)
    except Exception as e:
        return {"error": str(e)[:200]}


# --- Baseline comparison ---


def load_baseline(scenario: str, skill_set: str) -> dict | None:
    baseline_file = BASELINES_DIR / scenario / f"{skill_set}.json"
    if baseline_file.exists():
        return json.loads(baseline_file.read_text())
    return None


def save_baseline(scores: dict, scenario: str, skill_set: str):
    baseline_dir = BASELINES_DIR / scenario
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_file = baseline_dir / f"{skill_set}.json"
    baseline_file.write_text(json.dumps(scores, indent=2))
    print(f"  Baseline saved: {baseline_file}")


def compare_to_baseline(current: dict, baseline: dict) -> dict:
    """Compare current scores to baseline, return deltas."""
    deltas = {}
    for key in ["total_cost_usd", "input_tokens", "output_tokens", "duration_ms"]:
        if key in current and key in baseline:
            deltas[key] = {
                "current": current[key],
                "baseline": baseline[key],
                "delta": current[key] - baseline[key],
            }

    # Compare check results
    curr_checks = current.get("checks", {})
    base_checks = baseline.get("checks", {})
    check_deltas = {}
    for key in set(list(curr_checks.keys()) + list(base_checks.keys())):
        c = curr_checks.get(key)
        b = base_checks.get(key)
        if c != b:
            check_deltas[key] = {"current": c, "baseline": b}
    if check_deltas:
        deltas["checks_changed"] = check_deltas

    return deltas


# --- Main ---


def main():
    parser = argparse.ArgumentParser(description="Score dbt skill eval results")
    parser.add_argument("run_dir", help="Path to run results directory")
    parser.add_argument("--auto", action="store_true", help="Use LLM grading")
    parser.add_argument("--save-baseline", action="store_true", help="Save as baseline")
    parser.add_argument("--validate-dbt", action="store_true", help="Run dbt validation")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        print(f"Run directory not found: {run_dir}")
        sys.exit(1)

    all_scores = {}

    for scenario_dir in sorted(run_dir.iterdir()):
        if not scenario_dir.is_dir():
            continue
        scenario_name = scenario_dir.name

        for skill_set_dir in sorted(scenario_dir.iterdir()):
            if not skill_set_dir.is_dir():
                continue
            skill_set_name = skill_set_dir.name

            print(f"\n[{scenario_name}] [{skill_set_name}]")

            # Load run data
            metadata = {}
            metadata_file = skill_set_dir / "metadata.yaml"
            if metadata_file.exists():
                metadata = yaml.safe_load(metadata_file.read_text())

            output_text = ""
            output_file = skill_set_dir / "output.md"
            if output_file.exists():
                output_text = output_file.read_text()

            # Load changes
            changes = {}
            changes_dir = skill_set_dir / "changes"
            if changes_dir.exists():
                for root, _, files in os.walk(changes_dir):
                    for f in files:
                        file_path = Path(root) / f
                        rel_path = file_path.relative_to(changes_dir)
                        changes[str(rel_path)] = file_path.read_text(errors="replace")

            # Run deterministic validators
            validator = VALIDATORS.get(scenario_name)
            checks = {}
            if validator:
                checks = validator(changes, output_text)
                print(f"  Checks:")
                for k, v in checks.items():
                    status = "PASS" if v is True else ("FAIL" if v is False else v)
                    print(f"    {k}: {status}")

            # Run dbt validation
            dbt_checks = {}
            if args.validate_dbt and changes:
                context_dir = SCENARIOS_DIR / scenario_name / "context"
                dbt_checks = validate_with_dbt(scenario_name, changes, context_dir)
                print(f"  dbt checks:")
                for k, v in dbt_checks.items():
                    status = "PASS" if v is True else ("FAIL" if v is False else v)
                    print(f"    {k}: {status}")

            # LLM grading
            llm_scores = {}
            if args.auto:
                scenario_md_file = SCENARIOS_DIR / scenario_name / "scenario.md"
                scenario_md = scenario_md_file.read_text() if scenario_md_file.exists() else ""
                print(f"  Running LLM grading...")
                llm_scores = llm_grade(scenario_md, output_text, metadata)
                print(f"  LLM grade: {llm_scores}")

            # Compile scores
            scores = {
                "scenario": scenario_name,
                "skill_set": skill_set_name,
                "checks": {**checks, **dbt_checks},
                "total_cost_usd": metadata.get("total_cost_usd", 0),
                "input_tokens": metadata.get("input_tokens", 0),
                "output_tokens": metadata.get("output_tokens", 0),
                "duration_ms": metadata.get("duration_ms", 0),
                "tools_used": metadata.get("tools_used", []),
            }
            if llm_scores:
                scores["llm_grade"] = llm_scores

            # Baseline comparison
            baseline = load_baseline(scenario_name, skill_set_name)
            if baseline:
                deltas = compare_to_baseline(scores, baseline)
                if deltas:
                    print(f"  Baseline comparison:")
                    for k, v in deltas.items():
                        if isinstance(v, dict) and "delta" in v:
                            sign = "+" if v["delta"] > 0 else ""
                            print(f"    {k}: {sign}{v['delta']:.4f} ({v['baseline']:.4f} -> {v['current']:.4f})")
                        else:
                            print(f"    {k}: {v}")
                scores["baseline_deltas"] = deltas

            # Save baseline if requested
            if args.save_baseline:
                save_baseline(scores, scenario_name, skill_set_name)

            key = f"{scenario_name}/{skill_set_name}"
            all_scores[key] = scores

    # Save combined scores
    scores_file = run_dir / "scores.yaml"
    with open(scores_file, "w") as f:
        yaml.dump(all_scores, f, default_flow_style=False)
    print(f"\nScores saved to: {scores_file}")

    # Generate comparison summary
    generate_comparison(all_scores, run_dir)


def generate_comparison(all_scores: dict, run_dir: Path):
    """Generate a markdown comparison of no-skills vs with-skills."""
    lines = ["# Eval Results Comparison\n"]

    # Group by scenario
    scenarios = {}
    for key, scores in all_scores.items():
        scenario = scores["scenario"]
        if scenario not in scenarios:
            scenarios[scenario] = {}
        scenarios[scenario][scores["skill_set"]] = scores

    for scenario, sets in scenarios.items():
        lines.append(f"\n## {scenario}\n")
        lines.append("| Metric | " + " | ".join(sets.keys()) + " |")
        lines.append("|--------|" + "|".join(["-----"] * len(sets)) + "|")

        # Cost
        costs = [f"${s.get('total_cost_usd', 0):.4f}" for s in sets.values()]
        lines.append(f"| Cost | " + " | ".join(costs) + " |")

        # Tokens
        tokens = [f"{s.get('input_tokens', 0) + s.get('output_tokens', 0):,}" for s in sets.values()]
        lines.append(f"| Total Tokens | " + " | ".join(tokens) + " |")

        # Duration
        durations = [f"{s.get('duration_ms', 0) / 1000:.1f}s" for s in sets.values()]
        lines.append(f"| Duration | " + " | ".join(durations) + " |")

        # Tools
        tools = [", ".join(s.get("tools_used", [])) for s in sets.values()]
        lines.append(f"| Tools Used | " + " | ".join(tools) + " |")

        # Checks
        all_check_keys = set()
        for s in sets.values():
            all_check_keys.update(s.get("checks", {}).keys())

        for check in sorted(all_check_keys):
            values = []
            for s in sets.values():
                v = s.get("checks", {}).get(check)
                if v is True:
                    values.append("PASS")
                elif v is False:
                    values.append("FAIL")
                elif v is None:
                    values.append("-")
                else:
                    values.append(str(v)[:30])
            lines.append(f"| {check} | " + " | ".join(values) + " |")

    comparison_file = run_dir / "comparison.md"
    comparison_file.write_text("\n".join(lines))
    print(f"Comparison saved to: {comparison_file}")


if __name__ == "__main__":
    main()
