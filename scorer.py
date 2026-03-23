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
from typing import Final


SCENARIOS_DIR = Path(__file__).parent / "scenarios"
BASELINES_DIR = Path(__file__).parent / "baselines"
DBT_VERSION_TIMEOUT: Final[int] = 10
DBT_COMMAND_TIMEOUT: Final[int] = 120
LLM_GRADING_MAX_TURNS: Final[str] = "3"

# --- Deterministic validators per scenario ---


def validate_create_staging_model(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
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


def validate_add_tests_to_model(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
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
    checks["used_dbt_show"] = any(_command_mentions_dbt_show(command) for command in tool_commands)

    # Check for hallucinated accepted_values (rough heuristic)
    if yml_matches:
        yml_content = changes[yml_matches[0]]
        checks["no_accepted_values_hallucination"] = "accepted_values" not in yml_content

    return checks


def validate_debug_failing_build(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
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


def validate_add_relationships_tests(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
    checks = {}

    yml_matches = [k for k in changes if "stg_orders" in k and k.endswith(".yml")]
    checks["yml_modified"] = len(yml_matches) > 0

    if yml_matches:
        yml_content = changes[yml_matches[0]]
        checks["has_customer_relationship"] = _has_column_relationship_test(
            yml_content,
            column_name="customer_id",
            to_ref="ref('stg_customers')",
            field="customer_id",
        )
        checks["has_location_relationship"] = _has_column_relationship_test(
            yml_content,
            column_name="location_id",
            to_ref="ref('stg_locations')",
            field="location_id",
        )
        checks["preserves_order_id_tests"] = (
            _has_column_test_name(yml_content, column_name="order_id", test_name="not_null")
            and _has_column_test_name(yml_content, column_name="order_id", test_name="unique")
        )
        checks["preserves_expression_test"] = _has_model_test_name(
            yml_content,
            test_name="dbt_utils.expression_is_true",
        )

    checks["used_targeted_dbt_test"] = any(
        _command_mentions_targeted_stg_orders_test(command) for command in tool_commands
    )

    return checks


def validate_refactor_hardcoded_source(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
    checks = {}

    sql_matches = [k for k in changes if "stg_products" in k and k.endswith(".sql")]
    checks["sql_file_modified"] = len(sql_matches) > 0

    if sql_matches:
        content = changes[sql_matches[0]]
        checks["uses_source_macro"] = bool(
            re.search(r"source\s*\(\s*['\"]ecom['\"]\s*,\s*['\"]raw_products['\"]\s*\)", content)
        )
        lowered = content.lower()
        checks["removes_hardcoded_relation"] = (
            "raw.raw_products" not in lowered
            and '"raw"."raw_products"' not in lowered
        )
        checks["preserves_cents_to_dollars"] = "cents_to_dollars('price')" in content or 'cents_to_dollars("price")' in content
        checks["preserves_food_flag"] = "is_food_item" in content and "type = 'jaffle'" in content
        checks["preserves_drink_flag"] = "is_drink_item" in content and "type = 'beverage'" in content

    checks["used_targeted_compile"] = any(
        _command_mentions_targeted_compile(command, model_name="stg_products")
        for command in tool_commands
    )

    return checks


def validate_add_source_freshness(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
    checks = {}

    yml_matches = [k for k in changes if k.endswith("__sources.yml")]
    checks["yml_modified"] = len(yml_matches) > 0

    if yml_matches:
        yml_content = changes[yml_matches[0]]
        raw_orders = _find_source_table_entry(yml_content, source_name="ecom", table_name="raw_orders")
        checks["has_raw_orders_freshness"] = bool(raw_orders and isinstance(raw_orders.get("freshness"), dict))
        checks["warn_after_12h"] = _freshness_threshold_matches(
            raw_orders,
            threshold_name="warn_after",
            count=12,
            period="hour",
        )
        checks["error_after_24h"] = _freshness_threshold_matches(
            raw_orders,
            threshold_name="error_after",
            count=24,
            period="hour",
        )
        checks["preserves_loaded_at_field"] = bool(raw_orders and raw_orders.get("loaded_at_field") == "ordered_at")

    checks["used_targeted_source_freshness"] = any(
        _command_mentions_targeted_source_freshness(command, source_selector="source:ecom.raw_orders")
        for command in tool_commands
    )

    return checks


def validate_add_unit_test_to_model(
    changes: dict[str, str],
    output_text: str,
    tool_commands: list[str],
) -> dict:
    checks = {}

    yml_matches = [k for k in changes if "stg_products" in k and k.endswith(".yml")]
    checks["yml_modified"] = len(yml_matches) > 0

    if yml_matches:
        yml_content = changes[yml_matches[0]]
        unit_test = _find_unit_test_for_model(yml_content, model_name="stg_products")
        checks["has_unit_test"] = unit_test is not None
        checks["uses_raw_products_source_input"] = _unit_test_uses_input(unit_test, "source('ecom', 'raw_products')")
        checks["asserts_boolean_outputs"] = _unit_test_asserts_fields(
            unit_test,
            field_names=["is_food_item", "is_drink_item"],
        )
        checks["preserves_product_id_tests"] = (
            _has_column_test_name_for_model(
                yml_content,
                model_name="stg_products",
                column_name="product_id",
                test_name="not_null",
            )
            and _has_column_test_name_for_model(
                yml_content,
                model_name="stg_products",
                column_name="product_id",
                test_name="unique",
            )
        )

    checks["used_targeted_dbt_test"] = any(
        _command_mentions_targeted_model_test(command, model_name="stg_products")
        for command in tool_commands
    )

    return checks


VALIDATORS = {
    "add-relationships-tests": validate_add_relationships_tests,
    "add-source-freshness": validate_add_source_freshness,
    "add-unit-test-to-model": validate_add_unit_test_to_model,
    "create-staging-model": validate_create_staging_model,
    "add-tests-to-model": validate_add_tests_to_model,
    "debug-failing-build": validate_debug_failing_build,
    "refactor-hardcoded-source": validate_refactor_hardcoded_source,
}


def _format_command(command: list[str]) -> str:
    return " ".join(command)


def _resolve_dbt_command() -> tuple[list[str] | None, str | None]:
    candidates = [
        [str(Path(__file__).parent / ".venv" / "Scripts" / "dbt.exe")],
        [str(Path(__file__).parent / ".venv" / "Scripts" / "dbt")],
        [str(Path(__file__).parent / ".venv" / "bin" / "dbt")],
        [sys.executable, "-m", "dbt.cli.main"],
    ]

    system_dbt = shutil.which("dbt")
    if system_dbt:
        candidates.append([system_dbt])

    failure_notes: list[str] = []
    for command in candidates:
        executable = Path(command[0])
        if len(command) == 1 and executable.is_absolute() and not executable.exists():
            continue

        try:
            version_result = subprocess.run(
                [*command, "--version"],
                capture_output=True,
                text=True,
                timeout=DBT_VERSION_TIMEOUT,
            )
        except FileNotFoundError:
            continue
        except Exception as exc:
            failure_notes.append(f"{_format_command(command)}: {exc}")
            continue

        version_text = "\n".join(
            part.strip() for part in (version_result.stdout, version_result.stderr) if part.strip()
        )
        lowered = version_text.lower()

        if version_result.returncode == 0 and "dbt cloud cli" not in lowered:
            return command, None

        reason = version_text or f"exit code {version_result.returncode}"
        failure_notes.append(f"{_format_command(command)}: {reason}")

    if failure_notes:
        return None, "; ".join(failure_notes)
    return None, "No dbt executable or importable dbt module was found."


def _run_dbt(
    command: list[str],
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: int,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*command, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        check=check,
    )


def _summarize_dbt_failure(result: subprocess.CompletedProcess[str]) -> str:
    parts = [part.strip() for part in (result.stderr, result.stdout) if part and part.strip()]
    if parts:
        return "\n".join(parts)[:500]
    return f"{_format_command(list(result.args))} exited with code {result.returncode}"


def _command_mentions_dbt_show(command: str) -> bool:
    lowered = command.lower()
    return "dbt show" in lowered or "dbt.cli.main show" in lowered


def _command_mentions_targeted_compile(command: str, *, model_name: str) -> bool:
    lowered = command.lower()
    return (
        ("dbt compile" in lowered or "dbt.cli.main compile" in lowered)
        and f"--select {model_name.lower()}" in lowered
    )


def _command_mentions_targeted_source_freshness(command: str, *, source_selector: str) -> bool:
    lowered = command.lower()
    return (
        ("dbt source freshness" in lowered or "dbt.cli.main source freshness" in lowered)
        and source_selector.lower() in lowered
    )


def _command_mentions_targeted_stg_orders_test(command: str) -> bool:
    lowered = command.lower()
    return (
        ("dbt test" in lowered or "dbt.cli.main test" in lowered)
        and "path:models/staging/stg_orders.yml" in lowered
    )


def _command_mentions_targeted_model_test(command: str, *, model_name: str) -> bool:
    lowered = command.lower()
    return (
        ("dbt test" in lowered or "dbt.cli.main test" in lowered)
        and model_name.lower() in lowered
    )


def _load_schema_models(yml_content: str) -> list[dict]:
    parsed = yaml.safe_load(yml_content) or {}
    models = parsed.get("models", [])
    return [model for model in models if isinstance(model, dict)]


def _find_model_entry(yml_content: str, model_name: str) -> dict | None:
    for model in _load_schema_models(yml_content):
        if model.get("name") == model_name:
            return model
    return None


def _find_column_entry(yml_content: str, column_name: str) -> dict | None:
    return _find_column_entry_for_model(yml_content, model_name="stg_orders", column_name=column_name)


def _find_column_entry_for_model(yml_content: str, *, model_name: str, column_name: str) -> dict | None:
    model = _find_model_entry(yml_content, model_name)
    if model is None:
        return None

    columns = model.get("columns", [])
    for column in columns:
        if isinstance(column, dict) and column.get("name") == column_name:
            return column
    return None


def _normalize_test_arguments(test_value: object) -> dict:
    if not isinstance(test_value, dict):
        return {}
    arguments = test_value.get("arguments")
    if isinstance(arguments, dict):
        return arguments
    return test_value


def _has_column_test_name(yml_content: str, *, column_name: str, test_name: str) -> bool:
    return _has_column_test_name_for_model(
        yml_content,
        model_name="stg_orders",
        column_name=column_name,
        test_name=test_name,
    )


def _has_column_test_name_for_model(
    yml_content: str,
    *,
    model_name: str,
    column_name: str,
    test_name: str,
) -> bool:
    column = _find_column_entry_for_model(yml_content, model_name=model_name, column_name=column_name)
    if column is None:
        return False

    for test in column.get("data_tests", []):
        if test == test_name:
            return True
        if isinstance(test, dict) and test_name in test:
            return True
    return False


def _has_model_test_name(yml_content: str, *, test_name: str) -> bool:
    return _has_model_test_name_for_model(yml_content, model_name="stg_orders", test_name=test_name)


def _has_model_test_name_for_model(yml_content: str, *, model_name: str, test_name: str) -> bool:
    model = _find_model_entry(yml_content, model_name)
    if model is None:
        return False

    for test in model.get("data_tests", []):
        if test == test_name:
            return True
        if isinstance(test, dict) and test_name in test:
            return True
    return False


def _has_column_relationship_test(
    yml_content: str,
    *,
    column_name: str,
    to_ref: str,
    field: str,
) -> bool:
    column = _find_column_entry(yml_content, column_name)
    if column is None:
        return False

    for test in column.get("data_tests", []):
        if not isinstance(test, dict) or "relationships" not in test:
            continue
        arguments = _normalize_test_arguments(test["relationships"])
        if arguments.get("to") == to_ref and arguments.get("field") == field:
            return True
    return False


def _load_sources(yml_content: str) -> list[dict]:
    parsed = yaml.safe_load(yml_content) or {}
    sources = parsed.get("sources", [])
    return [source for source in sources if isinstance(source, dict)]


def _find_source_table_entry(yml_content: str, *, source_name: str, table_name: str) -> dict | None:
    for source in _load_sources(yml_content):
        if source.get("name") != source_name:
            continue
        for table in source.get("tables", []):
            if isinstance(table, dict) and table.get("name") == table_name:
                return table
    return None


def _freshness_threshold_matches(
    source_table: dict | None,
    *,
    threshold_name: str,
    count: int,
    period: str,
) -> bool:
    if source_table is None:
        return False
    freshness = source_table.get("freshness")
    if not isinstance(freshness, dict):
        return False
    threshold = freshness.get(threshold_name)
    return isinstance(threshold, dict) and threshold.get("count") == count and threshold.get("period") == period


def _load_unit_tests(yml_content: str) -> list[dict]:
    parsed = yaml.safe_load(yml_content) or {}
    unit_tests = parsed.get("unit_tests", [])
    return [unit_test for unit_test in unit_tests if isinstance(unit_test, dict)]


def _find_unit_test_for_model(yml_content: str, *, model_name: str) -> dict | None:
    for unit_test in _load_unit_tests(yml_content):
        model = unit_test.get("model")
        if model == model_name or model == f"ref('{model_name}')":
            return unit_test
    return None


def _unit_test_uses_input(unit_test: dict | None, input_value: str) -> bool:
    if unit_test is None:
        return False
    given = unit_test.get("given", [])
    for item in given:
        if isinstance(item, dict) and item.get("input") == input_value:
            return True
    return False


def _unit_test_asserts_fields(unit_test: dict | None, *, field_names: list[str]) -> bool:
    if unit_test is None:
        return False
    expect = unit_test.get("expect", {})
    rows = expect.get("rows", []) if isinstance(expect, dict) else []
    if not rows:
        return False
    return all(
        any(isinstance(row, dict) and field_name in row for row in rows)
        for field_name in field_names
    )


def _load_tool_commands(raw_file: Path) -> list[str]:
    commands: list[str] = []
    if not raw_file.exists():
        return commands

    for line in raw_file.read_text(errors="replace").splitlines():
        if not line.strip():
            continue

        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue

        if payload.get("type") != "assistant":
            continue

        message = payload.get("message", {})
        for block in message.get("content", []):
            if block.get("type") != "tool_use" or block.get("name") != "Bash":
                continue
            command = block.get("input", {}).get("command")
            if isinstance(command, str):
                commands.append(command)

    return commands


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
        dbt_command, resolution_error = _resolve_dbt_command()
        if dbt_command is None:
            checks["dbt_available"] = False
            checks["dbt_unavailable_reason"] = resolution_error
            return checks

        # Install deps and seed
        deps_result = _run_dbt(
            dbt_command,
            ["deps"],
            cwd=temp_dir,
            env=env,
            timeout=DBT_COMMAND_TIMEOUT,
        )
        if deps_result.returncode != 0:
            checks["dbt_setup_passes"] = False
            checks["dbt_setup_error"] = _summarize_dbt_failure(deps_result)
            return checks

        seed_result = _run_dbt(
            dbt_command,
            ["seed"],
            cwd=temp_dir,
            env=env,
            timeout=DBT_COMMAND_TIMEOUT,
        )
        if seed_result.returncode != 0:
            checks["dbt_setup_passes"] = False
            checks["dbt_setup_error"] = _summarize_dbt_failure(seed_result)
            return checks

        checks["dbt_setup_passes"] = True

        # Run scenario-specific dbt command
        if scenario_name == "create-staging-model":
            r = _run_dbt(
                dbt_command,
                ["compile", "--select", "stg_supplies"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_compile_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_compile_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "add-tests-to-model":
            r = _run_dbt(
                dbt_command,
                ["test", "--select", "stg_orders"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_test_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_test_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "add-relationships-tests":
            r = _run_dbt(
                dbt_command,
                ["test", "--select", "path:models/staging/stg_orders.yml"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_test_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_test_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "refactor-hardcoded-source":
            r = _run_dbt(
                dbt_command,
                ["compile", "--select", "stg_products"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_compile_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_compile_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "add-source-freshness":
            r = _run_dbt(
                dbt_command,
                ["source", "freshness", "--select", "source:ecom.raw_orders"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_source_freshness_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_source_freshness_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "add-unit-test-to-model":
            r = _run_dbt(
                dbt_command,
                ["test", "--select", "stg_products"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_COMMAND_TIMEOUT,
            )
            checks["dbt_test_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_test_error"] = _summarize_dbt_failure(r)

        elif scenario_name == "debug-failing-build":
            r = _run_dbt(
                dbt_command,
                ["build"],
                cwd=temp_dir,
                env=env,
                timeout=300,
            )
            checks["dbt_build_passes"] = r.returncode == 0
            if r.returncode != 0:
                checks["dbt_build_error"] = _summarize_dbt_failure(r)

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
            ["claude", "--print", "-p", grading_prompt, "--max-turns", LLM_GRADING_MAX_TURNS],
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
            tool_commands = _load_tool_commands(skill_set_dir / "raw.jsonl")

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
                checks = validator(changes, output_text, tool_commands)
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
