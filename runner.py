"""Eval runner: invokes Claude Code headlessly per scenario x skill-set."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import yaml
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Final


SCENARIOS_DIR = Path(__file__).parent / "scenarios"
RESULTS_DIR = Path(__file__).parent / "results"
VENV_DIR = Path(__file__).parent / ".venv"
RUN_TIMEOUT = 600  # 10 minutes per run
STALL_TIMEOUT = 120  # 2 minutes with no output = stalled
DBT_VERSION_TIMEOUT: Final[int] = 10
DBT_SETUP_TIMEOUT: Final[int] = 120
BOOTSTRAP_STG_SUPPLIES_SQL: Final[str] = """with

source as (

    select * from {{ source('ecom', 'raw_supplies') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
        id as supply_id,
        sku as product_id,
        name as supply_name,
        {{ cents_to_dollars('cost') }} as supply_cost,
        perishable as is_perishable_supply
    from source

)

select * from renamed
"""


@dataclass
class SkillSet:
    name: str
    skills: list[str]
    allowed_tools: list[str]


@dataclass
class Scenario:
    name: str
    path: Path
    prompt: str
    skill_sets: list[SkillSet]


@dataclass
class RunResult:
    scenario: str
    skill_set: str
    success: bool
    output_text: str = ""
    raw_messages: list[dict] = field(default_factory=list)
    duration_ms: int = 0
    total_cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    tools_used: list[str] = field(default_factory=list)
    num_turns: int = 0
    model: str = ""
    error: str = ""


def load_scenario(scenario_dir: Path) -> Scenario:
    """Load a scenario from its directory."""
    prompt = (scenario_dir / "prompt.txt").read_text().strip()
    skill_sets_data = yaml.safe_load((scenario_dir / "skill-sets.yaml").read_text())

    skill_sets = []
    for s in skill_sets_data["sets"]:
        skill_sets.append(SkillSet(
            name=s["name"],
            skills=s.get("skills", []),
            allowed_tools=s.get("allowed_tools", []),
        ))

    return Scenario(
        name=scenario_dir.name,
        path=scenario_dir,
        prompt=prompt,
        skill_sets=skill_sets,
    )


def _format_command(command: list[str]) -> str:
    return " ".join(command)


def _resolve_dbt_command() -> tuple[list[str] | None, str | None]:
    """Resolve a dbt Core command and explain why resolution failed when it does."""
    candidates = [
        [str(VENV_DIR / "Scripts" / "dbt.exe")],
        [str(VENV_DIR / "Scripts" / "dbt")],
        [str(VENV_DIR / "bin" / "dbt")],
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
        check=check,
        env=env,
        timeout=timeout,
    )


def _format_subprocess_error(exc: subprocess.CalledProcessError) -> str:
    parts = [part.strip() for part in (exc.stderr, exc.stdout) if part and part.strip()]
    if parts:
        return "\n".join(parts)[:1000]
    return f"{_format_command(exc.cmd if isinstance(exc.cmd, list) else [str(exc.cmd)])} exited with code {exc.returncode}"


def _copy_bootstrap_artifacts(bootstrap_dir: Path, temp_dir: Path) -> None:
    dbt_packages_dir = bootstrap_dir / "dbt_packages"
    if dbt_packages_dir.exists():
        shutil.copytree(dbt_packages_dir, temp_dir / "dbt_packages", dirs_exist_ok=True)

    package_lock = bootstrap_dir / "package-lock.yml"
    if package_lock.exists():
        shutil.copy2(package_lock, temp_dir / "package-lock.yml")

    for pattern in ("*.duckdb", "*.duckdb.wal"):
        for database_file in bootstrap_dir.glob(pattern):
            shutil.copy2(database_file, temp_dir / database_file.name)


def _bootstrap_create_staging_model_environment(
    temp_dir: Path,
    dbt_command: list[str],
    env: dict[str, str],
) -> None:
    bootstrap_dir = Path(tempfile.mkdtemp(prefix="dbt-bootstrap-create-staging-model-"))
    try:
        shutil.copytree(temp_dir, bootstrap_dir, dirs_exist_ok=True)
        stub_model = bootstrap_dir / "models" / "staging" / "stg_supplies.sql"
        stub_model.write_text(BOOTSTRAP_STG_SUPPLIES_SQL)

        _run_dbt(
            dbt_command,
            ["deps"],
            cwd=bootstrap_dir,
            env={**env, "DBT_PROFILES_DIR": str(bootstrap_dir)},
            timeout=DBT_SETUP_TIMEOUT,
            check=True,
        )
        _run_dbt(
            dbt_command,
            ["seed"],
            cwd=bootstrap_dir,
            env={**env, "DBT_PROFILES_DIR": str(bootstrap_dir)},
            timeout=DBT_SETUP_TIMEOUT,
            check=True,
        )
        _copy_bootstrap_artifacts(bootstrap_dir, temp_dir)
    finally:
        shutil.rmtree(bootstrap_dir, ignore_errors=True)


def prepare_environment(scenario: Scenario, skill_set: SkillSet) -> Path:
    """Copy context to a temp dir and prepare the dbt environment."""
    temp_dir = Path(tempfile.mkdtemp(prefix=f"dbt-eval-{scenario.name}-"))
    context_dir = scenario.path / "context"

    # Copy context files to temp dir
    shutil.copytree(context_dir, temp_dir, dirs_exist_ok=True)

    # Install dbt packages and seed data
    print(f"    Preparing dbt environment in {temp_dir}...")
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(temp_dir)

    dbt_command, resolution_error = _resolve_dbt_command()
    if dbt_command is None:
        print(f"    WARNING: dbt setup skipped: {resolution_error}")
        print("    Install project dependencies with: make setup")
        return temp_dir

    try:
        if scenario.name == "create-staging-model":
            _bootstrap_create_staging_model_environment(temp_dir, dbt_command, env)
        else:
            _run_dbt(
                dbt_command,
                ["deps"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_SETUP_TIMEOUT,
                check=True,
            )
            _run_dbt(
                dbt_command,
                ["seed"],
                cwd=temp_dir,
                env=env,
                timeout=DBT_SETUP_TIMEOUT,
                check=True,
            )
        print("    dbt deps + seed completed.")
    except subprocess.CalledProcessError as e:
        print(f"    WARNING: dbt setup failed with `{_format_command(dbt_command)}`:")
        print(f"    {_format_subprocess_error(e)}")
    except FileNotFoundError:
        print("    WARNING: dbt not found. Install with: make setup")

    return temp_dir


def run_claude(temp_dir: Path, prompt: str, skill_set: SkillSet) -> RunResult:
    """Run Claude Code headlessly and capture output."""
    dbt_command, _ = _resolve_dbt_command()
    if dbt_command is not None:
        dbt_cmd = _format_command(dbt_command)
        dbt_note = (
            f"IMPORTANT: Use `{dbt_cmd}` instead of `dbt` for all dbt commands. "
            f"The DBT_PROFILES_DIR is already set to the project directory."
        )
    else:
        dbt_note = (
            "IMPORTANT: No verified dbt Core command was found in this environment. "
            "If dbt commands fail, install project dependencies with `make setup` first. "
            "The DBT_PROFILES_DIR is already set to the project directory."
        )

    cmd = [
        "claude",
        "--print",
        "--verbose",
        "--output-format", "stream-json",
        "--max-turns", "15",
        "--allowedTools", ",".join(skill_set.allowed_tools),
        "--append-system-prompt", dbt_note,
        "-p", prompt,
    ]

    result = RunResult(scenario="", skill_set=skill_set.name, success=False)
    raw_messages = []
    output_parts = []
    last_output_time = time.time()

    # Ensure venv's dbt is on PATH
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(temp_dir)
    python_dir = str(Path(sys.executable).parent)
    env["PATH"] = python_dir + os.pathsep + env.get("PATH", "")
    venv_scripts = str(VENV_DIR / "Scripts")
    if os.path.isdir(venv_scripts):
        env["PATH"] = venv_scripts + os.pathsep + env.get("PATH", "")
    else:
        venv_bin = str(VENV_DIR / "bin")
        if os.path.isdir(venv_bin):
            env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=temp_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        # Read output with stall detection
        def read_stderr():
            for line in proc.stderr:
                pass  # Consume stderr to prevent blocking

        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        stderr_thread.start()

        start_time = time.time()
        for line in proc.stdout:
            last_output_time = time.time()
            line = line.strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
                raw_messages.append(msg)

                # Extract text output from assistant messages
                if msg.get("type") == "assistant":
                    for block in msg.get("message", {}).get("content", []):
                        if block.get("type") == "text":
                            output_parts.append(block["text"])
                        elif block.get("type") == "tool_use":
                            tool_name = block.get("name", "unknown")
                            if tool_name not in result.tools_used:
                                result.tools_used.append(tool_name)

                # Extract final result metrics
                if msg.get("type") == "result":
                    result.success = True
                    result.duration_ms = msg.get("duration_ms", 0)
                    result.total_cost_usd = msg.get("total_cost_usd", 0.0)
                    result.num_turns = msg.get("num_turns", 0)
                    result.model = msg.get("model", "")
                    usage = msg.get("usage", {})
                    result.input_tokens = usage.get("input_tokens", 0)
                    result.output_tokens = usage.get("output_tokens", 0)

            except json.JSONDecodeError:
                continue

            # Check timeouts
            elapsed = time.time() - start_time
            if elapsed > RUN_TIMEOUT:
                proc.kill()
                result.error = "Run timed out"
                break

        proc.wait(timeout=30)

    except Exception as e:
        result.error = str(e)

    result.output_text = "\n".join(output_parts)
    result.raw_messages = raw_messages
    return result


def detect_changes(original_dir: Path, modified_dir: Path) -> dict[str, str]:
    """Find files that were added or modified."""
    changes = {}
    for root, _, files in os.walk(modified_dir):
        for f in files:
            mod_path = Path(root) / f
            rel_path = mod_path.relative_to(modified_dir)

            # Skip non-project files
            if any(part.startswith(".") for part in rel_path.parts):
                continue
            if rel_path.parts[0] in ("target", "dbt_packages", "logs"):
                continue
            if f.endswith((".duckdb", ".duckdb.wal")):
                continue

            orig_path = original_dir / rel_path
            if not orig_path.exists():
                # New file
                changes[str(rel_path)] = mod_path.read_text(errors="replace")
            else:
                # Check if modified
                orig_content = orig_path.read_text(errors="replace")
                mod_content = mod_path.read_text(errors="replace")
                if orig_content != mod_content:
                    changes[str(rel_path)] = mod_content

    return changes


def save_results(
    result: RunResult,
    changes: dict[str, str],
    output_dir: Path,
):
    """Save run results to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save output text
    (output_dir / "output.md").write_text(result.output_text)

    # Save raw NDJSON
    with open(output_dir / "raw.jsonl", "w") as f:
        for msg in result.raw_messages:
            f.write(json.dumps(msg) + "\n")

    # Save metadata
    metadata = {
        "success": result.success,
        "model": result.model,
        "duration_ms": result.duration_ms,
        "num_turns": result.num_turns,
        "total_cost_usd": result.total_cost_usd,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "tools_used": result.tools_used,
        "error": result.error,
    }
    with open(output_dir / "metadata.yaml", "w") as f:
        yaml.dump(metadata, f, default_flow_style=False)

    # Save changed files
    if changes:
        changes_dir = output_dir / "changes"
        for rel_path, content in changes.items():
            out_path = changes_dir / rel_path
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(content)


def main():
    parser = argparse.ArgumentParser(description="Run dbt skill evals")
    parser.add_argument("--scenario", help="Run only this scenario")
    parser.add_argument("--skill-set", help="Run only this skill-set")
    args = parser.parse_args()

    # Discover scenarios
    scenario_dirs = sorted(p for p in SCENARIOS_DIR.iterdir() if p.is_dir())
    if args.scenario:
        scenario_dirs = [d for d in scenario_dirs if d.name == args.scenario]
        if not scenario_dirs:
            print(f"Scenario '{args.scenario}' not found.")
            sys.exit(1)

    scenarios = [load_scenario(d) for d in scenario_dirs]

    # Create timestamped run directory
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    run_dir = RESULTS_DIR / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Run directory: {run_dir}")
    print(f"Scenarios: {[s.name for s in scenarios]}")
    print()

    total_cost = 0.0

    for scenario in scenarios:
        skill_sets = scenario.skill_sets
        if args.skill_set:
            skill_sets = [s for s in skill_sets if s.name == args.skill_set]
            if not skill_sets:
                print(f"  Skill-set '{args.skill_set}' not found in {scenario.name}")
                continue

        for skill_set in skill_sets:
            print(f"  [{scenario.name}] [{skill_set.name}]")
            print(f"    Skills: {skill_set.skills or '(none)'}")

            # Prepare environment
            temp_dir = prepare_environment(scenario, skill_set)
            context_dir = scenario.path / "context"

            try:
                # Run Claude
                print(f"    Running Claude...")
                result = run_claude(temp_dir, scenario.prompt, skill_set)
                result.scenario = scenario.name

                # Detect changes
                changes = detect_changes(context_dir, temp_dir)

                # Save results
                output_dir = run_dir / scenario.name / skill_set.name
                save_results(result, changes, output_dir)

                # Print summary
                status = "OK" if result.success else f"FAIL: {result.error}"
                print(f"    Status: {status}")
                print(f"    Cost: ${result.total_cost_usd:.4f}")
                print(f"    Tokens: {result.input_tokens} in / {result.output_tokens} out")
                print(f"    Duration: {result.duration_ms / 1000:.1f}s")
                print(f"    Tools: {result.tools_used}")
                print(f"    Changes: {list(changes.keys()) or '(none)'}")
                print()

                total_cost += result.total_cost_usd

            finally:
                # Clean up temp dir
                shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Total cost: ${total_cost:.4f}")
    print(f"Results saved to: {run_dir}")


if __name__ == "__main__":
    main()
