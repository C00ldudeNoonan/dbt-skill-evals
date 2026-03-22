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


SCENARIOS_DIR = Path(__file__).parent / "scenarios"
RESULTS_DIR = Path(__file__).parent / "results"
VENV_DIR = Path(__file__).parent / ".venv"
RUN_TIMEOUT = 600  # 10 minutes per run
STALL_TIMEOUT = 120  # 2 minutes with no output = stalled


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


def _find_dbt() -> str:
    """Find dbt-core executable, preferring the project venv."""
    # Check venv (Windows and Unix)
    for candidate in [
        VENV_DIR / "Scripts" / "dbt.exe",
        VENV_DIR / "Scripts" / "dbt",
        VENV_DIR / "bin" / "dbt",
    ]:
        if candidate.exists():
            return str(candidate)
    return "dbt"  # Fall back to system dbt


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

    # Use venv dbt-core (system dbt may be dbt Cloud CLI)
    dbt_cmd = _find_dbt()

    try:
        subprocess.run(
            [dbt_cmd, "deps"],
            cwd=temp_dir, capture_output=True, text=True, check=True,
            env=env, timeout=120,
        )
        subprocess.run(
            [dbt_cmd, "seed"],
            cwd=temp_dir, capture_output=True, text=True, check=True,
            env=env, timeout=120,
        )
        print("    dbt deps + seed completed.")
    except subprocess.CalledProcessError as e:
        print(f"    WARNING: dbt setup failed: {e.stderr[:500]}")
    except FileNotFoundError:
        print("    WARNING: dbt not found. Install with: uv sync")

    return temp_dir


def run_claude(temp_dir: Path, prompt: str, skill_set: SkillSet) -> RunResult:
    """Run Claude Code headlessly and capture output."""
    dbt_cmd = _find_dbt()
    dbt_note = (
        f"IMPORTANT: Use `{dbt_cmd}` instead of `dbt` for all dbt commands. "
        f"The DBT_PROFILES_DIR is already set to the project directory."
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
