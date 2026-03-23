"""Improvement loop: turn scored eval runs into actionable skill feedback."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml


DEFAULT_SKILL_SET = "with-skills"
DEFAULT_OUTPUT_DIRNAME = "improvement"

CHECK_RECOMMENDATIONS: dict[str, dict[str, str]] = {
    "used_dbt_show": {
        "title": "Require Data Preview Before Value-Based Test Authoring",
        "instruction": (
            "Before writing tests that depend on actual values or business invariants, run "
            "`dbt show` (or an equivalent data preview command) against the target model and "
            "base the test logic on real rows."
        ),
        "rationale": "This reduces hallucinated assertions and grounds test design in actual model output.",
    },
    "uses_surrogate_key": {
        "title": "Call Out Composite Grain and Surrogate Keys Explicitly",
        "instruction": (
            "When a source repeats natural keys across another dimension, inspect the grain and "
            "prefer `dbt_utils.generate_surrogate_key(...)` for the staging model primary key."
        ),
        "rationale": "This helps the skill recognize repeated source grain instead of assuming a single-column key.",
    },
    "has_expression_test": {
        "title": "Add Arithmetic Invariants When They Are Obvious in SQL",
        "instruction": (
            "When a model exposes additive financial fields such as subtotal, tax, and total, "
            "consider adding an `expression_is_true` test for the invariant if it can be validated safely."
        ),
        "rationale": "This improves solution quality on test-authoring tasks without changing model logic.",
    },
    "has_customer_relationship": {
        "title": "Prefer Relationship Tests for Clear Foreign Keys",
        "instruction": (
            "When a column is clearly a foreign key and the parent model already exists, add a "
            "`relationships` test that points at the correct parent `ref()` and field."
        ),
        "rationale": "This reinforces referential-integrity coverage in schema YAML files.",
    },
    "has_location_relationship": {
        "title": "Prefer Relationship Tests for Clear Foreign Keys",
        "instruction": (
            "When a column is clearly a foreign key and the parent model already exists, add a "
            "`relationships` test that points at the correct parent `ref()` and field."
        ),
        "rationale": "This reinforces referential-integrity coverage in schema YAML files.",
    },
    "used_targeted_dbt_test": {
        "title": "Run the Smallest Validation Command That Matches the Edit",
        "instruction": (
            "After editing one model or schema file, prefer a targeted `dbt test --select ...` "
            "command rather than a broad project run."
        ),
        "rationale": "Targeted validation shortens feedback loops and makes tool usage more intentional.",
    },
    "uses_source_macro": {
        "title": "Prefer `source()` and `ref()` Over Hardcoded Relations",
        "instruction": (
            "Never hardcode raw schema relations when a declared source exists. Use `source()` for "
            "raw tables and `ref()` for model dependencies."
        ),
        "rationale": "This preserves lineage, portability, and project conventions.",
    },
    "removes_hardcoded_relation": {
        "title": "Eliminate Hardcoded Raw Relations",
        "instruction": (
            "When refactoring staging models, replace hardcoded raw schema references with the "
            "declared `source()` macro and preserve the existing transformation logic."
        ),
        "rationale": "This reduces convention drift and avoids brittle environment-specific SQL.",
    },
    "used_targeted_compile": {
        "title": "Compile the Edited Model Explicitly",
        "instruction": (
            "After changing SQL in a single model, run a targeted `dbt compile --select <model>` "
            "or equivalent validation command."
        ),
        "rationale": "This catches syntax and dependency errors with minimal extra work.",
    },
    "has_raw_orders_freshness": {
        "title": "Add Freshness When `loaded_at_field` Already Exists",
        "instruction": (
            "If a source table already defines `loaded_at_field` and the task mentions freshness, "
            "add a `freshness` block to that table instead of changing unrelated source metadata."
        ),
        "rationale": "This helps the skill modify source YAML cleanly and locally.",
    },
    "warn_after_12h": {
        "title": "Preserve Exact Thresholds in Source Freshness Tasks",
        "instruction": (
            "When a task gives explicit source freshness thresholds, copy the requested `warn_after` "
            "and `error_after` values exactly."
        ),
        "rationale": "These tasks are often graded deterministically against exact YAML values.",
    },
    "error_after_24h": {
        "title": "Preserve Exact Thresholds in Source Freshness Tasks",
        "instruction": (
            "When a task gives explicit source freshness thresholds, copy the requested `warn_after` "
            "and `error_after` values exactly."
        ),
        "rationale": "These tasks are often graded deterministically against exact YAML values.",
    },
    "used_targeted_source_freshness": {
        "title": "Validate Freshness Changes With Targeted Freshness Commands",
        "instruction": (
            "After editing source freshness config, run `dbt source freshness --select <source>` "
            "for the specific source table you changed."
        ),
        "rationale": "This confirms the YAML is valid and keeps the validation step focused.",
    },
    "has_unit_test": {
        "title": "Reach for Unit Tests on Branching Transformation Logic",
        "instruction": (
            "When a model derives booleans or category flags from source fields, consider adding a "
            "unit test with representative input rows and explicit expected outputs."
        ),
        "rationale": "This makes transformation behavior more concrete than relying only on generic data tests.",
    },
    "uses_raw_products_source_input": {
        "title": "Use Realistic Source Inputs in Unit Tests",
        "instruction": (
            "When adding a unit test for a staging model, prefer `source(...)` inputs that mirror the "
            "actual upstream table rather than unrelated refs."
        ),
        "rationale": "This keeps the test aligned with the model's true contract.",
    },
    "asserts_boolean_outputs": {
        "title": "Assert the Derived Fields Directly in Unit Tests",
        "instruction": (
            "For unit tests around derived booleans or categories, include expected rows that assert "
            "the transformed output columns directly."
        ),
        "rationale": "The skill should prove the transformation, not just execute it.",
    },
}


@dataclass(slots=True)
class ScenarioFinding:
    scenario: str
    classification: str
    failing_checks: list[str]
    changed_files: list[str]
    tools_used: list[str]
    commands: list[str]
    output_excerpt: str


@dataclass(slots=True)
class Recommendation:
    check: str
    title: str
    instruction: str
    rationale: str
    classification: str
    scenarios: list[str]


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text()) or {}


def _load_jsonl(path: Path) -> list[dict]:
    entries: list[dict] = []
    if not path.exists():
        return entries
    for line in path.read_text(errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def _load_tool_commands(raw_file: Path) -> list[str]:
    commands: list[str] = []
    for payload in _load_jsonl(raw_file):
        if payload.get("type") != "assistant":
            continue
        message = payload.get("message", {})
        for block in message.get("content", []):
            if block.get("type") != "tool_use":
                continue
            command = block.get("input", {}).get("command")
            if isinstance(command, str):
                commands.append(command)
    return commands


def _load_changed_files(changes_dir: Path) -> list[str]:
    if not changes_dir.exists():
        return []
    changed_files: list[str] = []
    for file_path in sorted(changes_dir.rglob("*")):
        if file_path.is_file():
            changed_files.append(str(file_path.relative_to(changes_dir)))
    return changed_files


def _output_excerpt(output_file: Path, *, limit: int = 600) -> str:
    if not output_file.exists():
        return ""
    text = output_file.read_text(errors="replace").strip()
    return text[:limit]


def _classification(
    *,
    with_skills_checks: dict[str, object],
    no_skills_checks: dict[str, object] | None,
    failing_checks: list[str],
) -> str:
    if no_skills_checks is None:
        return "with-skills-failure"
    if any(no_skills_checks.get(check) is True for check in failing_checks):
        return "skill-regression"
    return "shared-gap"


def _build_findings(run_dir: Path, scores: dict, skill_set: str) -> list[ScenarioFinding]:
    findings: list[ScenarioFinding] = []

    scenario_names = sorted({entry["scenario"] for entry in scores.values()})
    for scenario_name in scenario_names:
        target = scores.get(f"{scenario_name}/{skill_set}")
        if target is None:
            continue

        checks = target.get("checks", {})
        failing_checks = sorted(check for check, value in checks.items() if value is False)
        if not failing_checks:
            continue

        control = scores.get(f"{scenario_name}/no-skills")
        control_checks = control.get("checks", {}) if isinstance(control, dict) else None
        scenario_dir = run_dir / scenario_name / skill_set

        findings.append(
            ScenarioFinding(
                scenario=scenario_name,
                classification=_classification(
                    with_skills_checks=checks,
                    no_skills_checks=control_checks,
                    failing_checks=failing_checks,
                ),
                failing_checks=failing_checks,
                changed_files=_load_changed_files(scenario_dir / "changes"),
                tools_used=list(target.get("tools_used", [])),
                commands=_load_tool_commands(scenario_dir / "raw.jsonl"),
                output_excerpt=_output_excerpt(scenario_dir / "output.md"),
            )
        )

    return findings


def _build_recommendations(findings: list[ScenarioFinding]) -> list[Recommendation]:
    aggregated: dict[str, Recommendation] = {}

    for finding in findings:
        for check in finding.failing_checks:
            template = CHECK_RECOMMENDATIONS.get(
                check,
                {
                    "title": f"Address `{check}` Failures",
                    "instruction": (
                        f"Add explicit guidance so the agent reliably satisfies the `{check}` check "
                        "on similar tasks."
                    ),
                    "rationale": "This check is failing in the eval results and should be explained more clearly in the skill.",
                },
            )

            current = aggregated.get(check)
            if current is None:
                aggregated[check] = Recommendation(
                    check=check,
                    title=template["title"],
                    instruction=template["instruction"],
                    rationale=template["rationale"],
                    classification=finding.classification,
                    scenarios=[finding.scenario],
                )
                continue

            if finding.scenario not in current.scenarios:
                current.scenarios.append(finding.scenario)
            if current.classification != "skill-regression" and finding.classification == "skill-regression":
                current.classification = "skill-regression"

    return sorted(
        aggregated.values(),
        key=lambda item: (item.classification != "skill-regression", item.check),
    )


def _write_markdown_report(
    output_path: Path,
    *,
    run_dir: Path,
    skill_set: str,
    findings: list[ScenarioFinding],
    recommendations: list[Recommendation],
) -> None:
    lines = ["# Skill Improvement Report", ""]
    lines.append(f"- Run directory: `{run_dir}`")
    lines.append(f"- Target skill set: `{skill_set}`")
    lines.append(f"- Findings: `{len(findings)}` scenarios with failing deterministic checks")
    lines.append("")

    if not findings:
        lines.append("## Outcome")
        lines.append("")
        lines.append("No deterministic failures were found for the target skill set in this scored run.")
    else:
        lines.append("## Findings")
        lines.append("")
        for finding in findings:
            lines.append(f"### {finding.scenario}")
            lines.append(f"- Classification: `{finding.classification}`")
            lines.append(f"- Failing checks: `{', '.join(finding.failing_checks)}`")
            lines.append(f"- Changed files: `{', '.join(finding.changed_files) if finding.changed_files else '(none)'}`")
            lines.append(f"- Tools used: `{', '.join(finding.tools_used) if finding.tools_used else '(none)'}`")

            if finding.commands:
                lines.append("- Bash commands:")
                for command in finding.commands[:4]:
                    lines.append(f"  - `{command}`")

            if finding.output_excerpt:
                lines.append("- Output excerpt:")
                lines.append("")
                lines.append("```text")
                lines.append(finding.output_excerpt)
                lines.append("```")
            lines.append("")

        lines.append("## Recommendations")
        lines.append("")
        for recommendation in recommendations:
            lines.append(f"### {recommendation.title}")
            lines.append(f"- Check: `{recommendation.check}`")
            lines.append(f"- Priority: `{recommendation.classification}`")
            lines.append(f"- Scenarios: `{', '.join(recommendation.scenarios)}`")
            lines.append(f"- Suggested instruction: {recommendation.instruction}")
            lines.append(f"- Rationale: {recommendation.rationale}")
            lines.append("")

        lines.append("## Loop Boundary")
        lines.append("")
        lines.append(
            "This repo does not edit the external dbt skill directly. Apply the recommended updates in "
            "the skill source, rerun the evals, and compare against baseline."
        )

    output_path.write_text("\n".join(lines) + "\n")


def _write_prompt(
    output_path: Path,
    *,
    findings: list[ScenarioFinding],
    recommendations: list[Recommendation],
) -> None:
    lines = [
        "You are updating the dbt agent skill using eval feedback.",
        "",
        "Goal: improve the skill instructions so future runs avoid the observed failures without overfitting to one exact scenario.",
        "",
        "Observed failures:",
    ]

    if not findings:
        lines.append("- No deterministic failures were present in the target skill set for this run.")
    else:
        for finding in findings:
            lines.append(
                f"- {finding.scenario}: {finding.classification}; failing checks = {', '.join(finding.failing_checks)}"
            )

    lines.extend(["", "Recommended instruction changes:"])
    if not recommendations:
        lines.append("- No changes required from this run.")
    else:
        for recommendation in recommendations:
            lines.append(f"- {recommendation.instruction}")

    lines.extend(
        [
            "",
            "When proposing the skill edit:",
            "- preserve general dbt best practices",
            "- prefer concise instructions that generalize across tasks",
            "- avoid encoding scenario names or test-specific hacks",
            "- include explicit validation behaviors when the failures are tool-usage related",
            "",
            "Respond with:",
            "1. a short rationale",
            "2. the exact skill text changes you recommend",
            "3. why those changes should improve the failing eval checks",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n")


def _write_json(output_path: Path, *, findings: list[ScenarioFinding], recommendations: list[Recommendation]) -> None:
    payload = {
        "findings": [asdict(item) for item in findings],
        "recommendations": [asdict(item) for item in recommendations],
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate skill improvement feedback from a scored eval run")
    parser.add_argument("run_dir", help="Path to a scored run directory")
    parser.add_argument("--skill-set", default=DEFAULT_SKILL_SET, help="Skill set to analyze")
    parser.add_argument(
        "--output-dir",
        help="Directory for improvement artifacts (defaults to <run_dir>/improvement)",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    scores_file = run_dir / "scores.yaml"
    if not scores_file.exists():
        raise SystemExit(f"Scored run not found: {scores_file}. Run scorer.py first.")

    scores = _load_yaml(scores_file)
    output_dir = Path(args.output_dir) if args.output_dir else run_dir / DEFAULT_OUTPUT_DIRNAME
    output_dir.mkdir(parents=True, exist_ok=True)

    findings = _build_findings(run_dir, scores, args.skill_set)
    recommendations = _build_recommendations(findings)

    _write_markdown_report(
        output_dir / "report.md",
        run_dir=run_dir,
        skill_set=args.skill_set,
        findings=findings,
        recommendations=recommendations,
    )
    _write_prompt(output_dir / "skill_update_prompt.txt", findings=findings, recommendations=recommendations)
    _write_json(output_dir / "feedback.json", findings=findings, recommendations=recommendations)

    print(f"Improvement report: {output_dir / 'report.md'}")
    print(f"Skill update prompt: {output_dir / 'skill_update_prompt.txt'}")
    print(f"Machine-readable feedback: {output_dir / 'feedback.json'}")
    print(f"Findings: {len(findings)}")
    print(f"Recommendations: {len(recommendations)}")


if __name__ == "__main__":
    main()
