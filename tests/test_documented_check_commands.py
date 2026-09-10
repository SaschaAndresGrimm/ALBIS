"""The checks the docs ask contributors to run must be the checks CI runs.

This block has drifted twice before and been hand-patched twice, and by the
time it was measured it carried three separate defects at once: the pytest
line omitted `-p pytest_cov` while setting `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`,
so it exited with `unrecognized arguments: --cov=backend` rather than running;
its coverage gate read 50 against CI's 77; and its ruff invocation skipped
`albis_launcher.py`, 29 kB of code CI does lint.

A contributor who runs a command that errors out, or one weaker than the gate
their pull request will meet, learns the wrong thing about the state of their
branch. Comparing the text to the workflow is the only way this stays true.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _ci_run_lines() -> dict[str, str]:
    """The `run:` body of each named CI step, keyed by step name."""
    workflow = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    steps: dict[str, str] = {}
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            if "run" in step and step.get("name"):
                steps[step["name"]] = " ".join(step["run"].split())
    return steps


def _code_block_lines(markdown: Path, marker: str) -> list[str]:
    """The lines of the first fenced block following `marker`."""
    text = markdown.read_text(encoding="utf-8")
    start = text.index(marker)
    fence = text.index("```", start)
    body_start = text.index("\n", fence) + 1
    body_end = text.index("```", body_start)
    return [line.strip() for line in text[body_start:body_end].splitlines() if line.strip()]


@pytest.fixture(scope="module")
def ci_steps() -> dict[str, str]:
    return _ci_run_lines()


def test_the_ci_pytest_step_still_enables_the_coverage_plugin(ci_steps) -> None:
    """Guards the guard: everything below compares against this line."""
    pytest_step = ci_steps["Pytest"]

    assert "-p pytest_cov" in pytest_step, (
        "CI's pytest step lost `-p pytest_cov`. With "
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 set in the job env, the --cov flags "
        "are then unrecognized and pytest exits without running."
    )
    assert "--cov-fail-under=" in pytest_step


@pytest.mark.parametrize(
    ("doc", "marker"),
    [
        (REPO_ROOT / "CONTRIBUTING.md", "## Local checks before PR"),
        (REPO_ROOT / "docs" / "DEVELOPER_GUIDE.md", "Run local checks:"),
    ],
    ids=["CONTRIBUTING.md", "DEVELOPER_GUIDE.md"],
)
def test_a_documented_check_block_matches_the_ci_commands(doc: Path, marker: str, ci_steps) -> None:
    documented = _code_block_lines(doc, marker)
    ci_ruff = ci_steps["Ruff"]
    ci_black = ci_steps["Black"]
    ci_pytest = ci_steps["Pytest"]

    ruff_line = next((line for line in documented if line.startswith("ruff check")), None)
    black_line = next((line for line in documented if line.startswith("black ")), None)
    pytest_line = next((line for line in documented if "pytest" in line), None)

    assert ruff_line == ci_ruff, f"{doc.name} ruff line drifted from ci.yml"
    assert black_line == ci_black, f"{doc.name} black line drifted from ci.yml"
    assert pytest_line is not None, f"{doc.name} documents no pytest command"

    # The doc prefixes the env var that the CI job sets in `env:`; the rest of
    # the invocation has to match, flags and gate alike.
    normalised = pytest_line.replace("PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 ", "", 1)
    assert normalised == ci_pytest, (
        f"{doc.name} pytest line drifted from ci.yml:\n"
        f"  documented: {normalised}\n  ci.yml    : {ci_pytest}"
    )


def test_the_documented_pytest_command_is_runnable_as_written() -> None:
    """The specific failure that shipped: flags that pytest cannot parse.

    Checked by argument shape rather than by running it -- running the suite
    from inside the suite would recurse.
    """
    documented = _code_block_lines(REPO_ROOT / "CONTRIBUTING.md", "## Local checks before PR")
    pytest_line = next(line for line in documented if "pytest" in line)

    if "PYTEST_DISABLE_PLUGIN_AUTOLOAD=1" in pytest_line:
        assert "-p pytest_cov" in pytest_line, (
            "Plugin autoloading is disabled without loading pytest_cov explicitly, "
            "so the --cov flags below are unrecognized and pytest exits 4 "
            "without running a single test."
        )


def test_the_pull_request_template_checklist_matches_ci(ci_steps) -> None:
    template = (REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md").read_text(encoding="utf-8")
    checklist = re.findall(r"^- \[ \] `([^`]+)`", template, flags=re.MULTILINE)

    ruff_item = next((item for item in checklist if item.startswith("ruff check")), None)
    assert ruff_item == ci_steps["Ruff"], "PR template ruff scope drifted from ci.yml"

    pytest_item = next((item for item in checklist if "pytest" in item), None)
    assert pytest_item is not None
    assert "-p pytest_cov" in pytest_item
    assert "--cov-fail-under=77" in pytest_item


def test_every_documented_coverage_gate_agrees_with_ci(ci_steps) -> None:
    """One number, one meaning. It read 50 in one file and 77 in two others."""
    ci_gate = re.search(r"--cov-fail-under=(\d+)", ci_steps["Pytest"]).group(1)
    offenders: list[str] = []

    for path in REPO_ROOT.rglob("*.md"):
        if any(part in {".venv", "node_modules", ".git"} for part in path.parts):
            continue
        # The changelog records history, including gates that have since moved.
        if path.name == "CHANGELOG.md":
            continue
        for gate in re.findall(r"--cov-fail-under=(\d+)", path.read_text(encoding="utf-8")):
            if gate != ci_gate:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {gate} (ci.yml says {ci_gate})")

    assert not offenders, "coverage gate disagrees with ci.yml:\n  " + "\n  ".join(offenders)
