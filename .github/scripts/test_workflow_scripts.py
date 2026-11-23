"""Check that no workflow or action script has a `${{ }}` expression in it.

The runner pastes an expression's value into the script before the shell
parses it, so a value with shell syntax in it runs as code. Scripts read
values from `env:` instead.
"""

import json
import subprocess
import unittest
from pathlib import Path

GITHUB = Path(__file__).resolve().parents[1]
FILES = sorted(
    [*GITHUB.glob("workflows/*.y*ml"), *GITHUB.glob("actions/*/action.y*ml")]
)


def scripts(path: Path) -> list[tuple[str, str]]:
    """Each `run` step of a workflow or composite action, as (label, script)."""
    # yq rather than PyYAML, which runners don't reliably have.
    out = subprocess.check_output(["yq", "-o=json", ".", str(path)], text=True)
    doc = json.loads(out)
    jobs = doc["jobs"] if "jobs" in doc else {"runs": doc["runs"]}
    return [
        (f"{job}[{index}]", step["run"])
        for job, body in jobs.items()
        for index, step in enumerate(body.get("steps", []))
        if "run" in step
    ]


class TestWorkflowScripts(unittest.TestCase):
    def test_scripts_are_found(self):
        self.assertTrue(FILES)
        self.assertTrue(any(scripts(path) for path in FILES))

    def test_scripts_have_no_expressions(self):
        for path in FILES:
            for label, script in scripts(path):
                with self.subTest(file=str(path.relative_to(GITHUB)), step=label):
                    self.assertNotIn("${{", script)


if __name__ == "__main__":
    unittest.main()
