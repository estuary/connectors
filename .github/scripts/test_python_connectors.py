"""Check .github/python-connectors.yaml against the repository.

python.yaml builds its matrix from that list, so a Python connector missing
from it is never built, and a malformed entry breaks or skews the matrix.
"""

import json
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_LIST = REPO_ROOT / ".github" / "python-connectors.yaml"

# Top-level directories with a pyproject.toml that aren't connectors.
NOT_CONNECTORS = {"estuary-cdk"}

REQUIRED_KEYS = {"name", "type", "version", "usage_rate"}
OPTIONAL_KEYS = {"variants"}


def load_entries() -> list[dict]:
    # yq rather than PyYAML, which runners don't reliably have.
    out = subprocess.check_output(["yq", "-o=json", ".", str(CONNECTOR_LIST)], text=True)
    return json.loads(out)


class TestPythonConnectorList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = load_entries()
        cls.names = [entry.get("name") for entry in cls.entries]

    def test_entries_have_the_expected_keys(self):
        for entry in self.entries:
            with self.subTest(entry=entry.get("name")):
                keys = set(entry)
                self.assertLessEqual(REQUIRED_KEYS, keys)
                self.assertLessEqual(keys, REQUIRED_KEYS | OPTIONAL_KEYS)

    def test_usage_rate_is_a_string(self):
        # GitHub Actions reads an unquoted 0.0 as unset.
        for entry in self.entries:
            with self.subTest(entry=entry.get("name")):
                self.assertIsInstance(entry["usage_rate"], str)

    def test_names_are_unique(self):
        duplicates = sorted({n for n in self.names if self.names.count(n) > 1})
        self.assertEqual(duplicates, [])

    def test_every_entry_is_a_python_project(self):
        for name in self.names:
            with self.subTest(entry=name):
                self.assertTrue((REPO_ROOT / name / "pyproject.toml").is_file())

    def test_every_python_project_is_listed(self):
        # A Python connector missing from the list would never be built.
        projects = {p.parent.name for p in REPO_ROOT.glob("*/pyproject.toml")}
        self.assertEqual(sorted(projects - NOT_CONNECTORS - set(self.names)), [])


if __name__ == "__main__":
    unittest.main()
