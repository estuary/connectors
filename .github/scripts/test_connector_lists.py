"""Check the connector lists in .github/ against the repository.

python.yaml, go.yaml and rust.yaml each build their matrix from one of these
lists, so a connector missing from its list is never built, and a malformed
entry breaks or skews the matrix.
"""

import json
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_LIST = REPO_ROOT / ".github" / "python-connectors.yaml"
GO_LIST = REPO_ROOT / ".github" / "go-connectors.yaml"
RUST_LIST = REPO_ROOT / ".github" / "rust-connectors.yaml"

# Top-level directories with a pyproject.toml that aren't connectors.
NOT_CONNECTORS = {"estuary-cdk"}

# Top-level directories with a Dockerfile that aren't connectors.
NOT_DOCKER_CONNECTORS = {"base-image"}

REQUIRED_KEYS = {"name", "type", "version", "usage_rate"}
OPTIONAL_KEYS = {"variants"}


def load_entries(path: Path) -> list[dict]:
    # yq rather than PyYAML, which runners don't reliably have.
    out = subprocess.check_output(["yq", "-o=json", ".", str(path)], text=True)
    return json.loads(out)


def names(path: Path) -> list[str]:
    return [entry.get("name") for entry in load_entries(path)]


def duplicates(items: list[str]) -> list[str]:
    return sorted({n for n in items if items.count(n) > 1})


class TestPythonConnectorList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = load_entries(PYTHON_LIST)
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
        self.assertEqual(duplicates(self.names), [])

    def test_every_entry_is_a_python_project(self):
        for name in self.names:
            with self.subTest(entry=name):
                self.assertTrue((REPO_ROOT / name / "pyproject.toml").is_file())

    def test_every_python_project_is_listed(self):
        # A Python connector missing from the list would never be built.
        projects = {p.parent.name for p in REPO_ROOT.glob("*/pyproject.toml")}
        self.assertEqual(sorted(projects - NOT_CONNECTORS - set(self.names)), [])


class TestGoAndRustConnectorLists(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.go = load_entries(GO_LIST)
        cls.rust = load_entries(RUST_LIST)
        cls.go_names = [entry.get("name") for entry in cls.go]
        cls.rust_names = [entry.get("name") for entry in cls.rust]

    def test_entries_have_only_a_name(self):
        # Go and Rust connectors carry their version and variants in their
        # own VERSION and VARIANTS files.
        for entry in self.go + self.rust:
            with self.subTest(entry=entry.get("name")):
                self.assertEqual(set(entry), {"name"})

    def test_names_are_unique(self):
        self.assertEqual(duplicates(self.go_names + self.rust_names), [])

    def test_every_go_entry_has_go_code(self):
        for name in self.go_names:
            with self.subTest(entry=name):
                self.assertTrue(list((REPO_ROOT / name).glob("*.go")))

    def test_every_rust_entry_is_a_cargo_project(self):
        for name in self.rust_names:
            with self.subTest(entry=name):
                self.assertTrue((REPO_ROOT / name / "Cargo.toml").is_file())

    def test_every_go_and_rust_connector_is_listed(self):
        # A connector missing from both lists would never be built. Every
        # connector image is built from a top-level Dockerfile, and the Python
        # ones are the directories with a pyproject.toml.
        dockerfiles = {
            p.parent.name
            for p in REPO_ROOT.glob("*/Dockerfile")
            if not (p.parent / "pyproject.toml").exists()
        }
        listed = set(self.go_names) | set(self.rust_names)
        self.assertEqual(sorted(dockerfiles - NOT_DOCKER_CONNECTORS - listed), [])


class TestConnectorLists(unittest.TestCase):
    def test_no_connector_is_in_two_lists(self):
        everything = names(PYTHON_LIST) + names(GO_LIST) + names(RUST_LIST)
        self.assertEqual(duplicates(everything), [])


def copy_sources(dockerfile: Path) -> list[str]:
    """The paths a Dockerfile copies from the build context."""
    sources = []
    for line in dockerfile.read_text().splitlines():
        words = line.split()
        if not words or words[0] not in ("COPY", "ADD"):
            continue
        if any(w.startswith("--from") for w in words):
            continue
        sources += [w for w in words[1:-1] if not w.startswith("--")]
    return sources


class TestDockerfileInputs(unittest.TestCase):
    """Every input a connector's image copies must be one the classifier
    connects to that connector, or a change to it would build nothing."""

    def test_rust_images_copy_only_their_own_directory(self):
        # The classifier's Rust rules assume it.
        for name in names(RUST_LIST):
            for source in copy_sources(REPO_ROOT / name / "Dockerfile"):
                with self.subTest(connector=name, source=source):
                    self.assertEqual(source.split("/")[0], name)


if __name__ == "__main__":
    unittest.main()
