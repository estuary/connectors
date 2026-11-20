#!/usr/bin/env python3
"""
Unit tests for trivy-scan.sh, which scans connector images under trivy.yaml
with the root .trivyignore.yaml and the connector's own.

The script runs against a made-up repository whose `trivy` records the ignore
file and config each scan was given, so these pin which exceptions reach which
image without pulling anything. One test checks the repository's real
exception files.
"""

import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS.parents[1]
SCRIPT = SCRIPTS / "trivy-scan.sh"

# Records each scan's --config and --ignorefile under $CAPTURE, named after the
# image, and finds something in any image whose name says so.
FAKE_TRIVY = """\
#!/usr/bin/env bash
set -euo pipefail
while [ "$#" -gt 1 ]; do
    case "$1" in
        --config) config=$2; shift ;;
        --ignorefile) ignorefile=$2; shift ;;
    esac
    shift
done
name=${1//[^a-z0-9-]/_}
cp "$ignorefile" "$CAPTURE/${name}.ignore.yaml"
echo "$config" > "$CAPTURE/${name}.config"
case "$1" in *vulnerable*) exit 1 ;; esac
"""


def entry(cve: str, **overrides) -> dict:
    fields = {"id": cve, "expired_at": "2027-01-31", "statement": f"Why {cve} is fine."}
    fields.update(overrides)
    return {k: v for k, v in fields.items() if v is not None}


def ignorefile(*entries: dict) -> str:
    # JSON is YAML, and needs no YAML library to write.
    return json.dumps({"vulnerabilities": list(entries)})


class FakeRepo:
    def __init__(self, files: dict[str, str]):
        self.root = Path(tempfile.mkdtemp())
        self.capture = self.root / "capture"
        self.capture.mkdir()
        (self.root / ".github" / "scripts").mkdir(parents=True)
        shutil.copy(SCRIPT, self.root / ".github" / "scripts")
        shutil.copy(REPO_ROOT / "trivy.yaml", self.root)
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        (bin_dir / "trivy").write_text(FAKE_TRIVY)
        (bin_dir / "trivy").chmod(0o755)
        for path, content in files.items():
            (self.root / path).parent.mkdir(parents=True, exist_ok=True)
            (self.root / path).write_text(content)
        self.output = self.root / "github-output"

    def run(self, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        base = {k: v for k, v in os.environ.items() if not k.startswith("TRIVY_")}
        base.update(
            PATH=f"{self.root / 'bin'}:{os.environ['PATH']}",
            CAPTURE=str(self.capture),
            GITHUB_OUTPUT=str(self.output),
        )
        base.update(env or {})
        return subprocess.run(
            [str(self.root / ".github" / "scripts" / "trivy-scan.sh"), *args],
            capture_output=True, text=True, env=base,
        )

    def exceptions(self, image: str) -> list[str] | None:
        """The ids in the ignore file the image was scanned with, if it was."""
        path = self.capture / f"{image}.ignore.yaml"
        if not path.exists():
            return None
        merged = json.loads(subprocess.check_output(["yq", "-o=json", ".", str(path)], text=True))
        return sorted(e["id"] for e in merged["vulnerabilities"])

    def outputs(self) -> dict[str, str]:
        return dict(line.split("=", 1) for line in self.output.read_text().splitlines())

    def cleanup(self):
        shutil.rmtree(self.root)


class TestExceptions(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepo({
            ".trivyignore.yaml": ignorefile(entry("CVE-ROOT")),
            "source-a/.trivyignore.yaml": ignorefile(entry("CVE-A")),
            "source-b/.trivyignore.yaml": ignorefile(entry("CVE-B")),
            "source-c/VERSION": "v1",
        })
        self.addCleanup(self.repo.cleanup)

    def test_connector_gets_the_root_exceptions_and_its_own(self):
        proc = self.repo.run("source-a", "image-a")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.repo.exceptions("image-a"), ["CVE-A", "CVE-ROOT"])

    def test_no_other_connectors_exceptions_apply(self):
        proc = self.repo.run("source-a", "image-a", "source-b", "image-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.repo.exceptions("image-a"), ["CVE-A", "CVE-ROOT"])
        self.assertEqual(self.repo.exceptions("image-b"), ["CVE-B", "CVE-ROOT"])

    def test_connector_without_exceptions_gets_the_root_ones(self):
        proc = self.repo.run("source-c", "image-c")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.repo.exceptions("image-c"), ["CVE-ROOT"])

    def test_variant_gets_its_connectors_exceptions(self):
        proc = self.repo.run("source-a", "variant-of-a")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.repo.exceptions("variant-of-a"), ["CVE-A", "CVE-ROOT"])

    def test_scans_use_the_root_trivy_yaml(self):
        self.repo.run("source-c", "image-c")
        config = (self.repo.capture / "image-c.config").read_text().strip()
        self.assertEqual(config, str(self.repo.root / "trivy.yaml"))


class TestFailures(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepo({
            ".trivyignore.yaml": ignorefile(),
            "source-a/VERSION": "v1",
            "source-b/VERSION": "v1",
        })
        self.addCleanup(self.repo.cleanup)

    def test_finding_fails_after_every_image_is_scanned(self):
        proc = self.repo.run("source-a", "vulnerable-a", "source-b", "image-b")
        self.assertEqual(proc.returncode, 1)
        self.assertIsNotNone(self.repo.exceptions("image-b"))
        self.assertEqual(
            self.repo.outputs(), {"failed": "vulnerable-a", "failed_connectors": "source-a"}
        )

    def test_failed_connectors_are_listed_once(self):
        proc = self.repo.run("source-a", "vulnerable-a", "source-a", "vulnerable-variant")
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(
            self.repo.outputs(),
            {"failed": "vulnerable-a vulnerable-variant", "failed_connectors": "source-a"},
        )

    def test_clean_scan_writes_empty_outputs(self):
        proc = self.repo.run("source-a", "image-a")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.repo.outputs(), {"failed": "", "failed_connectors": ""})

    def test_unknown_connector_fails_without_scanning(self):
        for connector in ["source-z", "../source-a", ""]:
            with self.subTest(connector=connector):
                proc = self.repo.run(connector, "image-z")
                self.assertEqual(proc.returncode, 1)
                self.assertIsNone(self.repo.exceptions("image-z"))

    def test_usage(self):
        self.assertEqual(self.repo.run().returncode, 2)
        self.assertEqual(self.repo.run("source-a").returncode, 2)

    def test_trivy_settings_from_the_environment_are_refused(self):
        # They would take precedence over trivy.yaml.
        for name in ["TRIVY_SEVERITY", "TRIVY_IGNOREFILE", "TRIVY_EXIT_CODE"]:
            with self.subTest(name=name):
                proc = self.repo.run("source-a", "image-a", env={name: "x"})
                self.assertEqual(proc.returncode, 2)
                self.assertIn(name, proc.stderr)

    def test_image_source_and_cache_may_come_from_the_environment(self):
        proc = self.repo.run(
            "source-a", "image-a", env={"TRIVY_IMAGE_SRC": "remote", "TRIVY_CACHE_DIR": "/tmp"}
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)


class TestExceptionChecks(unittest.TestCase):
    """A malformed exception fails the scan of every image it would reach."""

    def scan(self, root: str, own: str | None = None) -> subprocess.CompletedProcess:
        files = {".trivyignore.yaml": root, "source-b/VERSION": "v1"}
        if own is not None:
            files["source-a/.trivyignore.yaml"] = own
        repo = FakeRepo(files)
        self.addCleanup(repo.cleanup)
        proc = repo.run("source-a", "image-a", "source-b", "image-b")
        proc.exceptions = {i: repo.exceptions(i) for i in ["image-a", "image-b"]}
        return proc

    def assert_refused(self, proc, message: str):
        self.assertEqual(proc.returncode, 1)
        self.assertIsNone(proc.exceptions["image-a"])
        self.assertIn(message, proc.stderr)

    def test_entry_needs_id_statement_and_expiry(self):
        for field in ["id", "statement", "expired_at"]:
            with self.subTest(field=field):
                proc = self.scan(ignorefile(), ignorefile(entry("CVE-A", **{field: None})))
                self.assert_refused(proc, f"has no {field}")
                # The other connector's scan doesn't read source-a's file.
                self.assertEqual(proc.exceptions["image-b"], [])

    def test_empty_values_count_as_missing(self):
        proc = self.scan(ignorefile(), ignorefile(entry("CVE-A", statement="")))
        self.assert_refused(proc, "has no statement")

    def test_expiry_must_be_a_date(self):
        proc = self.scan(ignorefile(), ignorefile(entry("CVE-A", expired_at="next year")))
        self.assert_refused(proc, "is not a YYYY-MM-DD date")

    def test_yaml_dates_are_accepted(self):
        own = textwrap.dedent("""\
            vulnerabilities:
              - id: CVE-A
                expired_at: 2027-01-31
                statement: >-
                  Why it is fine.
            """)
        proc = self.scan(ignorefile(), own)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.exceptions["image-a"], ["CVE-A"])

    def test_only_vulnerabilities_are_accepted(self):
        proc = self.scan(ignorefile(), json.dumps({"vulnerabilities": [], "secrets": []}))
        self.assert_refused(proc, "unexpected key secrets")

    def test_unparseable_file_is_refused(self):
        proc = self.scan(ignorefile(), "vulnerabilities: [")
        self.assert_refused(proc, "could not be read")

    def test_comments_alone_are_fine(self):
        proc = self.scan(ignorefile(), "# nothing yet\n")
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_malformed_root_file_fails_every_image(self):
        proc = self.scan(ignorefile(entry("CVE-ROOT", statement=None)))
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.exceptions, {"image-a": None, "image-b": None})


class TestRepositoryExceptions(unittest.TestCase):
    def test_every_exception_file_is_well_formed(self):
        # Each connector with exceptions, plus a directory without any, which
        # reads the root file alone.
        dirs = sorted(p.parent.name for p in REPO_ROOT.glob("*/.trivyignore.yaml"))
        args = [a for d in dirs + ["base-image"] for a in (d, f"image-{d}")]
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = Path(tmp) / "bin"
            bin_dir.mkdir()
            (bin_dir / "trivy").write_text(FAKE_TRIVY)
            (bin_dir / "trivy").chmod(0o755)
            env = {k: v for k, v in os.environ.items() if not k.startswith("TRIVY_")}
            env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", CAPTURE=tmp)
            env.pop("GITHUB_OUTPUT", None)
            proc = subprocess.run([str(SCRIPT), *args], capture_output=True, text=True, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
