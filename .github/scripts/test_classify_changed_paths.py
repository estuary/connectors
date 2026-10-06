#!/usr/bin/env python3
"""
Unit tests for classify-changed-paths.sh.

The classifier decides whether each connector CI workflow runs.
These tests pin both the routing rules and the "when in doubt, build" behaviour.
"""

import json
import subprocess
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "classify-changed-paths.sh"

# The python_connectors value for a change that affects every Python connector.
ALL = "all"


def classify(*paths: str, terminate: bool = True) -> tuple[bool, bool, str | list[str]]:
    """Run the classifier, returning (python, go_rust, python_connectors)."""
    stdin = "\n".join(paths) + ("\n" if terminate else "")
    proc = subprocess.run(
        [str(SCRIPT)], input=stdin, capture_output=True, text=True, check=True
    )
    verdict = dict(
        line.split("=", 1) for line in proc.stdout.strip().splitlines() if "=" in line
    )
    connectors = verdict["python_connectors"]
    return (
        verdict["python"] == "true",
        verdict["go_rust"] == "true",
        connectors if connectors == ALL else json.loads(connectors),
    )


class TestLanes(unittest.TestCase):
    def test_python_connector_is_python_only(self):
        self.assertEqual(
            classify("source-sentry/source_sentry/api.py"),
            (True, False, ["source-sentry"]),
        )

    def test_python_cdk_is_python_only(self):
        self.assertEqual(
            classify("estuary-cdk/estuary_cdk/http.py"), (True, False, ALL)
        )

    def test_go_connector_is_go_only(self):
        self.assertEqual(classify("source-postgres/main.go"), (False, True, []))

    def test_shared_go_package_is_go_only(self):
        self.assertEqual(classify("sqlcapture/backfill.go"), (False, True, []))

    def test_rust_connector_is_go_only(self):
        self.assertEqual(classify("source-kafka/src/main.rs"), (False, True, []))

    def test_top_level_file_is_go_only(self):
        self.assertEqual(classify("go.mod"), (False, True, []))

    def test_shared_plumbing_is_both(self):
        self.assertEqual(
            classify(".github/actions/setup/action.yaml"), (True, True, ALL)
        )
        self.assertEqual(classify("fetch-flow.sh"), (True, True, ALL))

    def test_docker_build_inputs_are_both(self):
        # Every image is built from the repo root, and every pushed image goes
        # through connector-variant.Dockerfile, so neither is Go-only.
        self.assertEqual(classify(".dockerignore"), (True, True, ALL))
        self.assertEqual(classify("connector-variant.Dockerfile"), (True, True, ALL))

    def test_docs_are_neither(self):
        self.assertEqual(classify("CLAUDE.md"), (False, False, []))
        self.assertEqual(classify("docs/contexts/captures.md"), (False, False, []))
        self.assertEqual(
            classify(".claude/skills/add-stream/SKILL.md"), (False, False, [])
        )

    def test_connector_changelog_is_neither(self):
        self.assertEqual(classify("source-postgres/CHANGELOG.md"), (False, False, []))

    def test_each_workflow_triggers_itself(self):
        self.assertEqual(classify(".github/workflows/python.yaml"), (True, False, ALL))
        self.assertEqual(classify(".github/workflows/ci.yaml"), (False, True, []))

    def test_python_connector_list_is_python_only(self):
        # python.yaml reads its matrix from this file, so editing the list
        # has to run the Python workflow as editing the matrix did.
        self.assertEqual(classify(".github/python-connectors.yaml"), (True, False, ALL))

    def test_estuary_cdk_workflow_triggers_neither_connector_lane(self):
        self.assertEqual(
            classify(".github/workflows/estuary-cdk.yaml"), (False, False, [])
        )

    def test_python_auto_rerun_workflow_triggers_neither_connector_lane(self):
        self.assertEqual(
            classify(".github/workflows/python-auto-rerun.yaml"), (False, False, [])
        )

    def test_ruff_config_triggers_neither_connector_lane(self):
        # Lint only reports, so the shared ruff config builds nothing. Without
        # this rule it would fall through to the Go & Rust lane like go.mod.
        self.assertEqual(classify("ruff.toml"), (False, False, []))

    def test_nested_pyproject_does_not_make_a_go_connector_python(self):
        # materialize-iceberg/python/pyproject.toml exists. Classing its parent as
        # Python would stop ci.yaml building a Go connector.
        self.assertEqual(
            classify("materialize-iceberg/python/foo.py"), (False, True, [])
        )

    def test_mixed_change_runs_both(self):
        self.assertEqual(
            classify("source-sentry/api.py", "sqlcapture/backfill.go"),
            (True, True, ["source-sentry"]),
        )

    def test_unknown_path_builds_rather_than_skipping(self):
        self.assertEqual(classify("some-new-thing/file.txt"), (False, True, []))

    def test_deleted_connector_is_still_python(self):
        # source-gone/ is not on disk, so its manifest appearing among the
        # changed paths is the only thing identifying it as a Python connector.
        self.assertEqual(
            classify("source-gone/api.py", "source-gone/pyproject.toml"),
            (True, False, ["source-gone"]),
        )

    def test_renamed_connector_is_python_on_both_sides(self):
        self.assertEqual(
            classify(
                "source-old/pyproject.toml",
                "source-old/api.py",
                "source-new/pyproject.toml",
                "source-new/api.py",
            ),
            (True, False, ["source-new", "source-old"]),
        )

    def test_deleted_nested_pyproject_still_does_not_count(self):
        # The one-level anchor has to hold for the changed-paths check too.
        self.assertEqual(
            classify("materialize-iceberg/python/pyproject.toml"), (False, True, [])
        )


class TestPythonConnectors(unittest.TestCase):
    def test_one_connector(self):
        self.assertEqual(
            classify(
                "source-sentry/source_sentry/api.py", "source-sentry/pyproject.toml"
            )[2],
            ["source-sentry"],
        )

    def test_two_connectors(self):
        self.assertEqual(
            classify("source-zuora/source_zuora/api.py", "source-sentry/api.py")[2],
            ["source-sentry", "source-zuora"],
        )

    def test_cdk_builds_all(self):
        self.assertEqual(classify("estuary-cdk/estuary_cdk/http.py")[2], ALL)

    def test_shared_dockerfile_builds_all(self):
        # Connectors symlink their Dockerfile into estuary-cdk/.
        self.assertEqual(classify("estuary-cdk/common.Dockerfile")[2], ALL)

    def test_python_workflow_builds_all(self):
        self.assertEqual(classify(".github/workflows/python.yaml")[2], ALL)

    def test_connector_list_builds_all(self):
        self.assertEqual(classify(".github/python-connectors.yaml")[2], ALL)

    def test_shared_actions_build_all(self):
        self.assertEqual(classify(".github/actions/deploy/action.yaml")[2], ALL)

    def test_dockerignore_builds_all(self):
        self.assertEqual(classify(".dockerignore")[2], ALL)

    def test_connector_plus_cdk_builds_all(self):
        self.assertEqual(
            classify("source-sentry/api.py", "estuary-cdk/estuary_cdk/http.py")[2], ALL
        )

    def test_go_only_change_builds_none(self):
        self.assertEqual(classify("source-postgres/main.go")[2], [])

    def test_connector_docs_build_none(self):
        self.assertEqual(classify("source-sentry/CHANGELOG.md")[2], [])


class TestRobustness(unittest.TestCase):
    def test_final_line_without_newline_is_still_read(self):
        # `while read` drops an unterminated final line, which would silently
        # omit that path from the verdict.
        self.assertEqual(
            classify("source-sentry/api.py", "sqlcapture/backfill.go", terminate=False),
            (True, True, ["source-sentry"]),
        )

    def test_blank_lines_are_ignored(self):
        self.assertEqual(
            classify("", "source-sentry/api.py", ""), (True, False, ["source-sentry"])
        )

    def test_change_touching_no_manifest_is_not_fatal(self):
        # The manifest scan greps for pyproject.toml and finds none here. Without
        # `|| true` that non-zero exit would kill the script under `set -e`,
        # breaking nearly every change.
        self.assertEqual(classify("sqlcapture/backfill.go"), (False, True, []))

    def test_empty_input_claims_nothing(self):
        # The caller treats an empty path list as a broken enumeration and runs
        # both lanes; the script itself just reports that it saw nothing.
        self.assertEqual(classify(), (False, False, []))


if __name__ == "__main__":
    unittest.main()
