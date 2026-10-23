#!/usr/bin/env python3
"""
Unit tests for go_connectors.py, which picks the Go connectors a change affects.

Most tests run against a small made-up import graph, so they pin the rules
rather than today's imports. One runs against the repository's real graph
when a Go toolchain that can read go.mod is available.
"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from go_connectors import select

SCRIPTS = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS.parents[1]
CLASSIFIER = SCRIPTS / "classify-changed-paths.sh"
GO_LIST_FORMAT = (
    '{{.ImportPath}}|{{join .Imports ","}}|{{join .TestImports ","}}|{{join .XTestImports ","}}'
)

# source-a and source-b share sqlcapture, which imports go/common. source-a's
# tests import a helper. materialize-x imports go/common alone. go/tool is
# imported by nothing.
GRAPH = {
    "sqlcapture": {"go/common"},
    "go/common": set(),
    "go/tool": set(),
    "go/testhelp": set(),
    "source-a": {"sqlcapture", "go/testhelp"},
    "source-b": {"sqlcapture"},
    "source-b/sub": set(),
    "materialize-x": {"go/common"},
    "tests/materialize/testctl": {"materialize-x"},
}
CONNECTORS = ["materialize-x", "source-a", "source-b", "source-mongodb", "materialize-snowflake"]
RUST = ["source-kafka", "materialize-kafka"]


def pick(*paths):
    return select(list(paths), GRAPH, CONNECTORS, RUST)


class TestRules(unittest.TestCase):
    def test_connector_builds_itself(self):
        self.assertEqual(pick("source-b/main.go"), ["source-b"])

    def test_non_go_file_in_a_connector_builds_it(self):
        self.assertEqual(pick("source-b/docker-compose.yaml"), ["source-b"])
        self.assertEqual(pick("source-b/VERSION"), ["source-b"])

    def test_shared_package_builds_its_importers(self):
        self.assertEqual(pick("sqlcapture/backfill.go"), ["source-a", "source-b"])

    def test_transitive_import_counts(self):
        self.assertEqual(
            pick("go/common/x.go"), ["materialize-x", "source-a", "source-b"]
        )

    def test_test_only_import_counts(self):
        self.assertEqual(pick("go/testhelp/x.go"), ["source-a"])

    def test_package_data_counts_as_the_package(self):
        # testdata and snapshots sit under their package's directory.
        self.assertEqual(
            pick("sqlcapture/testdata/x.json", "sqlcapture/.snapshots/TestX"),
            ["source-a", "source-b"],
        )

    def test_unimported_package_builds_nothing(self):
        self.assertEqual(pick("go/tool/main.go"), [])

    def test_shared_materialization_tests_build_go_materializations(self):
        self.assertEqual(pick("tests/materialize/run.sh"), ["materialize-snowflake", "materialize-x"])
        self.assertEqual(
            pick("tests/materialize/testctl/main.go"),
            ["materialize-snowflake", "materialize-x"],
        )

    def test_connector_test_fixtures_build_that_connector(self):
        self.assertEqual(pick("tests/source-a/capture.json"), ["source-a"])

    def test_other_test_tooling_builds_everything(self):
        self.assertEqual(pick("tests/smoke.sh"), "all")

    def test_non_go_inputs(self):
        self.assertEqual(pick("bson-transcoder/src/main.rs"), ["source-mongodb"])
        self.assertEqual(pick("estuary-cdk/estuary_cdk/http.py"), ["materialize-snowflake"])

    def test_rust_connector_builds_no_go_connector(self):
        self.assertEqual(pick("source-kafka/src/main.rs"), [])

    def test_other_ci_files_build_nothing(self):
        for path in [
            ".github/workflows/rust.yaml",
            ".github/workflows/delayed-tag.yaml",
            ".github/scripts/delayed_tag.py",
            ".github/rust-connectors.yaml",
        ]:
            with self.subTest(path=path):
                self.assertEqual(pick(path), [])

    def test_shared_inputs_build_everything(self):
        for path in [
            "go.mod",
            "go.sum",
            "base-image/Dockerfile",
            ".github/workflows/go.yaml",
            ".github/go-connectors.yaml",
            ".github/scripts/go_connectors.py",
            ".github/scripts/classify-changed-paths.sh",
        ]:
            with self.subTest(path=path):
                self.assertEqual(pick(path), "all")

    def test_unrecognised_path_builds_everything(self):
        # When in doubt, build.
        self.assertEqual(pick("build-local.sh"), "all")
        self.assertEqual(pick("scripts/notes.txt"), "all")

    def test_several_paths_union(self):
        self.assertEqual(
            pick("source-b/sub/x.go", "materialize-x/driver.go"),
            ["materialize-x", "source-b"],
        )


def go_list_graph(path: Path) -> bool:
    """Write the repository's import graph to path, as the action does."""
    if shutil.which("go") is None:
        return False
    env = dict(os.environ, GOFLAGS="-mod=mod", GOPROXY="off")
    with open(path, "w") as f:
        proc = subprocess.run(
            ["go", "list", "-e", "-f", GO_LIST_FORMAT, "./..."],
            cwd=REPO_ROOT, env=env, stdout=f, stderr=subprocess.DEVNULL,
        )
    return proc.returncode == 0 and path.stat().st_size > 0


def classify(*paths: str, graph: Path | None) -> dict[str, str]:
    env = dict(os.environ)
    env.pop("GO_IMPORT_GRAPH", None)
    if graph is not None:
        env["GO_IMPORT_GRAPH"] = str(graph)
    proc = subprocess.run(
        [str(CLASSIFIER)], input="\n".join(paths) + "\n",
        capture_output=True, text=True, check=True, env=env,
    )
    return dict(l.split("=", 1) for l in proc.stdout.splitlines() if "=" in l)


def go_connectors(verdict: dict[str, str]):
    value = verdict["go_connectors"]
    return value if value == "all" else json.loads(value)


class TestRealGraph(unittest.TestCase):
    """The classifier, end to end, with the repository's own import graph."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.graph = Path(cls.tmp.name) / "graph.txt"
        if not go_list_graph(cls.graph):
            raise unittest.SkipTest("no Go toolchain that can read go.mod")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_sqlcapture_builds_the_cdc_captures_only(self):
        got = go_connectors(classify("sqlcapture/backfill.go", graph=self.graph))
        for c in ["source-mysql", "source-postgres", "source-sqlserver"]:
            self.assertIn(c, got)
        self.assertNotIn("materialize-postgres", got)
        self.assertNotIn("source-s3", got)

    def test_materialize_sql_builds_sql_materializations_only(self):
        got = go_connectors(classify("materialize-sql/std_sql.go", graph=self.graph))
        self.assertIn("materialize-postgres", got)
        self.assertNotIn("source-postgres", got)

    def test_embedded_ca_bundles_build_the_mysql_captures(self):
        # go/mysql/tls embeds them from its cloudcas/ subdirectory.
        got = go_connectors(classify("go/mysql/tls/cloudcas/aws.pem", graph=self.graph))
        self.assertEqual(got, ["source-mysql", "source-mysql-batch"])

    def test_estuary_cdk_builds_materialize_snowflake(self):
        verdict = classify("estuary-cdk/estuary_cdk/http.py", graph=self.graph)
        self.assertEqual(go_connectors(verdict), ["materialize-snowflake"])

    def test_python_connector_builds_no_go_connector(self):
        verdict = classify("source-sentry/source_sentry/api.py", graph=self.graph)
        self.assertEqual(go_connectors(verdict), [])


class TestClassifierWiring(unittest.TestCase):
    def test_no_graph_builds_every_go_connector(self):
        self.assertEqual(go_connectors(classify("sqlcapture/x.go", graph=None)), "all")

    def test_unreadable_graph_builds_every_go_connector(self):
        missing = REPO_ROOT / "nonexistent-graph.txt"
        self.assertEqual(go_connectors(classify("sqlcapture/x.go", graph=missing)), "all")

    def test_docs_only_needs_no_graph(self):
        self.assertEqual(go_connectors(classify("docs/x.md", graph=None)), [])

    def test_shared_build_input_builds_every_go_connector(self):
        self.assertEqual(go_connectors(classify("fetch-flow.sh", graph=None)), "all")


if __name__ == "__main__":
    unittest.main()
