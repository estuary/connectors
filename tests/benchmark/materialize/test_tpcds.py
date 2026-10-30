"""Unit tests for the TPC-DS workload.

Run with: python3 -m unittest tests/benchmark/materialize/test_tpcds.py
"""

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest

import yaml

sys.path.insert(0, os.path.dirname(__file__))
import generate_spec  # noqa: E402
import tpcds  # noqa: E402

BINDINGS = [
    {
        "recommendedName": "store",
        "resourceConfig": {"table": "store"},
        "documentSchema": {"type": "object", "properties": {"s_store_sk": {"type": "integer"}}},
        "key": ["/s_store_sk"],
    },
    {
        "recommendedName": "item",
        "resourceConfig": {"table": "item"},
        "documentSchema": {"type": "object", "properties": {"i_item_sk": {"type": "integer"}}},
        "key": ["/i_item_sk"],
    },
]


class LoadTest(unittest.TestCase):
    def test_synthetic_scenario(self):
        self.assertIsNone(tpcds.load({"collections": [], "transactions": []}))

    def test_workload(self):
        w = tpcds.load({"tpcds": {"scale": 1, "transaction_size": "1KB",
                                  "resource": {"delta_updates": True}}})
        self.assertEqual(w.scale, 1.0)
        self.assertEqual(w.transaction_size, 1024)
        self.assertIsNone(w.tables)
        self.assertEqual(w.resource, {"delta_updates": True})

    def test_rejects(self):
        for raw in [
            {"tpcds": {"transaction_size": "1KB"}},
            {"tpcds": {"scale": 1}},
            {"tpcds": {"scale": 1, "transaction_size": 0}},
            {"tpcds": {"scale": 1, "transaction_size": "1KB"}, "collections": []},
        ]:
            with self.assertRaises(ValueError, msg=raw):
                tpcds.load(raw)


class SpecTest(unittest.TestCase):
    def test_capture_and_materialization_agree(self):
        with tempfile.TemporaryDirectory() as d:
            scenario = os.path.join(d, "s.yaml")
            config = os.path.join(d, "c.yaml")
            with open(scenario, "w") as f:
                yaml.safe_dump({"tpcds": {"scale": 0.01, "transaction_size": "1MB",
                                          "resource": {"delta_updates": True}}}, f)
            with open(config, "w") as f:
                yaml.safe_dump({"address": "x"}, f)
            workload = tpcds.load({"tpcds": {"scale": 0.01, "transaction_size": "1MB"}})
            capture = tpcds.capture_spec(workload, BINDINGS, "bench/cap")
            mat = generate_spec.build_spec(
                scenario, "materialize-x", config, task="bench/mat",
                table_suffix="_a", tpcds_bindings=BINDINGS)

        self.assertEqual(capture["collections"], mat["collections"])
        self.assertEqual(list(mat["collections"]), ["bench/tpcds/store", "bench/tpcds/item"])
        cap = capture["captures"]["bench/cap"]
        self.assertEqual(cap["endpoint"]["connector"]["config"], {"scale": 0.01})
        self.assertEqual([b["target"] for b in cap["bindings"]], list(mat["collections"]))
        self.assertEqual(
            mat["materializations"]["bench/mat"]["bindings"][0],
            {"source": "bench/tpcds/store",
             "resource": {"table": "store_a", "delta_updates": True}},
        )


def fake_flowctl(directory: str, lines: list, idle: bool) -> None:
    """A flowctl that prints the given capture preview lines, then idles as a
    finished capture does, or exits as a failed one does."""
    out = os.path.join(directory, "out.jsonl")
    with open(out, "w") as f:
        for line in lines:
            f.write(json.dumps(line, separators=(",", ":")) + "\n")
    path = os.path.join(directory, "flowctl")
    with open(path, "w") as f:
        f.write(f"#!/bin/sh\ncat {out}\n" + ("exec sleep 600\n" if idle else "exit 1\n"))
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


def doc(table: str, i: int) -> list:
    return [f"bench/tpcds/{table}", {"k": i, "pad": "x" * 10}]


def checkpoint(**states) -> list:
    return ["connectorState", {"updated": {"bindingStateV1": states}, "mergePatch": True}]


class RelayTest(unittest.TestCase):
    def setUp(self):
        real, tpcds.image_id = tpcds.image_id, lambda: "sha256:test"
        self.addCleanup(setattr, tpcds, "image_id", real)

    def run_relay(self, lines, transaction_size, idle=True):
        d = tempfile.mkdtemp()
        fake_flowctl(d, lines, idle)
        workload = tpcds.Workload(0.01, transaction_size, None, {})
        source = os.path.join(d, "capture.yaml")
        with open(source, "w") as f:
            yaml.safe_dump(tpcds.capture_spec(workload, BINDINGS, "bench/cap"), f)
        output = os.path.join(d, "fixture.jsonl")
        state = os.path.join(d, "state.json")
        old_path = os.environ["PATH"]
        os.environ["PATH"] = d + os.pathsep + old_path
        try:
            rc = tpcds.relay(workload, source, "bench/cap", output, state, os.devnull)
        finally:
            os.environ["PATH"] = old_path
        with open(output) as f:
            fixture = [json.loads(l) for l in f]
        with open(state) as f:
            return rc, fixture, json.load(f)

    def test_commits_by_size_and_stops_when_done(self):
        doc_bytes = len(json.dumps(doc("store", 0)[1], separators=(",", ":")))
        lines = [
            doc("store", 0), doc("item", 0), doc("store", 1),
            checkpoint(store={"done": True}),
            doc("item", 1), doc("item", 2),
            checkpoint(item={"done": True}),
            doc("item", 99),  # never reached: the relay stops at done
        ]
        rc, fixture, state = self.run_relay(lines, 2 * doc_bytes)

        self.assertEqual(rc, 0)
        commit = {"commit": True}
        self.assertEqual(fixture, [
            commit,
            doc("store", 0), doc("item", 0), commit,
            doc("store", 1), doc("item", 1), commit,
            doc("item", 2), commit,
        ])
        self.assertTrue(state["tpcds"]["complete"])
        self.assertEqual(state["tpcds"]["tables"], {
            "item": {"docs": 3, "bytes": 3 * doc_bytes},
            "store": {"docs": 2, "bytes": 2 * doc_bytes},
        })
        self.assertEqual(
            [(t["doc_count"], t["bytes"], t["tables"]) for t in state["transactions"]],
            [(2, 2 * doc_bytes, {"store": 1, "item": 1}),
             (2, 2 * doc_bytes, {"store": 1, "item": 1}),
             (1, doc_bytes, {"item": 1})],
        )

    def test_capture_exits_early(self):
        lines = [doc("store", 0), checkpoint(store={"done": True})]
        rc, fixture, state = self.run_relay(lines, 1 << 20, idle=False)

        self.assertEqual(rc, 1)
        self.assertEqual(fixture, [{"commit": True}, doc("store", 0), {"commit": True}])
        self.assertFalse(state["tpcds"]["complete"])
        self.assertEqual(state["tpcds"]["tables"]["store"]["docs"], 1)

    def test_cache_replays_the_generated_fixture(self):
        lines = [doc("store", 0), doc("item", 0),
                 checkpoint(store={"done": True}, item={"done": True})]
        d = tempfile.mkdtemp()
        fake_flowctl(d, lines, idle=True)
        workload = tpcds.Workload(0.01, 1 << 20, None, {})
        source = os.path.join(d, "capture.yaml")
        with open(source, "w") as f:
            yaml.safe_dump(tpcds.capture_spec(workload, BINDINGS, "bench/cap"), f)
        cache = os.path.join(d, "cache")
        old_path = os.environ["PATH"]
        os.environ["PATH"] = d + os.pathsep + old_path
        try:
            self.assertEqual(tpcds.prepare(workload, source, "bench/cap", cache, os.devnull), 0)
        finally:
            os.environ["PATH"] = old_path
        # A second prepare and the replay must not need the capture.
        os.remove(os.path.join(d, "flowctl"))
        self.assertEqual(tpcds.prepare(workload, source, "bench/cap", cache, os.devnull), 0)

        output, state = os.path.join(d, "fixture.jsonl"), os.path.join(d, "state.json")
        self.assertEqual(tpcds.replay(workload, cache, output, state), 0)
        with open(output) as f:
            self.assertEqual([json.loads(l) for l in f], [
                {"commit": True}, doc("store", 0), doc("item", 0), {"commit": True}])
        with open(state) as f:
            self.assertTrue(json.load(f)["tpcds"]["complete"])
        self.assertEqual(sorted(os.listdir(cache)), sorted(
            os.path.basename(p) for p in tpcds.cache_entry(workload, cache)))

    def test_cache_dir_must_be_private(self):
        d = tempfile.mkdtemp()
        os.chmod(d, 0o777)
        with self.assertRaises(SystemExit):
            tpcds.own_cache_dir(d)
        link = os.path.join(tempfile.mkdtemp(), "cache")
        os.symlink(tempfile.mkdtemp(), link)
        with self.assertRaises(SystemExit):
            tpcds.own_cache_dir(link)
        os.chmod(d, 0o755)
        tpcds.own_cache_dir(d)
        tpcds.own_cache_dir(os.path.join(d, "new"))


if __name__ == "__main__":
    unittest.main()
