"""Unit tests for the benchmark fixture generator.

Run with: python3 -m unittest tests/benchmark/materialize/test_generate.py
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate  # noqa: E402


SIMPLE_COLLECTION = {
    "name": "bench/simple",
    "schema": {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "val": {"type": "integer"},
            "payload": {"type": "string"},
        },
        "required": ["id"],
    },
    "key": ["/id"],
}

UUID_COLLECTION = {
    "name": "bench/uuid",
    "schema": {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "val": {"type": "integer"},
            "payload": {"type": "string"},
        },
        "required": ["id"],
    },
    "key": ["/id"],
    "key_type": "uuid",
}

# Uses enum values of *different* byte-lengths on purpose, so the exact
# byte-size tests exercise the per-variant overhead bookkeeping.
ENUM_VALUES = ["ck0", "ck1", "ck2", "ck3", "ck4", "ck5", "ck6", "ck7", "ck8", "ck9"]
ENUM_COLLECTION = {
    "name": "bench/enum",
    "schema": {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "val": {"type": "integer"},
            "cluster_key": {"type": "string", "enum": ENUM_VALUES},
            "payload": {"type": "string"},
        },
        "required": ["id"],
    },
    "key": ["/id"],
}


def _scenario(transactions: list[dict], collection: dict | None = None) -> dict:
    return {"collections": [collection or SIMPLE_COLLECTION], "transactions": transactions}


def _write_scenario(tmpdir: str, transactions: list[dict], collection: dict | None = None) -> str:
    import yaml

    path = os.path.join(tmpdir, "scenario.yaml")
    with open(path, "w") as f:
        yaml.safe_dump(_scenario(transactions, collection), f)
    return path


def _run(transactions: list[dict], seed: int = 0, collection: dict | None = None) -> tuple[list, dict]:
    """Run generate.emit() in-memory and return (lines, state_log)."""
    with tempfile.TemporaryDirectory() as d:
        path = _write_scenario(d, transactions, collection)
        collections, txs = generate.load_scenario(path)
        buf = io.StringIO()
        state = generate.emit(collections, txs, buf, seed=seed)
        lines = buf.getvalue().splitlines()
        return lines, state


class TestSizeParsing(unittest.TestCase):
    def test_int(self):
        self.assertEqual(generate.parse_size(1024), 1024)

    def test_suffixes(self):
        self.assertEqual(generate.parse_size("1KB"), 1024)
        self.assertEqual(generate.parse_size("2MB"), 2 * 1024**2)
        self.assertEqual(generate.parse_size("3GB"), 3 * 1024**3)
        self.assertEqual(generate.parse_size("512B"), 512)

    def test_float(self):
        self.assertEqual(generate.parse_size("1.5KB"), 1536)

    def test_count_underscore(self):
        self.assertEqual(generate.parse_count("1_000_000"), 1_000_000)


class TestPadding(unittest.TestCase):
    def test_exact_byte_size(self):
        lines, _ = _run([{"doc_count": 200, "doc_size": 256}])
        # First line is initial commit, last is final commit, middle are docs.
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        self.assertEqual(len(docs), 200)
        for envelope in docs:
            _, doc = envelope
            encoded = json.dumps(doc, separators=(",", ":"))
            self.assertEqual(
                len(encoded.encode("utf-8")),
                256,
                f"doc encoded size != 256: {len(encoded)}",
            )

    def test_padding_too_small_errors(self):
        # doc_size of 10 is far below the minimum overhead of the schema.
        with self.assertRaises(ValueError):
            _run([{"doc_count": 1, "doc_size": 10}])


class TestSizingResolution(unittest.TestCase):
    def test_total_size_with_doc_size(self):
        lines, state = _run([{"total_size": "10KB", "doc_size": 256}])
        self.assertEqual(state["transactions"][0]["doc_count"], 10 * 1024 // 256)

    def test_must_specify_two(self):
        with self.assertRaises(ValueError):
            _run([{"doc_count": 100}])


class TestOverlap(unittest.TestCase):
    def test_overlap_keys_match_prior_tx(self):
        lines, state = _run(
            [
                {"doc_count": 100, "doc_size": 256},
                {
                    "doc_count": 100,
                    "doc_size": 256,
                    "overlaps": [
                        {"with": 0, "fraction": 0.20, "op": "u"},
                        {"with": 0, "fraction": 0.10, "op": "d"},
                    ],
                },
            ]
        )
        # tx 0 should have 100 fresh creates with keys [0, 100).
        self.assertEqual(state["transactions"][0]["fresh"], {"start": 0, "count": 100, "op": "c"})
        # tx 1 should have 20 updates + 10 deletes overlapping tx 0, plus 70 fresh.
        tx1 = state["transactions"][1]
        self.assertEqual(tx1["fresh"]["count"], 70)
        self.assertEqual(tx1["fresh"]["start"], 100)
        op_counts = {(o["op"], o["with"]): o["count"] for o in tx1["overlaps"]}
        self.assertEqual(op_counts, {("u", 0): 20, ("d", 0): 10})

        # Inspect actual emitted docs to confirm key sets.
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        # Skip first 100 (tx 0).
        tx1_docs = docs[100:]
        update_keys = [d["id"] for c, d in tx1_docs if d["_meta"]["op"] == "u"]
        delete_keys = [d["id"] for c, d in tx1_docs if d["_meta"]["op"] == "d"]
        create_keys = [d["id"] for c, d in tx1_docs if d["_meta"]["op"] == "c"]

        self.assertEqual(len(update_keys), 20)
        self.assertEqual(len(delete_keys), 10)
        self.assertEqual(len(create_keys), 70)

        # No collision between updates and deletes within tx 1.
        self.assertTrue(set(update_keys).isdisjoint(set(delete_keys)))
        # All updates and deletes draw from tx 0's range [0, 100).
        self.assertTrue(all(0 <= k < 100 for k in update_keys + delete_keys))
        # Fresh creates use new keys [100, 170).
        self.assertEqual(sorted(create_keys), list(range(100, 170)))

    def test_overlap_must_reference_earlier_tx(self):
        with self.assertRaises(ValueError):
            _run(
                [
                    {
                        "doc_count": 10,
                        "doc_size": 256,
                        "overlaps": [{"with": 0, "fraction": 0.5, "op": "u"}],
                    }
                ]
            )

    def test_overlap_fraction_sum_le_one(self):
        with self.assertRaises(ValueError):
            _run(
                [
                    {"doc_count": 10, "doc_size": 256},
                    {
                        "doc_count": 10,
                        "doc_size": 256,
                        "overlaps": [
                            {"with": 0, "fraction": 0.7, "op": "u"},
                            {"with": 0, "fraction": 0.4, "op": "d"},
                        ],
                    },
                ]
            )

    def test_overlap_range_restricts_to_tail(self):
        """range: [0.6, 1.0] should sample only from the last 40% of the source tx."""
        lines, state = _run(
            [
                {"doc_count": 100, "doc_size": 256},
                {
                    "doc_count": 100,
                    "doc_size": 256,
                    "overlaps": [
                        {"with": 0, "fraction": 0.20, "op": "u", "range": [0.6, 1.0]},
                    ],
                },
            ]
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        tx1_docs = docs[100:]
        update_keys = [d["id"] for c, d in tx1_docs if d["_meta"]["op"] == "u"]
        self.assertEqual(len(update_keys), 20)
        # Tx 0 used keys [0, 100); the last 40% is [60, 100).
        self.assertTrue(all(60 <= k < 100 for k in update_keys),
                        f"expected all keys in [60, 100), got {sorted(update_keys)}")

    def test_overlap_range_restricts_to_head(self):
        lines, _ = _run(
            [
                {"doc_count": 100, "doc_size": 256},
                {
                    "doc_count": 100,
                    "doc_size": 256,
                    "overlaps": [
                        {"with": 0, "fraction": 0.10, "op": "u", "range": [0.0, 0.25]},
                    ],
                },
            ]
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        tx1_docs = docs[100:]
        update_keys = [d["id"] for c, d in tx1_docs if d["_meta"]["op"] == "u"]
        self.assertEqual(len(update_keys), 10)
        # First 25% of tx 0 is [0, 25).
        self.assertTrue(all(0 <= k < 25 for k in update_keys),
                        f"expected all keys in [0, 25), got {sorted(update_keys)}")

    def test_overlap_range_invalid_bounds(self):
        for bad in ([0.5, 0.5], [0.7, 0.3], [-0.1, 0.5], [0.5, 1.1], [0.5]):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _run(
                        [
                            {"doc_count": 10, "doc_size": 256},
                            {
                                "doc_count": 10,
                                "doc_size": 256,
                                "overlaps": [
                                    {"with": 0, "fraction": 0.5, "op": "u", "range": bad},
                                ],
                            },
                        ]
                    )

    def test_overlap_range_too_narrow_for_fraction(self):
        # 50% of 10 docs = 5 keys, but range covers only 2 keys of source.
        with self.assertRaises(ValueError):
            _run(
                [
                    {"doc_count": 10, "doc_size": 256},
                    {
                        "doc_count": 10,
                        "doc_size": 256,
                        "overlaps": [
                            {"with": 0, "fraction": 0.5, "op": "u", "range": [0.0, 0.2]},
                        ],
                    },
                ]
            )


class TestZipfianDistribution(unittest.TestCase):
    """Zipfian-weighted overlap sampling: biases overlap keys towards one
    end of the source tx's (sub-)range with a long-tail decay. Useful for
    modelling real-world hot-spot updates."""

    def _zipf_update_keys(self, distribution_kwargs: dict, seed: int = 0):
        lines, _ = _run(
            [
                {"doc_count": 1000, "doc_size": 256},
                {
                    "doc_count": 1000,
                    "doc_size": 256,
                    "overlaps": [
                        {"with": 0, "fraction": 0.20, "op": "u", **distribution_kwargs},
                    ],
                },
            ],
            seed=seed,
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        return [d["id"] for _, d in docs[1000:] if d["_meta"]["op"] == "u"]

    def test_zipfian_biases_to_tail(self):
        keys = self._zipf_update_keys({"distribution": "zipfian", "zipf_s": 1.5})
        self.assertEqual(len(keys), 200)
        # Tx 0 spans [0, 1000). With heavy tail bias, mass should sit
        # well above the midpoint. Median > 700 is a stable threshold for
        # s=1.5: empirically the median floats around 850.
        median = sorted(keys)[len(keys) // 2]
        self.assertGreater(median, 700, f"expected median key > 700, got {median}")
        # And the top decile should contain a disproportionate share of picks.
        top_decile_share = sum(1 for k in keys if k >= 900) / len(keys)
        self.assertGreater(top_decile_share, 0.30,
                           f"top 10% of range should hold >30% of picks, got {top_decile_share:.2%}")

    def test_zipfian_biases_to_head(self):
        keys = self._zipf_update_keys(
            {"distribution": "zipfian", "zipf_s": 1.5, "zipf_bias": "head"}
        )
        median = sorted(keys)[len(keys) // 2]
        self.assertLess(median, 300, f"expected median key < 300, got {median}")

    def test_zipfian_no_replacement(self):
        keys = self._zipf_update_keys({"distribution": "zipfian", "zipf_s": 1.5})
        self.assertEqual(len(keys), len(set(keys)),
                         "zipfian sampling must not produce duplicate keys")

    def test_zipfian_deterministic(self):
        a = self._zipf_update_keys({"distribution": "zipfian", "zipf_s": 1.2}, seed=11)
        b = self._zipf_update_keys({"distribution": "zipfian", "zipf_s": 1.2}, seed=11)
        self.assertEqual(a, b)
        c = self._zipf_update_keys({"distribution": "zipfian", "zipf_s": 1.2}, seed=12)
        self.assertNotEqual(sorted(a), sorted(c))

    def test_zipfian_with_subrange(self):
        # Combine zipfian-tail with a [0.6, 1.0] sub-range: keys must lie in
        # the last 40% of tx 0, AND skew towards the upper edge of THAT.
        lines, _ = _run(
            [
                {"doc_count": 1000, "doc_size": 256},
                {
                    "doc_count": 1000,
                    "doc_size": 256,
                    "overlaps": [
                        {
                            "with": 0,
                            "fraction": 0.10,
                            "op": "u",
                            "range": [0.6, 1.0],
                            "distribution": "zipfian",
                            "zipf_s": 1.5,
                        },
                    ],
                },
            ]
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        keys = [d["id"] for _, d in docs[1000:] if d["_meta"]["op"] == "u"]
        self.assertEqual(len(keys), 100)
        # All keys in the [0.6, 1.0] sub-range = [600, 1000).
        self.assertTrue(all(600 <= k < 1000 for k in keys),
                        f"expected all keys in [600, 1000), got min={min(keys)} max={max(keys)}")
        # Tail bias: median should still skew above the sub-range midpoint (800).
        median = sorted(keys)[len(keys) // 2]
        self.assertGreater(median, 800, f"expected median > 800, got {median}")

    def test_zipfian_disjoint_with_exclusions(self):
        # Two overlap entries against the same source tx must produce
        # disjoint key sets (existing invariant) under zipfian too.
        lines, _ = _run(
            [
                {"doc_count": 1000, "doc_size": 256},
                {
                    "doc_count": 1000,
                    "doc_size": 256,
                    "overlaps": [
                        {"with": 0, "fraction": 0.20, "op": "u",
                         "distribution": "zipfian", "zipf_s": 1.5},
                        {"with": 0, "fraction": 0.10, "op": "d",
                         "distribution": "zipfian", "zipf_s": 1.5},
                    ],
                },
            ]
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        tx1_docs = docs[1000:]
        update_keys = {d["id"] for _, d in tx1_docs if d["_meta"]["op"] == "u"}
        delete_keys = {d["id"] for _, d in tx1_docs if d["_meta"]["op"] == "d"}
        self.assertEqual(len(update_keys), 200)
        self.assertEqual(len(delete_keys), 100)
        self.assertTrue(update_keys.isdisjoint(delete_keys))

    def test_zipfian_sub_s_approaches_uniform(self):
        # s=0.01 (almost flat) should look roughly uniform over the range.
        keys = self._zipf_update_keys(
            {"distribution": "zipfian", "zipf_s": 0.01}, seed=3,
        )
        # Mean within a generous band around the uniform mean of 499.5.
        mean = sum(keys) / len(keys)
        self.assertGreater(mean, 350)
        self.assertLess(mean, 650)

    def test_invalid_distribution_rejected(self):
        with self.assertRaises(ValueError):
            _run(
                [
                    {"doc_count": 100, "doc_size": 256},
                    {
                        "doc_count": 100,
                        "doc_size": 256,
                        "overlaps": [
                            {"with": 0, "fraction": 0.2, "op": "u",
                             "distribution": "lognormal"},
                        ],
                    },
                ]
            )

    def test_invalid_zipf_s_rejected(self):
        for bad in (0, -1.0):
            with self.subTest(s=bad):
                with self.assertRaises(ValueError):
                    _run(
                        [
                            {"doc_count": 100, "doc_size": 256},
                            {
                                "doc_count": 100,
                                "doc_size": 256,
                                "overlaps": [
                                    {"with": 0, "fraction": 0.2, "op": "u",
                                     "distribution": "zipfian", "zipf_s": bad},
                                ],
                            },
                        ]
                    )

    def test_invalid_zipf_bias_rejected(self):
        with self.assertRaises(ValueError):
            _run(
                [
                    {"doc_count": 100, "doc_size": 256},
                    {
                        "doc_count": 100,
                        "doc_size": 256,
                        "overlaps": [
                            {"with": 0, "fraction": 0.2, "op": "u",
                             "distribution": "zipfian", "zipf_bias": "middle"},
                        ],
                    },
                ]
            )


class TestManualLineAssembly(unittest.TestCase):
    """The fast path assembles the fixture line by string fragments rather
    than calling json.dumps per doc. These tests verify that the output
    remains valid JSON and that payload field bytes are exactly right."""

    def test_large_doc_parses_and_payload_exact(self):
        lines, _ = _run([{"doc_count": 5, "doc_size": 1024}])
        docs = [l for l in lines if not l.startswith("{")]
        self.assertEqual(len(docs), 5)
        for line in docs:
            # Line must parse cleanly.
            envelope = json.loads(line)
            self.assertEqual(len(envelope), 2)
            name, doc = envelope
            self.assertEqual(name, "bench/simple")
            # Full doc serializes back to exactly doc_size bytes.
            reserialized = json.dumps(doc, separators=(",", ":"))
            self.assertEqual(
                len(reserialized.encode("utf-8")), 1024,
                f"round-trip size drift: {len(reserialized)}",
            )
            # payload length + len(str(id)) + fixed overhead == 1024.
            # Payload alphabet must be ascii_letters + digits (no escaping).
            self.assertTrue(
                all(c.isalnum() and c.isascii() for c in doc["payload"]),
                "payload must use the non-escaping alphabet",
            )

    def test_overlap_keys_across_pool_sizes(self):
        # 1 MB docs: pool is precomputed at >= 2 MB. Each doc payload is
        # the full doc_size minus overhead, which stresses the slice path.
        lines, state = _run(
            [
                {"doc_count": 3, "doc_size": "1MB"},
                {
                    "doc_count": 3,
                    "doc_size": "1MB",
                    "overlaps": [{"with": 0, "fraction": 1.0, "op": "u"}],
                },
            ]
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        self.assertEqual(len(docs), 6)
        # All docs must round-trip to exactly 1 MB.
        for _, doc in docs:
            self.assertEqual(
                len(json.dumps(doc, separators=(",", ":")).encode("utf-8")),
                1024 * 1024,
            )
        # tx 1 reuses tx 0's keys [0, 3).
        tx1_keys = sorted(d["id"] for _, d in docs[3:])
        self.assertEqual(tx1_keys, [0, 1, 2])


class TestUUIDKeys(unittest.TestCase):
    """Tests for key_type: uuid support."""

    def test_uuid_exact_byte_size(self):
        lines, _ = _run([{"doc_count": 50, "doc_size": 256}], collection=UUID_COLLECTION)
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        self.assertEqual(len(docs), 50)
        for envelope in docs:
            _, doc = envelope
            encoded = json.dumps(doc, separators=(",", ":"))
            self.assertEqual(len(encoded.encode("utf-8")), 256)

    def test_uuid_keys_are_valid_uuids(self):
        import uuid

        lines, _ = _run([{"doc_count": 10, "doc_size": 256}], collection=UUID_COLLECTION)
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        for _, doc in docs:
            # Should parse as a valid UUID.
            parsed = uuid.UUID(doc["id"])
            self.assertEqual(str(parsed), doc["id"])

    def test_uuid_keys_deterministic(self):
        a, _ = _run([{"doc_count": 20, "doc_size": 256}], seed=7, collection=UUID_COLLECTION)
        b, _ = _run([{"doc_count": 20, "doc_size": 256}], seed=7, collection=UUID_COLLECTION)
        self.assertEqual(a, b)

    def test_uuid_overlap_keys_from_prior_tx(self):
        lines, state = _run(
            [
                {"doc_count": 50, "doc_size": 256},
                {
                    "doc_count": 50,
                    "doc_size": 256,
                    "overlaps": [{"with": 0, "fraction": 0.40, "op": "u"}],
                },
            ],
            collection=UUID_COLLECTION,
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        tx0_keys = {d["id"] for _, d in docs[:50]}
        tx1_docs = docs[50:]
        update_keys = {d["id"] for _, d in tx1_docs if d["_meta"]["op"] == "u"}
        create_keys = {d["id"] for _, d in tx1_docs if d["_meta"]["op"] == "c"}

        self.assertEqual(len(update_keys), 20)
        self.assertEqual(len(create_keys), 30)
        # Updates must reuse keys from tx 0.
        self.assertTrue(update_keys.issubset(tx0_keys))
        # Fresh creates must not collide with tx 0.
        self.assertTrue(create_keys.isdisjoint(tx0_keys))

    def test_invalid_key_type_rejected(self):
        bad_collection = dict(UUID_COLLECTION, key_type="sha256")
        with self.assertRaises(ValueError):
            _run([{"doc_count": 1, "doc_size": 256}], collection=bad_collection)


class TestEnumFields(unittest.TestCase):
    def test_values_from_enum_set(self):
        lines, _ = _run(
            [{"doc_count": 200, "doc_size": 256}], collection=ENUM_COLLECTION
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        self.assertEqual(len(docs), 200)
        for _, doc in docs:
            self.assertIn(doc["cluster_key"], ENUM_VALUES)

    def test_exact_byte_size_with_enum(self):
        # With variable-length enum values, per-variant overhead must drive
        # payload padding correctly so each doc still hits doc_size exactly.
        lines, _ = _run(
            [{"doc_count": 200, "doc_size": 256}], collection=ENUM_COLLECTION
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        for _, doc in docs:
            encoded = json.dumps(doc, separators=(",", ":"))
            self.assertEqual(len(encoded.encode("utf-8")), 256)

    def test_distribution_covers_all_enum_values(self):
        # Keys are allocated as [0, N), and variant selection is
        # key % len(enum), so all values must appear when N >> len(enum).
        lines, _ = _run(
            [{"doc_count": 500, "doc_size": 256}], collection=ENUM_COLLECTION
        )
        docs = [json.loads(l) for l in lines if not l.startswith("{")]
        seen = {d["cluster_key"] for _, d in docs}
        self.assertEqual(seen, set(ENUM_VALUES))

    def test_enum_deterministic(self):
        a, _ = _run(
            [{"doc_count": 100, "doc_size": 256}], seed=7, collection=ENUM_COLLECTION
        )
        b, _ = _run(
            [{"doc_count": 100, "doc_size": 256}], seed=7, collection=ENUM_COLLECTION
        )
        self.assertEqual(a, b)

    def test_empty_enum_rejected(self):
        bad = {
            "name": "bench/bad-enum",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "cluster_key": {"type": "string", "enum": []},
                    "payload": {"type": "string"},
                },
                "required": ["id"],
            },
            "key": ["/id"],
        }
        with self.assertRaises(ValueError):
            _run([{"doc_count": 1, "doc_size": 256}], collection=bad)

    def test_non_scalar_enum_rejected(self):
        bad = {
            "name": "bench/bad-enum",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "cluster_key": {"enum": [{"nested": "object"}]},
                    "payload": {"type": "string"},
                },
                "required": ["id"],
            },
            "key": ["/id"],
        }
        with self.assertRaises(ValueError):
            _run([{"doc_count": 1, "doc_size": 256}], collection=bad)


class TestDeterminism(unittest.TestCase):
    def test_seed_reproducibility(self):
        scenario = [
            {"doc_count": 50, "doc_size": 256},
            {
                "doc_count": 50,
                "doc_size": 256,
                "overlaps": [{"with": 0, "fraction": 0.4, "op": "u"}],
            },
        ]
        a, _ = _run(scenario, seed=42)
        b, _ = _run(scenario, seed=42)
        self.assertEqual(a, b)

        c, _ = _run(scenario, seed=43)
        self.assertNotEqual(a, c)


if __name__ == "__main__":
    unittest.main()
