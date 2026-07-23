#!/usr/bin/env python3
"""Tests for strip_catalog.py and strip_catalog_history.py."""

from __future__ import annotations

import unittest

from strip_catalog import (
    _obf_name,
    load_allowlist,
    sanitize_configs,
    sanitize_spec,
    strip_derivation,
    strip_schema_annotations,
)
from strip_catalog_history import sanitize_row

SALT = "test"
ALLOW = load_allowlist()


class TestConfig(unittest.TestCase):
    def test_allowlist_keeps_only_agnostic_recursively(self):
        node = {"endpoint": {"connector": {"image": "img", "config": {
            "address": "db.acme.internal", "password": "p", "hardDelete": True,
            "advanced": {"sslmode": "verify-full", "skip_backfills": "public.x"},
        }}}, "bindings": [{"resource": {"table": "t", "schema": "s", "syncMode": "incremental"},
                          "target": "acme/c"}]}
        sanitize_configs(node, ALLOW)
        self.assertEqual(node["endpoint"]["connector"]["config"],
                         {"hardDelete": True, "advanced": {"sslmode": "verify-full"}})
        self.assertEqual(node["bindings"][0]["resource"], {"syncMode": "incremental"})

    def test_local_command_removed(self):
        node = {"endpoint": {"local": {"command": ["/opt/acme/run", "--tenant=acme"], "config": {"x": 1}}}}
        sanitize_configs(node, ALLOW)
        self.assertNotIn("command", node["endpoint"]["local"])


class TestSchemaAnnotations(unittest.TestCase):
    def test_annotations_removed_but_field_names_kept(self):
        schema = {"type": "object", "title": "Patients",
                  "properties": {
                      "default": {"type": "string", "description": "a field literally named default",
                                  "examples": ["123-45-6789"]},
                      "tier": {"type": "string", "enum": ["acme-gold", "acme-platinum"], "const": "acme-gold"},
                  }}
        strip_schema_annotations(schema)
        self.assertNotIn("title", schema)
        # A property NAMED "default" must survive (not mistaken for the annotation).
        self.assertIn("default", schema["properties"])
        self.assertNotIn("description", schema["properties"]["default"])
        self.assertNotIn("examples", schema["properties"]["default"])
        self.assertNotIn("enum", schema["properties"]["tier"])
        self.assertNotIn("const", schema["properties"]["tier"])
        self.assertIn("tier", schema["properties"])  # field name kept


class TestDerivation(unittest.TestCase):
    def test_code_stripped_and_transform_name_obfuscated(self):
        derive = {"using": {"sqlite": {"migrations": ["CREATE TABLE x (id INT);"]},
                            "typescript": {"module": "export default ..."}},
                  "transforms": [{"name": "flagVips", "source": "acme/c",
                                  "lambda": "SELECT $ssn WHERE balance > 1e6", "shuffle": {"lambda": "x"}}]}
        strip_derivation(derive, SALT)
        t = derive["transforms"][0]
        self.assertNotIn("lambda", t)
        self.assertNotIn("lambda", t["shuffle"])
        self.assertNotIn("migrations", derive["using"]["sqlite"])
        self.assertNotIn("module", derive["using"]["typescript"])
        self.assertNotEqual(t["name"], "flagVips")
        self.assertEqual(t["source"], "acme/c")  # references renamed elsewhere, not here


class TestNames(unittest.TestCase):
    def test_obf_name_segmented_and_prefix_consistent(self):
        full = _obf_name("AcmeCorp/prod/patients", SALT)
        self.assertEqual(full.count("/"), 2)
        self.assertNotEqual(full, "AcmeCorp/prod/patients")
        # A prefix obfuscates to the prefix of the full name's obfuscation.
        self.assertEqual(_obf_name("AcmeCorp/prod", SALT), "/".join(full.split("/")[:2]))

    def test_sanitize_spec_renames_references_consistently(self):
        spec = {"bindings": [{"resource": {"table": "t"}, "target": "AcmeCorp/prod/patients"}]}
        sanitize_spec(spec, ALLOW, SALT)
        self.assertEqual(spec["bindings"][0]["target"], _obf_name("AcmeCorp/prod/patients", SALT))
        self.assertEqual(spec["bindings"][0]["resource"], {})


class TestHistory(unittest.TestCase):
    def test_row_sanitized(self):
        row = {"catalog_name": "AcmeCorp/prod/src", "catalog_type": "capture",
               "publication": {"publicationId": "0011", "publishedAt": "2024-01-02T03:04:05Z",
                               "userEmail": "alice@acmecorp.com", "userFullName": "Alice", "userId": "uuid-1",
                               "detail": "acme fix",
                               "model": {"endpoint": {"connector": {"image": "img", "config": {
                                   "password": "p", "hardDelete": True}}},
                                   "bindings": [{"resource": {"table": "t"}, "target": "AcmeCorp/prod/patients"}]}}}
        out = sanitize_row(row, ALLOW, SALT)
        pub = out["publication"]
        self.assertEqual(out["catalog_name"], _obf_name("AcmeCorp/prod/src", SALT))
        self.assertEqual(out["catalog_type"], "capture")           # kept
        self.assertEqual(pub["publicationId"], "0011")             # kept (timeline)
        self.assertEqual(pub["publishedAt"], "2024-01-02T03:04:05Z")
        self.assertNotEqual(pub["userEmail"], "alice@acmecorp.com")  # PII obfuscated
        self.assertNotEqual(pub["userFullName"], "Alice")
        self.assertNotEqual(pub["detail"], "acme fix")
        self.assertEqual(pub["model"]["endpoint"]["connector"]["config"], {"hardDelete": True})
        self.assertEqual(pub["model"]["bindings"][0]["target"], _obf_name("AcmeCorp/prod/patients", SALT))


if __name__ == "__main__":
    unittest.main()
