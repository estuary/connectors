#!/usr/bin/env python3
"""Tests for obfuscate_jsonl.py — run with `python3 -m unittest` in this dir."""

from __future__ import annotations

import json
import unittest
from datetime import datetime

from obfuscate_jsonl import _obfuscate_string, collect_known_fields, obfuscate

SALT = "test"


def obf_doc(doc: dict, known=()) -> dict:
    out = obfuscate(doc, (), set(known), SALT)
    assert isinstance(out, dict)
    return out


SAMPLES = {
    "arabic": "مرحبا بالعالم",
    "hebrew": "שלום עולם",
    "mandarin": "你好世界",
    "japanese_kana": "こんにちは カタカナ",
    "japanese_kanji": "日本語",
    "korean": "안녕하세요 세계",
    "thai": "สวัสดีชาวโลก",
    "devanagari": "नमस्ते दुनिया",
    "greek": "Γειά σου κόσμε",
    "cyrillic": "Привет мир",
    "latin_accented": "Café Zürich naïve",
    "emoji": "pizza 🍕 party 🎉👨‍👩‍👧 flag 🇯🇵",
    "symbols": "© € ™ ½ £",
    "mixed": "User №42: 山田さん <yamada@例え.jp> 💰",
}


def _target_class(ch: str) -> str:
    if ch.isdigit():
        return "digit"
    if ch.islower():
        return "lower"
    if ch.isupper():
        return "upper"
    return "cjk"


def _in_class(ch: str, cls: str) -> bool:
    if cls == "digit":
        return ch.isdigit() and ch.isascii()
    if cls == "lower":
        return "a" <= ch <= "z"
    if cls == "upper":
        return "A" <= ch <= "Z"
    return 0x4E00 <= ord(ch) <= 0x9FA5  # cjk


class TestObfuscateString(unittest.TestCase):
    def test_all_scripts_fully_covered(self):
        for name, s in SAMPLES.items():
            out = _obfuscate_string(s, SALT)
            self.assertEqual(len(out), len(s), f"{name}: length changed")
            for i, (src, dst) in enumerate(zip(s, out)):
                cls = _target_class(src)
                self.assertTrue(_in_class(dst, cls), f"{name}[{i}] {src!r}->{dst!r} (want {cls})")

    def test_no_caseless_letter_or_emoji_passes_through(self):
        for s in ("中", "あ", "한", "א", "م", "ก", "🍕", "€", "½"):
            out = _obfuscate_string(s, SALT)
            self.assertEqual(len(out), 1)
            self.assertTrue(0x4E00 <= ord(out) <= 0x9FA5, f"{s!r} -> {out!r} not obfuscated")

    def test_structure_is_obfuscated(self):
        s = "jane.doe+tag@example.co.uk / id-123 = {a:b}\ttab space"
        out = _obfuscate_string(s, SALT)
        self.assertEqual(len(out), len(s))
        for src, dst in zip(s, out):
            self.assertTrue(_in_class(dst, _target_class(src)))
        for structural in (" ", ".", "@", "/", "=", "+", "{", ":", "-", "\t"):
            self.assertNotIn(structural, out)

    def test_deterministic(self):
        for s in SAMPLES.values():
            self.assertEqual(_obfuscate_string(s, SALT), _obfuscate_string(s, SALT))

    def test_salt_changes_output(self):
        self.assertNotEqual(_obfuscate_string(SAMPLES["mixed"], "a"), _obfuscate_string(SAMPLES["mixed"], "b"))

    def test_valid_utf8_roundtrip(self):
        for s in SAMPLES.values():
            out = _obfuscate_string(s, SALT)
            self.assertEqual(json.loads(json.dumps(out)), out)


class TestDatetime(unittest.TestCase):
    def test_no_timezone_precision_or_era_leak(self):
        # Offset + fractional seconds present in input, none in output.
        out = _obfuscate_string("2020-06-01T09:00:00.123456+05:30", SALT)
        self.assertRegex(out, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        self.assertNotIn("+05:30", out)
        self.assertNotIn(".", out)
        datetime.fromisoformat(out.replace("Z", "+00:00"))  # valid

    def test_utc_z_form(self):
        for ts in ("2020-01-02T03:04:05Z", "1999-12-31T23:59:59-05:00"):
            out = _obfuscate_string(ts, SALT)
            self.assertTrue(out.endswith("Z"))
            self.assertNotEqual(out, ts)

    def test_date_only_stays_date(self):
        out = _obfuscate_string("2020-01-02", SALT)
        self.assertRegex(out, r"^\d{4}-\d{2}-\d{2}$")
        datetime.fromisoformat(out)  # valid date
        self.assertNotEqual(out, "2020-01-02")


class TestSchemaKnownFields(unittest.TestCase):
    def test_collect_known_fields(self):
        schema = {"type": "object", "properties": {
            "a": {"type": "string"},
            "nested": {"type": "object", "properties": {"b": {"type": "integer"}}},
        }, "allOf": [{"properties": {"c": {}}}]}
        self.assertEqual(collect_known_fields(schema), {"a", "nested", "b", "c"})


class TestObfuscateDocument(unittest.TestCase):
    def test_meta_uuid_and_op_preserved(self):
        doc = {"id": "cust-99", "_meta": {"uuid": "f81d-uuid", "op": "u",
                                          "source": {"table": "顧客", "ts": "2021-05-05T05:05:05Z"}}}
        out = obf_doc(doc, known={"id"})
        self.assertEqual(out["_meta"]["uuid"], "f81d-uuid")
        self.assertEqual(out["_meta"]["op"], "u")
        self.assertIn("table", out["_meta"]["source"])          # _meta keys kept as structure
        self.assertNotEqual(out["_meta"]["source"]["table"], "顧客")  # ...but values obfuscated

    def test_known_key_kept_unknown_key_obfuscated(self):
        out = obf_doc({"kept": "x", "patient_ssn": "123-45-6789", "_meta": {"uuid": "u", "op": "c"}},
                      known={"kept"})
        self.assertIn("kept", out)
        self.assertNotEqual(out["kept"], "x")            # value obfuscated
        self.assertNotIn("patient_ssn", out)             # unknown key name obfuscated away
        self.assertEqual(len(out), 3)                    # kept + obfuscated-key + _meta

    def test_unknown_subtree_fully_obfuscated(self):
        # 'city' is a known name, but it sits under an unknown key -> still obfuscated.
        out = obf_doc({"knownObj": {"dyn": {"city": "NYC"}}, "_meta": {"uuid": "u", "op": "c"}},
                      known={"knownObj", "city"})
        self.assertIn("knownObj", out)
        inner = out["knownObj"]
        self.assertNotIn("dyn", inner)                   # unknown key obfuscated
        grandchild = next(iter(inner.values()))
        self.assertNotIn("city", grandchild)             # inside an unknown subtree, all keys go

    def test_no_schema_obfuscates_all_keys(self):
        out = obf_doc({"anything": 1, "_meta": {"uuid": "u", "op": "c"}})
        self.assertNotIn("anything", out)                # no schema -> every non-_meta key obfuscated
        self.assertIn("_meta", out)

    def test_value_consistency(self):
        a = obf_doc({"x": "東京", "y": "東京"}, known={"x", "y"})
        b = obf_doc({"z": "東京"}, known={"z"})
        self.assertEqual(a["x"], a["y"])
        self.assertEqual(a["x"], b["z"])

    def test_scalars(self):
        out = obf_doc({"n": 4321, "f": 3.14, "b": True, "z": None, "e": ""},
                      known={"n", "f", "b", "z", "e"})
        self.assertNotEqual(out["n"], 4321)
        self.assertEqual(len(str(abs(out["n"]))), 4)
        self.assertIsInstance(out["b"], bool)
        self.assertIsNone(out["z"])
        self.assertEqual(out["e"], "")


if __name__ == "__main__":
    unittest.main()
