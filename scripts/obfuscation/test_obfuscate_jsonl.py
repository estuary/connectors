#!/usr/bin/env python3
"""Tests for obfuscate_jsonl.py — run with `python3 -m unittest` in this dir.

Focus: every "content" character (letters/digits/numerics/emoji/non-ASCII
symbols across all scripts) is obfuscated, while structure (ASCII punctuation
and symbols, whitespace, combining marks) is preserved, deterministically and
length-preserving.
"""

from __future__ import annotations

import unittest

from obfuscate_jsonl import _obfuscate_string, obfuscate

SALT = "test"


def obf_doc(doc: dict) -> dict:
    out = obfuscate(doc, (), SALT)
    assert isinstance(out, dict)
    return out

# Representative samples across scripts / unicode blocks.
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
    "symbols": "© € ™ ½ Ⅳ ² £",
    "mixed": "User №42: 山田さん <yamada@例え.jp> 💰",
}


def _target_class(ch: str) -> str:
    """The class the obfuscator must map `ch` into (mirrors the contract).

    Every character is obfuscated: ASCII cased letters and digits stay within
    their class; EVERYTHING else (punctuation, symbols, emoji, whitespace,
    marks, caseless letters) maps to CJK. Nothing is kept."""
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
        """Every char maps to its obfuscation target class. Catches any script
        or character type slipping through unobfuscated."""
        for name, s in SAMPLES.items():
            out = _obfuscate_string(s, SALT)
            self.assertEqual(len(out), len(s), f"{name}: length changed")
            for i, (src, dst) in enumerate(zip(s, out)):
                cls = _target_class(src)
                self.assertTrue(
                    _in_class(dst, cls),
                    f"{name}[{i}] {src!r} (want {cls}) not obfuscated -> {dst!r}",
                )

    def test_no_caseless_letter_or_emoji_passes_through(self):
        """Regression for the review finding: caseless letters and emoji must
        not survive unchanged."""
        for s in ("中", "あ", "한", "א", "م", "ก", "🍕", "€", "½"):
            out = _obfuscate_string(s, SALT)
            # Each maps to a single CJK ideograph.
            self.assertEqual(len(out), 1)
            self.assertTrue(0x4E00 <= ord(out) <= 0x9FA5, f"{s!r} -> {out!r} not obfuscated")

    def test_structure_is_obfuscated(self):
        """Punctuation, symbols and whitespace are obfuscated too (to CJK) —
        no original structure survives."""
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
        s = SAMPLES["mixed"]
        self.assertNotEqual(_obfuscate_string(s, "a"), _obfuscate_string(s, "b"))

    def test_valid_utf8_roundtrip(self):
        """Obfuscated output must survive JSON encode/decode unchanged."""
        import json
        for s in SAMPLES.values():
            out = _obfuscate_string(s, SALT)
            self.assertEqual(json.loads(json.dumps(out)), out)


class TestObfuscateDocument(unittest.TestCase):
    def test_meta_uuid_and_op_preserved_everything_else_obfuscated(self):
        doc = {
            "id": "cust-99",
            "名前": "山田太郎",
            "note": "call me 📞",
            "_meta": {
                "uuid": "f81d4fae-7dec-11d0",
                "op": "u",
                "source": {"table": "顧客", "ts": "2021-05-05T05:05:05Z"},
            },
        }
        out = obf_doc(doc)
        # preserved
        self.assertEqual(out["_meta"]["uuid"], "f81d4fae-7dec-11d0")
        self.assertEqual(out["_meta"]["op"], "u")
        # obfuscated (key, japanese key's value, emoji, _meta.source)
        self.assertNotEqual(out["id"], "cust-99")
        self.assertNotEqual(out["名前"], "山田太郎")
        self.assertNotEqual(out["note"], "call me 📞")
        self.assertNotEqual(out["_meta"]["source"]["table"], "顧客")

    def test_datetime_stays_valid_rfc3339(self):
        from datetime import datetime
        for ts in ("2020-01-02T03:04:05Z", "2020-01-02T03:04:05.123456+02:00", "1999-12-31T23:59:59-05:00"):
            out = obf_doc({"ts": ts})["ts"]
            self.assertNotEqual(out, ts)
            parsed = datetime.fromisoformat(out[:-1] + "+00:00" if out.endswith("Z") else out)
            self.assertIsNotNone(parsed)

    def test_value_consistency_across_fields_and_docs(self):
        a = obf_doc({"x": "東京", "y": "東京"})
        b = obf_doc({"z": "東京"})
        self.assertEqual(a["x"], a["y"])
        self.assertEqual(a["x"], b["z"])

    def test_scalars(self):
        out = obf_doc({"n": 4321, "f": 3.14, "b": True, "z": None, "e": ""})
        self.assertNotEqual(out["n"], 4321)
        self.assertEqual(len(str(abs(out["n"]))), 4)  # magnitude preserved
        self.assertIsInstance(out["b"], bool)
        self.assertIsNone(out["z"])
        self.assertEqual(out["e"], "")


if __name__ == "__main__":
    unittest.main()
