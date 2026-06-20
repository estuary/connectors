"""Unit tests for delayed_tag.py."""

import unittest
from delayed_tag import (
    ConnectorImage,
    PullRequest,
    PlanEntry,
    LaterEntry,
    Verdict,
    build_safe_plan,
    build_claude_content,
    build_claude_prompt,
    find_boundary,
    parse_claude_verdict,
    pr_touches_dir,
    resolve_family,
)


def make_pr(number: int, merged_epoch: int, sha_prefix: str, paths: list) -> PullRequest:
    return PullRequest(
        number=number,
        title=f'PR {number}',
        merged_epoch=merged_epoch,
        merge_sha=sha_prefix * (40 // len(sha_prefix) + 1),
        paths=frozenset(paths),
    )


def make_plan(image: str, sha7: str = 'abc1234', pr: int = 1, source_dir: str = '') -> PlanEntry:
    return PlanEntry(
        image_name=image,
        chosen_sha7=sha7,
        chosen_pr=pr,
        source_dir=source_dir or image,
    )


NEVER_REVERTED = lambda sha: False
ALWAYS_REVERTED = lambda sha: True


# ---------------------------------------------------------------------------
# pr_touches_dir
# ---------------------------------------------------------------------------

class TestPrTouchesDir(unittest.TestCase):

    def test_direct_child_file(self):
        pr = make_pr(1, 0, 'a', ['source-mysql/main.go'])
        self.assertTrue(pr_touches_dir(pr, 'source-mysql'))

    def test_nested_file(self):
        pr = make_pr(1, 0, 'a', ['source-mysql/internal/cfg/config.go'])
        self.assertTrue(pr_touches_dir(pr, 'source-mysql'))

    def test_prefix_does_not_match_sibling(self):
        # source-mysql-batch should NOT match source-mysql
        pr = make_pr(1, 0, 'a', ['source-mysql-batch/main.go'])
        self.assertFalse(pr_touches_dir(pr, 'source-mysql'))

    def test_unrelated_dir(self):
        pr = make_pr(1, 0, 'a', ['source-postgres/main.go'])
        self.assertFalse(pr_touches_dir(pr, 'source-mysql'))

    def test_empty_paths(self):
        pr = make_pr(1, 0, 'a', [])
        self.assertFalse(pr_touches_dir(pr, 'source-mysql'))

    def test_root_files_dont_match(self):
        pr = make_pr(1, 0, 'a', ['Makefile', 'go.sum', '.github/workflows/ci.yaml'])
        self.assertFalse(pr_touches_dir(pr, 'source-mysql'))

    def test_multiple_paths_one_matches(self):
        pr = make_pr(1, 0, 'a', ['source-postgres/main.go', 'source-mysql/main.go'])
        self.assertTrue(pr_touches_dir(pr, 'source-mysql'))
        self.assertTrue(pr_touches_dir(pr, 'source-postgres'))

    def test_trailing_slash_on_dir_arg(self):
        pr = make_pr(1, 0, 'a', ['source-mysql/main.go'])
        self.assertTrue(pr_touches_dir(pr, 'source-mysql/'))


# ---------------------------------------------------------------------------
# find_boundary
# ---------------------------------------------------------------------------

class TestFindBoundary(unittest.TestCase):

    def test_basic_selection(self):
        cutoff = 1000
        prs = [
            make_pr(3, 1200, 'ccc', ['source-foo/c.go']),  # after cutoff → later
            make_pr(2, 900,  'bbb', ['source-foo/b.go']),  # within cutoff ← chosen
            make_pr(1, 600,  'aaa', ['source-foo/a.go']),  # older, not considered
        ]
        chosen, later = find_boundary('source-foo', 'source-foo', prs, cutoff, NEVER_REVERTED)
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.chosen_pr, 2)
        self.assertEqual(chosen.chosen_sha7, prs[1].merge_sha[:7])
        self.assertEqual(len(later), 1)
        self.assertEqual(later[0].pr_number, 3)

    def test_skips_prs_in_other_dirs(self):
        cutoff = 1000
        prs = [
            make_pr(2, 900, 'bbb', ['source-bar/a.go']),  # wrong dir
            make_pr(1, 800, 'aaa', ['source-foo/a.go']),  # correct dir ← chosen
        ]
        chosen, later = find_boundary('source-foo', 'source-foo', prs, cutoff, NEVER_REVERTED)
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.chosen_pr, 1)
        self.assertEqual(later, [])

    def test_skips_reverted_and_falls_back(self):
        cutoff = 1000
        reverted_sha = 'aaaa' * 10
        prs = [
            PullRequest(2, 'PR 2', 900, reverted_sha, frozenset(['source-foo/a.go'])),
            make_pr(1, 800, 'bbb', ['source-foo/b.go']),  # not reverted ← chosen
        ]
        chosen, later = find_boundary(
            'source-foo', 'source-foo', prs, cutoff,
            is_reverted=lambda sha: sha == reverted_sha,
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.chosen_pr, 1)

    def test_returns_none_when_all_within_cutoff(self):
        cutoff = 1000
        prs = [make_pr(1, 1100, 'aaa', ['source-foo/a.go'])]
        chosen, later = find_boundary('source-foo', 'source-foo', prs, cutoff, NEVER_REVERTED)
        self.assertIsNone(chosen)
        self.assertEqual(len(later), 1)

    def test_returns_none_when_all_reverted(self):
        cutoff = 1000
        prs = [make_pr(1, 900, 'aaa', ['source-foo/a.go'])]
        chosen, later = find_boundary('source-foo', 'source-foo', prs, cutoff, ALWAYS_REVERTED)
        self.assertIsNone(chosen)
        self.assertEqual(later, [])

    def test_returns_none_when_empty(self):
        chosen, later = find_boundary('source-foo', 'source-foo', [], 1000, NEVER_REVERTED)
        self.assertIsNone(chosen)
        self.assertEqual(later, [])

    def test_later_includes_only_connector_prs(self):
        cutoff = 1000
        prs = [
            make_pr(5, 1400, 'eee', ['source-foo/a.go']),  # later, same dir
            make_pr(4, 1200, 'ddd', ['source-bar/a.go']),  # later, different dir → excluded
            make_pr(3, 1100, 'ccc', ['source-foo/b.go']),  # later, same dir
            make_pr(2, 900,  'bbb', ['source-foo/c.go']),  # chosen
            make_pr(1, 700,  'aaa', ['source-foo/d.go']),  # older
        ]
        chosen, later = find_boundary('source-foo', 'source-foo', prs, cutoff, NEVER_REVERTED)
        self.assertEqual(chosen.chosen_pr, 2)
        self.assertEqual({e.pr_number for e in later}, {3, 5})

    def test_image_name_propagated_to_later_entries(self):
        cutoff = 1000
        prs = [
            make_pr(2, 1200, 'bbb', ['source-foo/a.go']),
            make_pr(1, 900, 'aaa', ['source-foo/b.go']),
        ]
        chosen, later = find_boundary(
            'source-foo-variant', 'source-foo', prs, cutoff, NEVER_REVERTED
        )
        self.assertEqual(chosen.image_name, 'source-foo-variant')
        self.assertEqual(later[0].image_name, 'source-foo-variant')

    def test_sha7_is_first_seven_chars(self):
        cutoff = 1000
        prs = [make_pr(1, 900, 'deadbeef1', ['source-foo/a.go'])]
        chosen, _ = find_boundary('source-foo', 'source-foo', prs, cutoff, NEVER_REVERTED)
        self.assertEqual(chosen.chosen_sha7, prs[0].merge_sha[:7])


# ---------------------------------------------------------------------------
# parse_claude_verdict
# ---------------------------------------------------------------------------

class TestParseClaudeVerdict(unittest.TestCase):

    def test_safe_verdict(self):
        raw = '{"verdict": "safe", "reason": "unrelated change", "fix_prs": []}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.verdict, 'safe')
        self.assertEqual(v.reason, 'unrelated change')
        self.assertEqual(v.image_name, 'img')

    def test_hold_verdict(self):
        raw = '{"verdict": "hold", "reason": "PR #42 patches regression in CHOSEN", "fix_prs": [42]}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.verdict, 'hold')
        self.assertIn('42', v.reason)

    def test_invalid_json_falls_back_to_safe(self):
        v = parse_claude_verdict('not valid json!', 'img')
        self.assertEqual(v.verdict, 'safe')
        self.assertIn('could not parse', v.reason)

    def test_unknown_verdict_value_falls_back_to_safe(self):
        raw = '{"verdict": "maybe", "reason": "unsure"}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.verdict, 'safe')

    def test_missing_verdict_key_defaults_to_safe(self):
        raw = '{"reason": "looks fine"}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.verdict, 'safe')

    def test_empty_string_falls_back_to_safe(self):
        v = parse_claude_verdict('', 'img')
        self.assertEqual(v.verdict, 'safe')

    def test_verdict_case_insensitive(self):
        raw = '{"verdict": "HOLD", "reason": "bug found"}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.verdict, 'hold')

    def test_missing_reason_defaults_to_empty(self):
        raw = '{"verdict": "safe"}'
        v = parse_claude_verdict(raw, 'img')
        self.assertEqual(v.reason, '')


# ---------------------------------------------------------------------------
# build_safe_plan
# ---------------------------------------------------------------------------

class TestBuildSafePlan(unittest.TestCase):

    def test_no_verdicts_all_pass(self):
        plan = [make_plan('a'), make_plan('b')]
        safe, held = build_safe_plan(plan, {})
        self.assertEqual([e.image_name for e in safe], ['a', 'b'])
        self.assertEqual(held, [])

    def test_hold_removes_entry(self):
        plan = [make_plan('a'), make_plan('b'), make_plan('c')]
        verdicts = {'b': Verdict('b', 'hold', 'regression found')}
        safe, held = build_safe_plan(plan, verdicts)
        self.assertEqual([e.image_name for e in safe], ['a', 'c'])
        self.assertEqual([e.image_name for e in held], ['b'])

    def test_safe_verdict_passes_entry(self):
        plan = [make_plan('a')]
        verdicts = {'a': Verdict('a', 'safe', 'all good')}
        safe, held = build_safe_plan(plan, verdicts)
        self.assertEqual(len(safe), 1)
        self.assertEqual(held, [])

    def test_multiple_holds(self):
        plan = [make_plan('a'), make_plan('b'), make_plan('c')]
        verdicts = {
            'a': Verdict('a', 'hold', 'bug a'),
            'c': Verdict('c', 'hold', 'bug c'),
        }
        safe, held = build_safe_plan(plan, verdicts)
        self.assertEqual([e.image_name for e in safe], ['b'])
        self.assertEqual({e.image_name for e in held}, {'a', 'c'})

    def test_preserves_plan_order(self):
        plan = [make_plan('z'), make_plan('a'), make_plan('m')]
        safe, _ = build_safe_plan(plan, {})
        self.assertEqual([e.image_name for e in safe], ['z', 'a', 'm'])


# ---------------------------------------------------------------------------
# build_claude_prompt
# ---------------------------------------------------------------------------

class TestBuildClaudePrompt(unittest.TestCase):

    def test_contains_image_and_dir(self):
        prompt = build_claude_prompt(
            'source-mysql', 'source-mysql', 42,
            'Title\n\nbody text', [(99, 'fix title\n\nfix body')]
        )
        self.assertIn('source-mysql', prompt)
        self.assertIn('PR #42', prompt)
        self.assertIn('PR #99', prompt)

    def test_instructs_json_response(self):
        prompt = build_claude_prompt('img', 'dir', 1, 'body', [])
        self.assertIn('"verdict"', prompt)
        self.assertIn('"hold"', prompt)
        self.assertIn('"safe"', prompt)

    def test_multiple_later_prs(self):
        later = [(10, 'PR ten'), (20, 'PR twenty'), (30, 'PR thirty')]
        prompt = build_claude_prompt('img', 'dir', 5, 'chosen', later)
        self.assertIn('PR #10', prompt)
        self.assertIn('PR #20', prompt)
        self.assertIn('PR #30', prompt)

    def test_no_later_prs(self):
        prompt = build_claude_prompt('img', 'dir', 5, 'chosen body', [])
        self.assertIn('PR #5', prompt)


# ---------------------------------------------------------------------------
# build_claude_content (prompt caching)
# ---------------------------------------------------------------------------

class TestBuildClaudeContent(unittest.TestCase):

    def test_two_blocks_prefix_is_cached(self):
        blocks = build_claude_content('img', 'dir', 5, 'chosen', [(9, 'fix')])
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0]['cache_control'], {'type': 'ephemeral'})
        self.assertNotIn('cache_control', blocks[1])

    def test_chosen_pr_is_in_uncached_tail(self):
        # The per-connector CHOSEN PR must live in the trailing block, not the
        # cached prefix — otherwise the prefix would differ per connector.
        blocks = build_claude_content('img', 'dir', 5, 'chosen', [(9, 'fix')])
        self.assertNotIn('CHOSEN: PR #5', blocks[0]['text'])
        self.assertIn('CHOSEN: PR #5', blocks[1]['text'])
        self.assertIn('LATER: PR #9', blocks[0]['text'])

    def test_shared_later_set_yields_identical_prefix(self):
        # The whole point: two connectors with different CHOSEN PRs but the same
        # LATER set produce a byte-identical cacheable prefix (a cache hit).
        later = [(99, 'sweep diff')]
        a = build_claude_content('conn-a', 'conn-a', 1, 'a body', later)
        b = build_claude_content('conn-b', 'conn-b', 2, 'b body', later)
        self.assertEqual(a[0]['text'], b[0]['text'])
        self.assertNotEqual(a[1]['text'], b[1]['text'])

    def test_later_section_sorted_for_byte_stability(self):
        # Same LATER set in different order must still produce the same prefix.
        forward = build_claude_content('i', 'd', 1, 'c', [(10, 'x'), (20, 'y')])
        reverse = build_claude_content('i', 'd', 1, 'c', [(20, 'y'), (10, 'x')])
        self.assertEqual(forward[0]['text'], reverse[0]['text'])


# ---------------------------------------------------------------------------
# resolve_family
# ---------------------------------------------------------------------------

class TestResolveFamily(unittest.TestCase):
    # source-postgres with two variants, plus an unrelated connector.
    IMAGES = [
        ConnectorImage('source-postgres', 'source-postgres'),
        ConnectorImage('source-alloydb', 'source-postgres'),
        ConnectorImage('source-amazon-rds-postgres', 'source-postgres'),
        ConnectorImage('source-mysql', 'source-mysql'),
    ]

    def test_parent_returns_whole_family(self):
        family = resolve_family(self.IMAGES, 'source-postgres')
        self.assertEqual(
            {c.image_name for c in family},
            {'source-postgres', 'source-alloydb', 'source-amazon-rds-postgres'},
        )

    def test_variant_returns_whole_family(self):
        # Selecting a variant still fast-forwards the entire family.
        family = resolve_family(self.IMAGES, 'source-alloydb')
        self.assertEqual(
            {c.image_name for c in family},
            {'source-postgres', 'source-alloydb', 'source-amazon-rds-postgres'},
        )

    def test_standalone_connector(self):
        family = resolve_family(self.IMAGES, 'source-mysql')
        self.assertEqual([c.image_name for c in family], ['source-mysql'])

    def test_unknown_connector_is_empty(self):
        self.assertEqual(resolve_family(self.IMAGES, 'source-nope'), [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
