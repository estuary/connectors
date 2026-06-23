#!/usr/bin/env python3
"""
delayed_tag.py — compute the ':delayed' image tag plan for Estuary connectors.

Subcommands
-----------
plan   Read ci.yaml / python.yaml, query GitHub for merged PRs, then for each
       connector pick the most recent PR that (a) touched that connector's
       directory, (b) was merged at least CUTOFF_DAYS ago, and (c) has not
       been reverted on main. Writes plan.tsv and later.tsv.

check  Read plan.tsv / later.tsv, call Claude Sonnet for each connector that
       has LATER PRs to detect roll-forward fixes, then write safe_plan.tsv
       (plan rows safe to publish) and claude_verdicts.tsv.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PullRequest:
    number: int
    title: str
    merged_epoch: int       # unix timestamp of mergedAt
    merge_sha: str          # full SHA (mergeCommit.oid)
    paths: frozenset        # file paths changed by this PR


@dataclass(frozen=True)
class ConnectorImage:
    image_name: str  # e.g. "source-mysql"
    source_dir: str  # source directory; same as image_name except for variants


@dataclass(frozen=True)
class PlanEntry:
    image_name: str
    chosen_sha7: str  # first 7 chars of merge_sha
    chosen_pr: int
    source_dir: str


@dataclass(frozen=True)
class LaterEntry:
    image_name: str
    pr_number: int


@dataclass(frozen=True)
class Verdict:
    image_name: str
    verdict: str  # "safe" | "hold"
    reason: str


# ---------------------------------------------------------------------------
# Pure functions — fully testable without I/O or subprocess
# ---------------------------------------------------------------------------

def pr_touches_dir(pr: PullRequest, source_dir: str) -> bool:
    """Return True if any changed path is under source_dir/."""
    prefix = source_dir.rstrip('/') + '/'
    return any(p.startswith(prefix) for p in pr.paths)


def find_boundary(
    image_name: str,
    source_dir: str,
    prs: list,
    cutoff_epoch: int,
    is_reverted: Callable,
) -> tuple:
    """
    Walk PRs newest-first and return (chosen, later) where:
      chosen  — most recent PlanEntry with merged_epoch <= cutoff that touches
                source_dir and whose merge commit hasn't been reverted on main.
      later   — LaterEntries for PRs merged after cutoff touching source_dir
                (the candidates for roll-forward fixes of chosen).

    Returns (None, later) when no eligible PR is found.
    """
    later: list = []
    for pr in prs:
        if not pr_touches_dir(pr, source_dir):
            continue
        if pr.merged_epoch > cutoff_epoch:
            later.append(LaterEntry(image_name=image_name, pr_number=pr.number))
            continue
        if is_reverted(pr.merge_sha):
            print(f'  {image_name}: PR #{pr.number} ({pr.merge_sha[:7]}) reverted, skipping',
                  file=sys.stderr)
            continue
        return PlanEntry(
            image_name=image_name,
            chosen_sha7=pr.merge_sha[:7],
            chosen_pr=pr.number,
            source_dir=source_dir,
        ), later
    return None, later


def parse_claude_verdict(response_text: str, image_name: str) -> Verdict:
    """
    Parse Claude's JSON response into a Verdict.
    Falls back to 'safe' on any parse error so that a malformed response
    does not silently block a deployment.
    """
    try:
        obj = json.loads(response_text)
        v = str(obj.get('verdict', 'safe')).lower().strip()
        if v not in ('safe', 'hold'):
            v = 'safe'
        return Verdict(image_name=image_name, verdict=v, reason=str(obj.get('reason', '')))
    except (json.JSONDecodeError, AttributeError, TypeError) as exc:
        return Verdict(
            image_name=image_name,
            verdict='safe',
            reason=f'could not parse Claude response ({exc}): {response_text[:120]}',
        )


def build_safe_plan(
    plan: list,
    verdicts: dict,
) -> tuple:
    """
    Split plan entries into (safe, held) based on Claude verdicts.
    Connectors absent from verdicts default to safe.
    """
    safe, held = [], []
    for entry in plan:
        v = verdicts.get(entry.image_name)
        if v and v.verdict == 'hold':
            held.append(entry)
        else:
            safe.append(entry)
    return safe, held


def select_commit_tag(tags: list) -> Optional[str]:
    """
    Return the first commit-SHA tag in `tags`, or None.

    CI tags every published image with the 7-char commit SHA (`git rev-parse
    HEAD | head -c7`), alongside non-commit tags like 'dev', 'delayed', 'local'
    and ':vN' version tags. A commit tag is exactly 7 lowercase hex chars, which
    none of the others match (':vN' starts with 'v', 'dev' contains 'v', etc.).
    """
    return next((t for t in tags if re.fullmatch(r'[0-9a-f]{7}', t)), None)


def filter_backward_moves(
    plan: list,
    current_commit: Callable,
    is_ancestor: Callable,
) -> tuple:
    """
    Drop plan entries whose boundary commit would move ':delayed' BACKWARD onto
    an older image than it currently points at. This protects deliberate
    fast-forwards (forward-delayed-tag.yaml) from being silently undone by the
    next nightly run, which otherwise re-pins ':delayed' to the 2-week boundary
    with no regard for where the tag already sits.

      current_commit(image_name) -> Optional[str]
          the commit (sha7) ':delayed' currently points at, or None when there
          is no ':delayed' tag yet or it cannot be resolved.
      is_ancestor(a, b) -> bool
          True when commit a is an ancestor of b (a is older-or-equal).

    An entry is dropped iff its boundary is a STRICT ancestor of (older than)
    the current ':delayed' commit.

    Entries with an unresolvable current commit are kept: we cannot prove a
    regression, so we preserve the pre-guard behaviour (tag the boundary).
    Returns (kept, skipped) where skipped is [(entry, current_sha7), ...].
    """
    kept, skipped = [], []
    for e in plan:
        cur = current_commit(e.image_name)
        if not cur or cur == e.chosen_sha7:
            kept.append(e)
            continue
        # Strict-ancestor test: boundary older than current, and not equal.
        if is_ancestor(e.chosen_sha7, cur) and not is_ancestor(cur, e.chosen_sha7):
            skipped.append((e, cur))
        else:
            kept.append(e)
    return kept, skipped


# ---------------------------------------------------------------------------
# Connector image discovery
# ---------------------------------------------------------------------------

def load_connector_images(ci_yaml: str, python_yaml: str) -> list:
    """
    Return the list of (image_name, source_dir) pairs from the workflow
    matrix entries.  Variants in <connector>/VARIANTS inherit their parent's
    source_dir.
    """
    def yq_list(query: str, path: str) -> list:
        try:
            out = subprocess.check_output(
                ['yq', query, path], text=True, stderr=subprocess.DEVNULL
            )
            return [l.strip() for l in out.splitlines() if l.strip()]
        except subprocess.CalledProcessError:
            return []

    raw_connectors = (
        yq_list('.jobs.build_connectors.strategy.matrix.connector[]', ci_yaml) +
        yq_list('.jobs.build_connectors.strategy.matrix.include[].connector', ci_yaml) +
        yq_list('.jobs.py_connector.strategy.matrix.connector[].name', python_yaml)
    )

    seen: set = set()
    connectors: list = []
    for c in raw_connectors:
        if c not in seen:
            seen.add(c)
            connectors.append(c)

    images: list = []
    seen_images: set = set()

    def add(image: str, source_dir: str) -> None:
        if image not in seen_images:
            seen_images.add(image)
            images.append(ConnectorImage(image_name=image, source_dir=source_dir))

    for connector in connectors:
        add(connector, connector)
        variants_path = Path(connector) / 'VARIANTS'
        if variants_path.exists():
            for variant in variants_path.read_text().split():
                add(variant, connector)

    return images


def resolve_family(connector_images: list, selected: str) -> list:
    """
    Return every ConnectorImage that shares the source_dir of `selected`
    (matched by image_name) — i.e. the connector plus all of its variants,
    regardless of whether the user named the parent or a variant.

    Returns [] when `selected` is not a known image name.
    """
    match = next((c for c in connector_images if c.image_name == selected), None)
    if match is None:
        return []
    return [c for c in connector_images if c.source_dir == match.source_dir]


def resolve_version(source_dir: str, python_yaml: str) -> str:
    """
    Return the version tag (e.g. 'v3') CI publishes for a connector.

    Python connectors carry their version in the python.yaml matrix; Go
    connectors carry it in a VERSION file in the connector directory. We check
    the matrix first because some Python connectors have no VERSION file.
    Returns '' when neither source yields a version.
    """
    try:
        out = subprocess.check_output(
            ['yq',
             f'.jobs.py_connector.strategy.matrix.connector[] '
             f'| select(.name == "{source_dir}") | .version',
             python_yaml],
            text=True, stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            v = line.strip()
            if v and v != 'null':
                return v
    except subprocess.CalledProcessError:
        pass

    version_file = Path(source_dir) / 'VERSION'
    if version_file.exists():
        return version_file.read_text().strip()
    return ''


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

_FETCH_PRS_QUERY = """
query($owner: String!, $repo: String!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      states: MERGED
      baseRefName: "main"
      orderBy: {field: UPDATED_AT, direction: DESC}
      first: 100
      after: $after
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        mergedAt
        updatedAt
        mergeCommit { oid }
        files(first: 100) { totalCount nodes { path } }
      }
    }
  }
}
"""


def _graphql(query: str, variables: dict, token: str) -> dict:
    payload = json.dumps({'query': query, 'variables': variables}).encode()
    req = urllib.request.Request(
        'https://api.github.com/graphql',
        data=payload,
        headers={
            'Authorization': f'bearer {token}',
            'Content-Type': 'application/json',
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _rest_pr_files(pr_number: int, owner: str, repo: str, token: str) -> list:
    """Fetch all changed file paths for a PR via REST (handles >100 files)."""
    paths = []
    page = 1
    while True:
        url = (
            f'https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files'
            f'?per_page=100&page={page}'
        )
        req = urllib.request.Request(url, headers={
            'Authorization': f'bearer {token}',
            'Accept': 'application/vnd.github+json',
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            batch = json.loads(resp.read())
        if not batch:
            break
        paths.extend(f['filename'] for f in batch)
        if len(batch) < 100:
            break
        page += 1
    return paths


# Diff size caps. The diff dominates the prompt we send Claude, and a single PR
# can regenerate golden snapshot files (e.g. several connectors'
# .snapshots/TestIntegration-migrate) whose diffs pack tens of thousands of
# characters onto a SINGLE line. A line-count cap alone does not bound such a
# diff: 2000 lines at 60k chars each is ~3 MB / ~800k tokens, which the
# Anthropic API rejects with HTTP 400 "prompt is too long", aborting the run.
# We therefore also truncate any individual over-long line (keeping the file
# visible without its noise) and cap the diff's total character count.
DEFAULT_DIFF_LINE_CAP = 2000
DEFAULT_LINE_CHAR_CAP = 1000
DEFAULT_DIFF_CHAR_CAP = 120_000


def _truncate_diff(
    lines: list,
    line_cap: int,
    line_char_cap: int,
    char_cap: int,
) -> str:
    """
    Bound a diff three ways so it can never blow the model's context window:
    by line count, by per-line length (over-long generated/snapshot lines are
    truncated in place, not dropped, so the file stays visible), and by total
    character count. Returns the joined, truncated diff text.
    """
    out: list = []
    chars = 0
    for i, line in enumerate(lines):
        if i >= line_cap:
            out.append(f'... [truncated at {line_cap} lines]')
            break
        if len(line) > line_char_cap:
            line = f'{line[:line_char_cap]} ... [line truncated, was {len(line)} chars]'
        if chars + len(line) > char_cap:
            out.append(f'... [truncated at {char_cap} chars]')
            break
        out.append(line)
        chars += len(line) + 1  # +1 for the newline added by the final join
    return '\n'.join(out)


def _rest_pr_diff(
    pr_number: int,
    owner: str,
    repo: str,
    token: str,
    diff_line_cap: int = DEFAULT_DIFF_LINE_CAP,
    line_char_cap: int = DEFAULT_LINE_CHAR_CAP,
    diff_char_cap: int = DEFAULT_DIFF_CHAR_CAP,
) -> str:
    """
    Reconstruct a diff from the paginated 'list PR files' REST API.

    Used as fallback when gh pr diff returns HTTP 406 (PR exceeds GitHub's
    aggregate diff limits: >300 files or >20 000 lines). The per-file API
    returns individual `patch` fields that are not subject to those limits,
    so large PRs can still be summarised file-by-file.

    Binary files and files whose individual diff is also too large have no
    `patch` field; they are noted with a placeholder line. The collected lines
    are bounded by _truncate_diff before returning.
    """
    lines: list = []
    page = 1

    while True:
        url = (
            f'https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files'
            f'?per_page=100&page={page}'
        )
        req = urllib.request.Request(url, headers={
            'Authorization': f'bearer {token}',
            'Accept': 'application/vnd.github+json',
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            batch = json.loads(resp.read())

        if not batch:
            break

        for f in batch:
            lines.append(f'--- a/{f["filename"]}\n+++ b/{f["filename"]}')
            patch = f.get('patch') or (
                f'(binary or oversized file; {f.get("changes", "?")} changes)'
            )
            lines.extend(patch.splitlines())

        # Stop paginating once we already have more than enough lines to fill
        # the cap; _truncate_diff makes the final cut below.
        if len(lines) > diff_line_cap or len(batch) < 100:
            break
        page += 1

    return _truncate_diff(lines, diff_line_cap, line_char_cap, diff_char_cap)


def _iso_to_epoch(iso: str) -> int:
    return int(
        datetime.datetime.fromisoformat(iso.replace('Z', '+00:00')).timestamp()
    )


def fetch_merged_prs(
    owner: str,
    repo: str,
    token: str,
    lookback_days: int = 60,
) -> list:
    """
    Fetch merged PRs against main via GraphQL, sorted newest-first by mergedAt.
    Stops paginating once updatedAt is older than lookback_days.
    """
    stop_updated_epoch = int(time.time()) - lookback_days * 86400
    raw_nodes: list = []
    cursor: Optional[str] = None

    for page in range(1, 11):
        data = _graphql(
            _FETCH_PRS_QUERY,
            {'owner': owner, 'repo': repo, 'after': cursor},
            token,
        )
        pr_conn = data['data']['repository']['pullRequests']
        nodes = pr_conn['nodes']
        raw_nodes.extend(nodes)
        print(f'  page {page}: {len(raw_nodes)} PRs fetched', file=sys.stderr)

        has_next = pr_conn['pageInfo']['hasNextPage']
        cursor = pr_conn['pageInfo']['endCursor']
        if not nodes:
            break
        oldest_updated = _iso_to_epoch(nodes[-1]['updatedAt'])
        if not has_next or oldest_updated < stop_updated_epoch:
            break

    prs = []
    for node in raw_nodes:
        merge_commit = node.get('mergeCommit') or {}
        merge_sha = merge_commit.get('oid', '')
        if not merge_sha:
            continue

        files = node['files']
        if files['totalCount'] > 100:
            print(
                f"  PR #{node['number']} has {files['totalCount']} files; fetching full list",
                file=sys.stderr,
            )
            paths = frozenset(_rest_pr_files(node['number'], owner, repo, token))
        else:
            paths = frozenset(f['path'] for f in files['nodes'])

        prs.append(PullRequest(
            number=node['number'],
            title=node['title'],
            merged_epoch=_iso_to_epoch(node['mergedAt']),
            merge_sha=merge_sha,
            paths=paths,
        ))

    prs.sort(key=lambda p: p.merged_epoch, reverse=True)
    return prs


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def check_is_reverted(sha: str, head_sha: str) -> bool:
    """
    Return True if a revert commit for sha exists between sha..head_sha.
    git revert (and GitHub's UI-driven revert) writes the exact string
    "This reverts commit <sha>." in the commit message.
    """
    try:
        out = subprocess.check_output(
            ['git', 'log', '--format=%H', f'{sha}..{head_sha}',
             f'--grep=This reverts commit {sha}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        return bool(out.strip())
    except subprocess.CalledProcessError:
        return False


def check_is_ancestor(ancestor: str, descendant: str) -> bool:
    """
    Return True if `ancestor` is an ancestor of (older than or equal to)
    `descendant`. Returns False when either commit is unknown to git, so an
    unresolvable revision can never be misread as a backward move.
    """
    try:
        subprocess.check_call(
            ['git', 'merge-base', '--is-ancestor', ancestor, descendant],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    except subprocess.CalledProcessError:
        return False


# ---------------------------------------------------------------------------
# Registry / package introspection for the backward-move guard
# ---------------------------------------------------------------------------

def image_digests(ref: str) -> frozenset:
    """
    Return the set of content digests for an image ref: the top-level manifest
    digest plus every child (platform) manifest digest.

    The nightly and forward workflows both publish ':delayed' via
    `docker buildx imagetools create`, which wraps a connector's single-arch
    image in a fresh manifest LIST. So ':delayed' carries a list digest that
    matches no ':<sha7>' tag, while its child digest equals the single-arch
    image that the ':<sha7>' tag points at. Returning both lets a caller match
    ':delayed' back to its originating commit regardless of which layer the
    ':<sha7>' tag sits on. Empty when the ref cannot be inspected.
    """
    try:
        out = subprocess.check_output(
            ['docker', 'buildx', 'imagetools', 'inspect', ref,
             '--format', '{{json .Manifest}}'],
            text=True, stderr=subprocess.DEVNULL,
        )
        manifest = json.loads(out)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return frozenset()

    digests = set()
    top = manifest.get('digest')
    if top:
        digests.add(top)
    for child in manifest.get('manifests') or []:
        d = child.get('digest')
        if d:
            digests.add(d)
    return frozenset(digests)


def _list_package_versions(
    image: str, owner: str, token: str, page: int,
) -> list:
    """One page (newest-first) of an org container package's versions."""
    url = (
        f'https://api.github.com/orgs/{owner}/packages/container/{image}'
        f'/versions?per_page=100&page={page}'
    )
    req = urllib.request.Request(url, headers={
        'Authorization': f'bearer {token}',
        'Accept': 'application/vnd.github+json',
    })
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def current_delayed_commit(
    image: str, owner: str, token: str, max_pages: int = 3,
) -> Optional[str]:
    """
    Return the commit (sha7) that ':delayed' currently points at, or None.

    Inspects the live ':delayed' manifest to learn its content digests, then
    scans the package's versions (newest-first, stopping at the first match) for
    the version carrying one of those digests and reads its commit-SHA tag.
    Stops after `max_pages`; ':delayed' always points at a recent image, so the
    match is on the first page or two. Returns None on any failure so the guard
    fails open (never blocks a legitimate forward move on a transient error).
    """
    digests = image_digests(f'ghcr.io/estuary/{image}:delayed')
    if not digests:
        return None
    try:
        for page in range(1, max_pages + 1):
            versions = _list_package_versions(image, owner, token, page)
            if not versions:
                break
            for v in versions:
                if v.get('name') not in digests:
                    continue
                tags = (v.get('metadata') or {}).get('container', {}).get('tags', [])
                sha = select_commit_tag(tags)
                if sha:
                    return sha
    except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
        print(f'::warning::{image}: could not resolve current :delayed commit ({exc})',
              file=sys.stderr)
        return None
    return None


# ---------------------------------------------------------------------------
# Claude / PR details
# ---------------------------------------------------------------------------

def fetch_pr_details(
    pr_number: int,
    owner: str,
    repo: str,
    token: str,
    diff_line_cap: int = DEFAULT_DIFF_LINE_CAP,
    line_char_cap: int = DEFAULT_LINE_CHAR_CAP,
    diff_char_cap: int = DEFAULT_DIFF_CHAR_CAP,
) -> str:
    """
    Fetch PR title, body, and diff.

    Tries gh pr diff first (fast, single request). If GitHub rejects it with
    HTTP 406 (PR exceeds the aggregate diff limit of 300 files / 20 000 lines),
    falls back to the paginated 'list PR files' REST API which returns per-file
    patches without that aggregate limit. Either way the diff is bounded by
    _truncate_diff so a PR that regenerates large golden files cannot push the
    prompt past the model's context window.
    """
    try:
        meta = subprocess.check_output(
            ['gh', 'pr', 'view', str(pr_number),
             '--json', 'title,body',
             '--template', '{{.title}}\n\n{{.body}}'],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        meta = f'(could not fetch PR #{pr_number} metadata)'

    try:
        diff_lines = subprocess.check_output(
            ['gh', 'pr', 'diff', str(pr_number)],
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
        diff = _truncate_diff(diff_lines, diff_line_cap, line_char_cap, diff_char_cap)
    except subprocess.CalledProcessError:
        # HTTP 406: PR exceeds GitHub's aggregate diff size limit.
        # Fall back to the paginated files API which returns per-file patches.
        try:
            diff = _rest_pr_diff(
                pr_number, owner, repo, token,
                diff_line_cap, line_char_cap, diff_char_cap,
            )
        except Exception as exc:
            diff = f'(diff not available: {exc})'

    return f'{meta.strip()}\n\n--- diff ---\n{diff}'


# Static task framing. Deliberately free of any per-connector specifics (no
# image name, PR numbers, or diffs) so it is byte-identical across every call
# in a run and sits at the front of the cacheable prefix.
_VERDICT_INSTRUCTIONS = (
    "You are auditing a release pipeline for the Estuary connectors monorepo.\n"
    "We promote one connector image to the ':delayed' tag, pinned to a CHOSEN\n"
    "pull request. Determine whether any LATER PR is a roll-forward fix of the\n"
    "CHOSEN PR — i.e. patches a bug or regression introduced BY the CHOSEN PR. A\n"
    "feature addition, refactor, dependency bump, test addition, or unrelated\n"
    "bugfix is NOT a roll-forward fix. Only flag PRs that exist because the\n"
    "CHOSEN PR was buggy. Note that PR descriptions may explicitly include a link\n"
    "to the pull-request causing the regression being fixed, however you cannot\n"
    "take this explicit statement of regression as granted. It is possible that\n"
    "no such explicit mention of a regression is there, but the pull-request is\n"
    "still a roll-forward fix of a previous PR. Use the explicit statement as\n"
    "additional information and not as the sole source of truth.\n\n"
    "Note: this repo uses rebase-and-merge, so each PR's commits land on main as\n"
    "a linear sequence — judge PRs as units, not individual commits."
)


def _later_section(later_pr_bodies: list) -> str:
    """Render the LATER-PR block, sorted by PR number so that two connectors
    sharing the same set of LATER PRs produce byte-identical text (and thus a
    shared cache prefix)."""
    return '\n\n'.join(
        f'=== LATER: PR #{num} ===\n{body}' for num, body in sorted(later_pr_bodies)
    )


def _chosen_section(
    image_name: str,
    source_dir: str,
    chosen_pr_num: int,
    chosen_pr_body: str,
) -> str:
    """The per-connector tail: the CHOSEN PR plus the response instruction."""
    return (
        f"We are about to promote image '{image_name}' (source dir "
        f"'{source_dir}/') to the ':delayed' tag, pinned to PR #{chosen_pr_num}.\n\n"
        f"=== CHOSEN: PR #{chosen_pr_num} ===\n{chosen_pr_body}\n\n"
        f'Reply with ONLY a JSON object, no prose:\n'
        f'{{"verdict": "safe" | "hold", "reason": "<one short sentence>", '
        f'"fix_prs": [<pr numbers that are roll-forward fixes of CHOSEN>]}}\n'
        f'Use "hold" iff at least one LATER PR is a roll-forward fix of CHOSEN. '
        f'Otherwise "safe".'
    )


def build_claude_content(
    image_name: str,
    source_dir: str,
    chosen_pr_num: int,
    chosen_pr_body: str,
    later_pr_bodies: list,  # [(pr_num, body_text), ...]
) -> list:
    """
    Build the Messages API `content` blocks for one verdict request.

    The static instructions + LATER-PR diffs form the first block and carry a
    cache_control breakpoint. Connectors that share the same set of LATER PRs —
    the common (and expensive) case after a monorepo-wide sweep PR, where one
    large diff is a LATER PR for many connectors — produce a byte-identical
    prefix, so every call after the first reads those diffs from cache (~0.1x
    input price) instead of re-billing them in full at 1x. The per-connector
    CHOSEN PR + question go in a trailing, uncached block.

    cmd_check orders its calls so connectors sharing a LATER set run
    back-to-back, keeping the cache entry warm within its TTL.
    """
    prefix = f'{_VERDICT_INSTRUCTIONS}\n\n{_later_section(later_pr_bodies)}'
    suffix = _chosen_section(image_name, source_dir, chosen_pr_num, chosen_pr_body)
    return [
        {'type': 'text', 'text': prefix, 'cache_control': {'type': 'ephemeral'}},
        {'type': 'text', 'text': suffix},
    ]


def build_claude_prompt(
    image_name: str,
    source_dir: str,
    chosen_pr_num: int,
    chosen_pr_body: str,
    later_pr_bodies: list,  # [(pr_num, body_text), ...]
) -> str:
    """Full prompt text as a single string. build_claude_content holds the
    cache-aware block split actually sent to the API; this mirrors it for
    readability and tests."""
    blocks = build_claude_content(
        image_name, source_dir, chosen_pr_num, chosen_pr_body, later_pr_bodies
    )
    return '\n\n'.join(b['text'] for b in blocks)


# HTTP statuses worth retrying: rate-limit (429), request timeout/conflict, and
# server/gateway/overload errors. Any other 4xx (e.g. 401 auth, 400 bad request)
# won't be fixed by retrying, so we fail fast and loudly on those.
_RETRYABLE_STATUS = frozenset({408, 409, 429, 500, 502, 503, 504, 529})


def _content_chars(content) -> int:
    """Approximate prompt size in characters, for logging how close a request
    sits to the model's context window."""
    if isinstance(content, list):
        return sum(len(b.get('text', '')) for b in content)
    return len(content)


def _http_error_body(exc: urllib.error.HTTPError) -> str:
    """Read an HTTPError's response body. The Anthropic API returns a JSON error
    whose message states the precise cause (e.g. 'prompt is too long: 812345
    tokens > 200000 maximum'), which is exactly what we want in the CI log when
    a request is rejected. Returns '' if the body can't be read, and truncates
    so a large body can't flood the log."""
    try:
        return exc.read().decode('utf-8', 'replace')[:2000]
    except Exception:
        return ''


def _claude_request(content, api_key: str, model: str) -> str:
    """One Anthropic Messages API call. Returns the assistant's text, or raises
    urllib/JSONDecodeError on failure — call_claude decides what is retryable.
    A 200 with an unexpected body shape raises KeyError, which call_claude does
    NOT retry: an identical request would parse identically, so it propagates
    immediately and fails the run."""
    payload = json.dumps({
        'model': model,
        'max_tokens': 1024,
        'messages': [{'role': 'user', 'content': content}],
    }).encode()
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=payload,
        headers={
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        },
    )
    print(f'    request: ~{_content_chars(content)} chars', file=sys.stderr)
    with urllib.request.urlopen(req, timeout=90) as resp:
        obj = json.loads(resp.read())
    u = obj.get('usage', {})
    print(
        f'    tokens: input={u.get("input_tokens", 0)} '
        f'cache_write={u.get("cache_creation_input_tokens", 0)} '
        f'cache_read={u.get("cache_read_input_tokens", 0)} '
        f'output={u.get("output_tokens", 0)}',
        file=sys.stderr,
    )
    return obj['content'][0]['text']


def call_claude(
    content,
    api_key: str,
    model: str = 'claude-sonnet-4-6',
    *,
    attempts: int = 4,
    base_delay: float = 2.0,
    sleep: Callable = time.sleep,
) -> str:
    """Call the Anthropic Messages API and return the assistant's text.

    `content` is a Messages API content value — either a plain string or a list
    of content blocks (the cache-aware form from build_claude_content). Token
    usage (including cache reads/writes) is logged to stderr so the cache hit
    rate is visible in CI logs.

    Transient failures (network errors, HTTP 429/5xx, malformed responses) are
    retried up to `attempts` times with exponential backoff; non-retryable
    failures (e.g. HTTP 401/400) raise immediately. When every attempt is
    exhausted the final RuntimeError propagates. Callers MUST NOT downgrade this
    to a 'safe' verdict: a roll-forward check we never ran is not a check that
    passed, and treating it as safe could ship a known-buggy image to
    ':delayed'."""
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _claude_request(content, api_key, model)
        except urllib.error.HTTPError as exc:
            body = _http_error_body(exc)
            if exc.code not in _RETRYABLE_STATUS:
                # Include the API's error body so the CI log states exactly why
                # the request was rejected (e.g. an over-long prompt), rather
                # than a bare 'HTTP 400'.
                raise RuntimeError(
                    f'Claude API error: HTTP {exc.code} {exc.reason}'
                    + (f': {body}' if body else '')
                ) from exc
            if body:
                print(f'    Claude API HTTP {exc.code} body: {body}', file=sys.stderr)
            last_exc = exc
        except (urllib.error.URLError, json.JSONDecodeError) as exc:
            last_exc = exc

        if attempt < attempts:
            delay = base_delay * (2 ** (attempt - 1))
            print(
                f'    Claude API attempt {attempt}/{attempts} failed'
                f' ({last_exc}); retrying in {delay:.0f}s',
                file=sys.stderr,
            )
            sleep(delay)

    raise RuntimeError(
        f'Claude API failed after {attempts} attempts: {last_exc}'
    )


# ---------------------------------------------------------------------------
# TSV I/O
# ---------------------------------------------------------------------------

def write_plan(entries: list, path: str) -> None:
    with open(path, 'w') as f:
        for e in entries:
            f.write(f'{e.image_name}\t{e.chosen_sha7}\t{e.chosen_pr}\t{e.source_dir}\n')


def read_plan(path: str) -> list:
    entries = []
    with open(path) as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) == 4:
                entries.append(PlanEntry(
                    image_name=parts[0],
                    chosen_sha7=parts[1],
                    chosen_pr=int(parts[2]),
                    source_dir=parts[3],
                ))
    return entries


def write_later(entries: list, path: str) -> None:
    with open(path, 'w') as f:
        for e in entries:
            f.write(f'{e.image_name}\t{e.pr_number}\n')


def read_later(path: str) -> dict:
    """Returns {image_name: [pr_number, ...]}."""
    later: dict = {}
    try:
        with open(path) as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) == 2:
                    later.setdefault(parts[0], []).append(int(parts[1]))
    except FileNotFoundError:
        pass
    return later


def write_safe_plan(entries: list, path: str) -> None:
    """Write the two-column file consumed by the tag step (image TAB sha7)."""
    with open(path, 'w') as f:
        for e in entries:
            f.write(f'{e.image_name}\t{e.chosen_sha7}\n')


def read_safe_plan(path: str) -> list:
    """Read the two-column 'image TAB sha7' file written by write_safe_plan."""
    entries = []
    with open(path) as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) == 2 and parts[0]:
                entries.append(PlanEntry(
                    image_name=parts[0],
                    chosen_sha7=parts[1],
                    chosen_pr=0,
                    source_dir=parts[0],
                ))
    return entries


def write_verdicts(verdicts: list, path: str) -> None:
    with open(path, 'w') as f:
        for v in verdicts:
            f.write(f'{v.image_name}\t{v.verdict}\t{v.reason}\n')


def write_skips(skips: list, path: str) -> None:
    """Write the (entry, current_sha7) backward-move skips: image, boundary, current."""
    with open(path, 'w') as f:
        for entry, current in skips:
            f.write(f'{entry.image_name}\t{entry.chosen_sha7}\t{current}\n')


def write_check_failures(failures: list, path: str) -> None:
    """Write the (image, error) connectors whose roll-forward check could not be
    completed. Errors are flattened to a single line so the TSV stays one row
    per connector."""
    with open(path, 'w') as f:
        for image, error in failures:
            f.write(f'{image}\t{" ".join(error.split())}\n')


# ---------------------------------------------------------------------------
# Cross-run verdict cache
# ---------------------------------------------------------------------------
#
# A roll-forward verdict is fully determined by its inputs: the CHOSEN PR, the
# connector's source dir, and the set of LATER PRs (PR diffs are immutable once
# merged). The `check` step runs nightly, but most connectors are idle from one
# night to the next — same boundary, same LATER set — so re-asking Claude the
# identical question every night is pure waste. The workflow persists this
# cache across runs via actions/cache; a later run reuses any verdict whose key
# is unchanged and only calls Claude for connectors that actually moved.

def verdict_cache_key(chosen_pr: int, source_dir: str, later_prs: list) -> str:
    """Stable key identifying the inputs to a roll-forward verdict. Variants
    share source_dir (and therefore boundary + LATER set), so they collapse to
    the same key. Sorted LATER PRs keep the key order-independent."""
    later = ','.join(str(n) for n in sorted(later_prs))
    return f'{chosen_pr}|{source_dir}|{later}'


def load_verdict_cache(path: str) -> dict:
    """Load the prior run's verdict cache (key -> {'verdict', 'reason'}). A
    missing or unparseable file yields an empty cache: a cold start re-checks
    every connector, it never produces a wrong verdict."""
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        k: v for k, v in data.items()
        if isinstance(v, dict) and v.get('verdict') in ('safe', 'hold')
    }


def write_verdict_cache(cache: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(cache, f, indent=0, sort_keys=True)


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_plan(args: argparse.Namespace) -> None:
    head_sha = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], text=True,
    ).strip()
    cutoff_epoch = int(time.time()) - args.cutoff_days * 86400
    owner, repo = args.repo.split('/', 1)
    token = os.environ['GH_TOKEN']

    print(f'Cutoff: {datetime.datetime.utcfromtimestamp(cutoff_epoch).isoformat()}Z')
    print(f'HEAD:   {head_sha[:7]}')

    connector_images = load_connector_images(args.ci_yaml, args.python_yaml)
    print(f'Considering {len(connector_images)} connector image(s)')

    prs = fetch_merged_prs(owner, repo, token, lookback_days=args.lookback_days)
    print(f'Fetched {len(prs)} merged PR(s) in {args.lookback_days}-day window')

    plan: list = []
    later_all: list = []

    for ci in connector_images:
        if not Path(ci.source_dir).is_dir():
            print(f'skip {ci.image_name}: {ci.source_dir}/ not found at HEAD')
            continue
        chosen, later = find_boundary(
            ci.image_name,
            ci.source_dir,
            prs,
            cutoff_epoch,
            is_reverted=lambda sha, h=head_sha: check_is_reverted(sha, h),
        )
        if chosen is None:
            print(f'skip {ci.image_name}: no eligible PR in lookback window')
            continue
        plan.append(chosen)
        later_all.extend(later)
        print(f'  {ci.image_name} -> PR #{chosen.chosen_pr} ({chosen.chosen_sha7})')

    write_plan(plan, args.output_plan)
    write_later(later_all, args.output_later)
    print(f'\nPlan: {len(plan)} entries, {len(later_all)} (image, later-PR) pairs')


def cmd_check(args: argparse.Namespace) -> None:
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    plan = read_plan(args.plan)

    if not api_key:
        print('::warning::ANTHROPIC_API_KEY not set; skipping roll-forward check')
        write_safe_plan(plan, args.output_safe)
        write_verdicts([], args.output_verdicts)
        return

    gh_token = os.environ.get('GH_TOKEN', '')
    gh_repo = os.environ.get('GITHUB_REPOSITORY', args.repo)
    owner, repo = gh_repo.split('/', 1) if '/' in gh_repo else ('', '')

    later_map = read_later(args.later)
    verdicts: list = []
    # In-run cache keyed by (chosen_pr, source_dir) so variants sharing the same
    # parent dir don't trigger duplicate Claude API calls.
    verdict_cache: dict = {}
    # Prior-run verdicts, restored across runs via actions/cache. Reused
    # whenever a connector's (chosen_pr, source_dir, later set) is unchanged.
    prior_cache = load_verdict_cache(args.verdict_cache)
    # Verdicts to persist for the next run, keyed by verdict_cache_key. Written
    # in a finally below so progress survives even if a later connector aborts
    # the run on an unrecoverable API error.
    new_cache: dict = {}
    api_calls = 0
    prior_hits = 0
    # (image_name, error) for connectors whose check could not be completed.
    # Surfaced loudly (::error:: + summary file) so a persistently-failing
    # connector is visible even though it no longer aborts the whole run.
    check_failures: list = []
    # Cache PR details by PR number so large LATER PRs shared across multiple
    # connectors (e.g. a monorepo sweep) are only fetched once.
    pr_details_cache: dict = {}

    def fetch_cached(pr_number: int) -> str:
        if pr_number not in pr_details_cache:
            pr_details_cache[pr_number] = fetch_pr_details(
                pr_number, owner, repo, gh_token,
                diff_line_cap=args.diff_line_cap,
                line_char_cap=args.line_char_cap,
                diff_char_cap=args.diff_char_cap,
            )
        return pr_details_cache[pr_number]

    # Process connectors grouped by their LATER-PR set. build_claude_content
    # caches the (instructions + LATER diffs) prefix, which is identical for
    # connectors sharing a LATER set; running them back-to-back keeps that
    # cache entry warm so all but the first read it instead of re-billing the
    # diffs. Iterate a sorted copy — `plan` stays in its original order for
    # build_safe_plan below.
    ordered = sorted(
        plan,
        key=lambda e: (sorted(later_map.get(e.image_name, [])), e.chosen_pr),
    )
    try:
        for entry in ordered:
            later_prs = later_map.get(entry.image_name, [])
            if not later_prs:
                continue

            xrun_key = verdict_cache_key(entry.chosen_pr, entry.source_dir, later_prs)
            in_run_key = (entry.chosen_pr, entry.source_dir)

            if in_run_key in verdict_cache:
                cached = verdict_cache[in_run_key]
                verdict = Verdict(entry.image_name, cached.verdict, cached.reason)
                print(f'  {entry.image_name}: reusing verdict from sibling connector')
            elif xrun_key in prior_cache:
                prev = prior_cache[xrun_key]
                verdict = Verdict(entry.image_name, prev['verdict'], prev.get('reason', ''))
                verdict_cache[in_run_key] = verdict
                prior_hits += 1
                print(
                    f'  {entry.image_name}: reusing prior-run verdict'
                    f' ({verdict.verdict}; PR #{entry.chosen_pr} + later set unchanged)'
                )
            else:
                print(
                    f'Checking {entry.image_name}: PR #{entry.chosen_pr}'
                    f' ({len(later_prs)} later PR(s))'
                )
                chosen_body = fetch_cached(entry.chosen_pr)
                later_bodies = [
                    (pr, fetch_cached(pr))
                    for pr in later_prs
                ]
                content = build_claude_content(
                    entry.image_name, entry.source_dir,
                    entry.chosen_pr, chosen_body,
                    later_bodies,
                )

                # No fallback to 'safe' on failure. call_claude already retries
                # transient errors, so a RuntimeError here means the check could
                # not be performed for THIS connector. We HOLD it (exclude it
                # from tagging) rather than promote an unchecked, possibly-buggy
                # image to ':delayed' — but we isolate the failure to this one
                # connector and keep going, so a single bad PR can't freeze the
                # whole pipeline (the prior behaviour, where one failure aborted
                # the run and no connector was ever tagged). The synthetic hold
                # is deliberately NOT persisted to new_cache below, so the
                # connector is re-checked next run instead of being stuck held.
                try:
                    raw = call_claude(content, api_key, model=args.model)
                except RuntimeError as exc:
                    print(
                        f'::error::{entry.image_name}: roll-forward check could'
                        f' not be completed; holding (not tagging) — {exc}'
                    )
                    verdict = Verdict(
                        entry.image_name, 'hold', f'roll-forward check failed: {exc}'
                    )
                    verdict_cache[in_run_key] = verdict
                    verdicts.append(verdict)
                    check_failures.append((entry.image_name, str(exc)))
                    continue
                verdict = parse_claude_verdict(raw, entry.image_name)
                api_calls += 1
                verdict_cache[in_run_key] = verdict

                if verdict.verdict == 'hold':
                    print(f'::warning::{entry.image_name}: HOLD — {verdict.reason}')
                else:
                    print(f'  {entry.image_name}: safe — {verdict.reason or "no roll-forward fix"}')

            verdicts.append(verdict)
            new_cache[xrun_key] = {'verdict': verdict.verdict, 'reason': verdict.reason}
    finally:
        # Persist verdicts computed so far so the next run reuses them even when
        # this run aborts partway on an unrecoverable API error.
        write_verdict_cache(new_cache, args.verdict_cache)

    verdict_map = {v.image_name: v for v in verdicts}
    safe, held = build_safe_plan(plan, verdict_map)

    write_safe_plan(safe, args.output_safe)
    write_verdicts(verdicts, args.output_verdicts)
    write_check_failures(check_failures, args.output_failures)

    print(
        f'\nClaude checked {api_calls} connector(s) via API;'
        f' {prior_hits} reused from prior-run cache'
    )
    if held:
        print(f'Held ({len(held)}): {", ".join(e.image_name for e in held)}')
    if check_failures:
        print(
            f'::warning::{len(check_failures)} connector(s) could not be checked'
            f' and were held: {", ".join(img for img, _ in check_failures)}'
        )
    print(f'Tagging {len(safe)}/{len(plan)} connector(s) after hold filter')


def cmd_guard(args: argparse.Namespace) -> None:
    """
    Filter the safe plan so the nightly never moves ':delayed' backward.

    For each connector about to be tagged, resolve the commit ':delayed' already
    points at and drop the entry when re-tagging the 2-week boundary would
    regress onto an older image (e.g. undoing a forward-delayed-tag run). Writes
    the surviving entries to output_safe and the dropped ones to output_skips.
    """
    plan = read_safe_plan(args.plan)
    gh_repo = os.environ.get('GITHUB_REPOSITORY', args.repo)
    owner = gh_repo.split('/', 1)[0] if '/' in gh_repo else ''
    token = os.environ.get('GH_TOKEN', '')

    if not owner or not token:
        print('::warning::GH_TOKEN/owner unavailable; skipping backward-move guard')
        write_safe_plan(plan, args.output_safe)
        write_skips([], args.output_skips)
        return

    # Resolve each connector's current ':delayed' commit at most once.
    commit_cache: dict = {}

    def current_commit(image: str) -> Optional[str]:
        if image not in commit_cache:
            commit_cache[image] = current_delayed_commit(image, owner, token)
        return commit_cache[image]

    kept, skipped = filter_backward_moves(plan, current_commit, check_is_ancestor)

    write_safe_plan(kept, args.output_safe)
    write_skips(skipped, args.output_skips)

    for entry, current in skipped:
        print(
            f'::warning::{entry.image_name}: holding :delayed at {current} — boundary '
            f'{entry.chosen_sha7} is older (would move backward)'
        )
    print(f'Guard: {len(kept)} to tag, {len(skipped)} held to avoid moving backward')


def cmd_forward(args: argparse.Namespace) -> None:
    """
    Resolve a user-selected connector to the (image, version) pairs whose
    ':delayed' tag should be fast-forwarded to match the latest published
    version tag. Writes a two-column 'image TAB version' file consumed by the
    workflow's tagging step. Includes all variants of the selected connector.
    """
    selected = args.connector.strip()
    images = load_connector_images(args.ci_yaml, args.python_yaml)
    family = resolve_family(images, selected)
    if not family:
        print(
            f'::error::unknown connector {selected!r}: not found in the '
            f'ci.yaml / python.yaml connector matrices',
            file=sys.stderr,
        )
        sys.exit(1)

    source_dir = family[0].source_dir
    version = resolve_version(source_dir, args.python_yaml)
    if not version:
        print(
            f'::error::could not determine the version tag for {selected!r} '
            f'(source dir {source_dir!r}/)',
            file=sys.stderr,
        )
        sys.exit(1)

    with open(args.output, 'w') as f:
        for ci in family:
            f.write(f'{ci.image_name}\t{version}\n')

    print(f'{selected}: version {version}, {len(family)} image(s) to fast-forward:')
    for ci in family:
        suffix = '' if ci.image_name == source_dir else ' (variant)'
        print(f'  {ci.image_name}:{version} -> :delayed{suffix}')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('plan', help='compute per-connector boundary PRs')
    p.add_argument('--repo',          default=os.environ.get('GITHUB_REPOSITORY', ''))
    p.add_argument('--ci-yaml',       default='.github/workflows/ci.yaml')
    p.add_argument('--python-yaml',   default='.github/workflows/python.yaml')
    p.add_argument('--cutoff-days',   type=int, default=14)
    p.add_argument('--lookback-days', type=int, default=60)
    p.add_argument('--output-plan',   default='/tmp/plan.tsv')
    p.add_argument('--output-later',  default='/tmp/later.tsv')

    p = sub.add_parser('check', help='Claude roll-forward fix check')
    p.add_argument('--repo',            default=os.environ.get('GITHUB_REPOSITORY', ''))
    p.add_argument('--plan',            default='/tmp/plan.tsv')
    p.add_argument('--later',           default='/tmp/later.tsv')
    p.add_argument('--model',           default='claude-sonnet-4-6')
    p.add_argument('--diff-line-cap',   type=int, default=DEFAULT_DIFF_LINE_CAP)
    p.add_argument('--line-char-cap',   type=int, default=DEFAULT_LINE_CHAR_CAP,
                   help='truncate any single diff line longer than this')
    p.add_argument('--diff-char-cap',   type=int, default=DEFAULT_DIFF_CHAR_CAP,
                   help="cap each PR's diff at this many characters")
    p.add_argument('--output-safe',     default='/tmp/safe_plan.tsv')
    p.add_argument('--output-verdicts', default='/tmp/claude_verdicts.tsv')
    p.add_argument('--output-failures', default='/tmp/check_failures.tsv',
                   help='connectors whose roll-forward check could not be completed')
    p.add_argument('--verdict-cache',   default='/tmp/verdict_cache.json',
                   help='cross-run verdict cache (restored/saved via actions/cache)')

    p = sub.add_parser(
        'guard',
        help="drop plan entries that would move ':delayed' backward",
    )
    p.add_argument('--repo',         default=os.environ.get('GITHUB_REPOSITORY', ''))
    p.add_argument('--plan',         default='/tmp/safe_plan.tsv')
    p.add_argument('--output-safe',  default='/tmp/final_plan.tsv')
    p.add_argument('--output-skips', default='/tmp/backward_skips.tsv')

    p = sub.add_parser(
        'forward',
        help="fast-forward a connector's ':delayed' tag to its latest version",
    )
    p.add_argument('--connector',   required=True,
                   help='connector image name, e.g. source-postgres')
    p.add_argument('--ci-yaml',     default='.github/workflows/ci.yaml')
    p.add_argument('--python-yaml', default='.github/workflows/python.yaml')
    p.add_argument('--output',      default='/tmp/forward_plan.tsv')

    args = parser.parse_args(argv)
    {
        'plan': cmd_plan,
        'check': cmd_check,
        'guard': cmd_guard,
        'forward': cmd_forward,
    }[args.command](args)


if __name__ == '__main__':
    main()
