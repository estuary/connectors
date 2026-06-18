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


def _rest_pr_diff(
    pr_number: int,
    owner: str,
    repo: str,
    token: str,
    diff_line_cap: int = 2000,
) -> str:
    """
    Reconstruct a diff from the paginated 'list PR files' REST API.

    Used as fallback when gh pr diff returns HTTP 406 (PR exceeds GitHub's
    aggregate diff limits: >300 files or >20 000 lines). The per-file API
    returns individual `patch` fields that are not subject to those limits,
    so large PRs can still be summarised file-by-file.

    Binary files and files whose individual diff is also too large have no
    `patch` field; they are noted with a placeholder line.
    """
    lines_seen = 0
    parts: list = []
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

        truncated = False
        for f in batch:
            header = f'--- a/{f["filename"]}\n+++ b/{f["filename"]}'
            patch = f.get('patch') or (
                f'(binary or oversized file; {f.get("changes", "?")} changes)'
            )
            file_lines = [header] + patch.splitlines()

            if lines_seen + len(file_lines) > diff_line_cap:
                parts.append(f'... [truncated at {diff_line_cap} lines]')
                truncated = True
                break

            parts.extend(file_lines)
            lines_seen += len(file_lines)

        if truncated or len(batch) < 100:
            break
        page += 1

    return '\n'.join(parts)


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
    diff_line_cap: int = 2000,
) -> str:
    """
    Fetch PR title, body, and diff.

    Tries gh pr diff first (fast, single request). If GitHub rejects it with
    HTTP 406 (PR exceeds the aggregate diff limit of 300 files / 20 000 lines),
    falls back to the paginated 'list PR files' REST API which returns per-file
    patches without that aggregate limit.
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
        diff = '\n'.join(diff_lines[:diff_line_cap])
        if len(diff_lines) > diff_line_cap:
            diff += f'\n... [truncated at {diff_line_cap} lines]'
    except subprocess.CalledProcessError:
        # HTTP 406: PR exceeds GitHub's aggregate diff size limit.
        # Fall back to the paginated files API which returns per-file patches.
        try:
            diff = _rest_pr_diff(pr_number, owner, repo, token, diff_line_cap)
        except Exception as exc:
            diff = f'(diff not available: {exc})'

    return f'{meta.strip()}\n\n--- diff ---\n{diff}'


def build_claude_prompt(
    image_name: str,
    source_dir: str,
    chosen_pr_num: int,
    chosen_pr_body: str,
    later_pr_bodies: list,  # [(pr_num, body_text), ...]
) -> str:
    later_sections = '\n\n'.join(
        f'=== LATER: PR #{num} ===\n{body}' for num, body in later_pr_bodies
    )
    return (
        f"You are auditing a release pipeline for the Estuary connectors monorepo.\n"
        f"We are about to promote image '{image_name}' (source dir '{source_dir}/') to\n"
        f"the ':delayed' tag, pinned to PR #{chosen_pr_num}.\n\n"
        f"Determine whether any LATER PR is a roll-forward fix of the CHOSEN PR —\n"
        f"i.e. patches a bug or regression introduced BY the CHOSEN PR. A feature\n"
        f"addition, refactor, dependency bump, test addition, or unrelated bugfix\n"
        f"is NOT a roll-forward fix. Only flag PRs that exist because the CHOSEN\n"
        f"PR was buggy. Note that PR descriptions may explicitly include a link\n"
        f"to the pull-request causing the regression being fixed, however you cannot\n"
        f"take this explicit statement of regression as granted. It is possible that\n"
        f"no such explicit mention of a regression is there, but the pull-request is still\n"
        f"a roll-forward fix of a previous PR. Use the explicit statement as additional\n"
        f"information and not as the sole source of truth.\n\n"
        f"Note: this repo uses rebase-and-merge, so each PR's commits land on main\n"
        f"as a linear sequence — judge PRs as units, not individual commits.\n\n"
        f"=== CHOSEN: PR #{chosen_pr_num} ===\n{chosen_pr_body}\n\n"
        f"{later_sections}\n\n"
        f'Reply with ONLY a JSON object, no prose:\n'
        f'{{"verdict": "safe" | "hold", "reason": "<one short sentence>", '
        f'"fix_prs": [<pr numbers that are roll-forward fixes of CHOSEN>]}}\n'
        f'Use "hold" iff at least one LATER PR is a roll-forward fix of CHOSEN. '
        f'Otherwise "safe".'
    )


def call_claude(prompt: str, api_key: str, model: str = 'claude-sonnet-4-6') -> str:
    """Call the Anthropic Messages API and return the assistant's text."""
    payload = json.dumps({
        'model': model,
        'max_tokens': 1024,
        'messages': [{'role': 'user', 'content': prompt}],
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
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read())['content'][0]['text']
    except (urllib.error.URLError, KeyError, json.JSONDecodeError) as exc:
        raise RuntimeError(f'Claude API error: {exc}') from exc


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
    # Cache keyed by (chosen_pr, source_dir) so variants sharing the same
    # parent dir don't trigger duplicate Claude API calls.
    verdict_cache: dict = {}
    # Cache PR details by PR number so large LATER PRs shared across multiple
    # connectors (e.g. a monorepo sweep) are only fetched once.
    pr_details_cache: dict = {}

    def fetch_cached(pr_number: int) -> str:
        if pr_number not in pr_details_cache:
            pr_details_cache[pr_number] = fetch_pr_details(
                pr_number, owner, repo, gh_token, diff_line_cap=args.diff_line_cap
            )
        return pr_details_cache[pr_number]

    for entry in plan:
        later_prs = later_map.get(entry.image_name, [])
        if not later_prs:
            continue

        cache_key = (entry.chosen_pr, entry.source_dir)
        if cache_key in verdict_cache:
            cached = verdict_cache[cache_key]
            verdicts.append(Verdict(entry.image_name, cached.verdict, cached.reason))
            print(f'  {entry.image_name}: reusing verdict from sibling connector')
            continue

        print(
            f'Checking {entry.image_name}: PR #{entry.chosen_pr}'
            f' ({len(later_prs)} later PR(s))'
        )
        chosen_body = fetch_cached(entry.chosen_pr)
        later_bodies = [
            (pr, fetch_cached(pr))
            for pr in later_prs
        ]
        prompt = build_claude_prompt(
            entry.image_name, entry.source_dir,
            entry.chosen_pr, chosen_body,
            later_bodies,
        )

        try:
            raw = call_claude(prompt, api_key, model=args.model)
        except RuntimeError as exc:
            print(f'::warning::{entry.image_name}: {exc}; treating as safe')
            verdict = Verdict(entry.image_name, 'safe', f'api error: {exc}')
        else:
            verdict = parse_claude_verdict(raw, entry.image_name)

        verdict_cache[cache_key] = verdict
        verdicts.append(verdict)

        if verdict.verdict == 'hold':
            print(f'::warning::{entry.image_name}: HOLD — {verdict.reason}')
        else:
            print(f'  {entry.image_name}: safe — {verdict.reason or "no roll-forward fix"}')

    verdict_map = {v.image_name: v for v in verdicts}
    safe, held = build_safe_plan(plan, verdict_map)

    write_safe_plan(safe, args.output_safe)
    write_verdicts(verdicts, args.output_verdicts)

    print(f'\nClaude checked {len(verdict_cache)} unique connector(s)')
    if held:
        print(f'Held ({len(held)}): {", ".join(e.image_name for e in held)}')
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
    p.add_argument('--diff-line-cap',   type=int, default=2000)
    p.add_argument('--output-safe',     default='/tmp/safe_plan.tsv')
    p.add_argument('--output-verdicts', default='/tmp/claude_verdicts.tsv')

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
