# Contributing to estuary/connectors

This file collects the conventions a contributor needs to know when
submitting a PR. It's intentionally short; expand sections as patterns
solidify.

## Changelog entries

Each connector has its own `CHANGELOG.md` at the root of its directory
(e.g. `source-postgres/CHANGELOG.md`, `materialize-bigquery/CHANGELOG.md`).
These document user-visible changes for customers using docs.estuary.dev.

### When to add an entry

Add an entry when your change is **user-visible** — anything a customer
might notice or care about:

- New configuration fields or resource bindings
- Changes to default behavior
- Bug fixes that affect produced documents or destination state
- New supported source/destination variants
- Performance characteristics changing in a noticeable way
- Deprecations and removals

You **don't** need an entry for:

- Internal refactors that don't change behavior
- Test additions or test infrastructure
- Documentation-only changes (those go straight to `docs/`)
- Dependency bumps with no behavioral effect

### Format

Use date-headed sections with [Keep a Changelog](https://keepachangelog.com/)
categories. Most recent on top.

```markdown
# Changelog

## 2026-05-24

### Added
- New `read_only` config field to skip replication-slot creation.

### Changed
- `VARCHAR` columns now use UTF-8 collation by default; previous default
  was the database's server collation.

### Fixed
- Reconnect after `wal_sender_timeout` no longer loops indefinitely.
```

Write entries for **customers**, not engineers — describe the user-visible
effect, not the implementation. "Refactored to use new strategy interface"
is a bad entry. "Field type detection now correctly handles NUMERIC(p,0)
as integer" is a good one.

### Seeding a connector's changelog

If a connector doesn't have a `CHANGELOG.md` yet, create the file with a
single header (`# Changelog`) alongside your first user-visible change to
it. The CI check below will prompt you when that's warranted.

## Docs / CHANGELOG CI check

PRs that change connector code run an automated review
(`.github/workflows/docs-changelog-check.yaml`). It uses Claude to judge
whether the change is user-visible enough to warrant a documentation
update and/or a `CHANGELOG.md` entry, then verifies the PR contains them.
The check fails when it believes something is missing.

It's a model making a judgment call, so it will sometimes be wrong.
To dismiss it, apply the **`docs-check-skip`** label to the PR — the
check re-runs and passes. Use the label when:

- The verdict is a false positive (the change isn't actually user-visible)
- The docs update is tracked in a separate linked PR (e.g. the connector's
  docs still live in `estuary/flow`)

Fork PRs can't access the API key; the check passes with a notice and
reviewers judge manually.

### Claude Code skill

If you use Claude Code, run `/changelog` in a session that has your PR
branch checked out. It reads the diff, identifies which connector(s) are
affected, and drafts an entry for each `CHANGELOG.md`. Review and edit
before committing.
