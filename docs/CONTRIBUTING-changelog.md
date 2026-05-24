# Connector CHANGELOGs

Each connector has its own `CHANGELOG.md` at the root of its directory
(e.g. `source-postgres/CHANGELOG.md`, `materialize-bigquery/CHANGELOG.md`).
These document user-visible changes for customers using docs.estuary.dev.

## When to add an entry

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

## Format

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

## CI check

A non-blocking GitHub Action (`.github/workflows/changelog-check.yaml`)
posts a PR comment if a connector's code changed but `CHANGELOG.md`
didn't. The check is opt-in — connectors without a `CHANGELOG.md` are
ignored. To start tracking a new connector, just create the file with a
single header (`# Changelog`).

## Claude Code skill

If you use Claude Code, run `/changelog` in a session that has your PR
branch checked out. It reads the diff, identifies which connector(s) are
affected, and drafts an entry for each `CHANGELOG.md`. Review and edit
before committing.
