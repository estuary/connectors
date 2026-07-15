---
description: Draft CHANGELOG.md entries for connectors changed in the current branch. Use when the user says "update the changelog", "write a changelog entry", "/changelog", or before submitting a PR that touches a connector.
---

# Changelog Skill

Drafts user-visible entries for each connector's `CHANGELOG.md` based on
the diff between the current branch and `main`. See [CONTRIBUTING.md](../../CONTRIBUTING.md#changelog-entries)
for the convention.

## When to invoke

- User says "update the changelog", "draft a changelog entry", "/changelog"
- User is preparing a PR and the diff touches a connector directory
  (`source-*/`, `materialize-*/`, `capture-*/`, `filesource/`, `filesink/`)

## Procedure

1. **Identify affected connectors.** Run:
   ```bash
   git diff --name-only main...HEAD
   ```
   Group changed files by top-level directory. Keep only connector
   directories that have an existing `CHANGELOG.md` at their root —
   ignore directories without one (per opt-in convention).

2. **For each affected connector**, gather context:
   - Read its existing `CHANGELOG.md` to understand existing voice/format.
   - Read its recent diff: `git diff main...HEAD -- <connector-dir>/`.
   - Identify *user-visible* effects:
     - New/removed/renamed config fields → schema diff (look for changes
       in `endpoint.go`, `*.json`, `schema.go`, or jsonschema struct tags).
     - Default behavior changes → look for new feature flags, changed
       constants, modified default values.
     - Bug fixes affecting output → look for fixes to value mapping,
       reconnect/retry logic, transaction handling.
     - New supported variants (e.g. RDS, Aurora) → new sub-directories
       or registration entries.
     - Documentation changes alone → don't need an entry; the docs ARE
       the change.

3. **Draft an entry** for each connector. Format:

   ```markdown
   ## YYYY-MM-DD

   ### Added
   - ...

   ### Changed
   - ...

   ### Fixed
   - ...
   ```

   Only include categories that have at least one bullet. Today's date
   in UTC (`date -u +%Y-%m-%d`).

   **Voice rules:**
   - Write for customers, not engineers.
   - Describe the user-visible effect, not the implementation.
     - Bad: "Refactored field type mapping to use new strategy interface."
     - Good: "`NUMERIC(p, 0)` columns are now captured as integers instead
       of strings."
   - One bullet per distinct effect. Don't merge unrelated changes.
   - Active voice, present tense.
   - Reference user-facing names (field names, config keys, error messages)
     in backticks.

4. **Show the draft to the user** with the path it'll go to, e.g.

   > Draft for `source-postgres/CHANGELOG.md`:
   > ```
   > ## 2026-05-24
   > ### Fixed
   > - Replication slot is no longer dropped when ...
   > ```
   > Apply, or want me to revise?

5. **Apply on confirmation.** Insert the new entry below `# Changelog`
   and above the most recent existing entry. Don't delete or modify
   existing entries.

6. **Multiple connectors.** If the PR touches multiple connectors,
   show all drafts at once, then apply all on a single confirmation.

## What to skip

- Don't draft an entry if the only changes in the connector dir are:
  - Tests (`*_test.go`, `tests/`, `.snapshots/`)
  - CI/build files
  - Comments-only diffs
  - Dependency bumps with no behavioral effect
- If unsure whether a change is user-visible, surface that uncertainty
  to the user rather than guessing.

## Don't

- Don't invent changes that aren't in the diff.
- Don't commit the CHANGELOG edits — let the user review and commit them.
- Don't add a CHANGELOG.md to a connector that doesn't already have one;
  that's a deliberate opt-in step the user should take manually.
