---
name: changelog
description: Draft CHANGELOG.md entries for connectors changed in the current branch. Use when the user says "update the changelog", "write a changelog entry", "/changelog", or before submitting a PR that touches a connector.
allowed-tools: Bash Read Write Edit Glob Grep
---

# Changelog Skill

Drafts user-visible entries for each connector's `CHANGELOG.md` based on
the diff between the current branch and `main`. See [CONTRIBUTING.md](../../../CONTRIBUTING.md#changelog-entries)
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
   Group changed files by top-level directory. Keep every connector
   directory, including ones with no `CHANGELOG.md` at their root yet.
   A missing file is not a reason to skip a user-visible change.

2. **For each affected connector**, gather context:
   - Read its existing `CHANGELOG.md` to understand existing voice/format.
     If it has none, read a sibling connector's instead.
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

   **Who reads this:** customers skimming the connector's docs page for
   changes that affect them. An entry says *what* changed, not what to do
   about it and not why.

   **Categories:**
   - Added: a new option, stream, binding, or supported variant.
   - Changed: existing behavior differs but wasn't broken, including
     performance and resource use.
   - Fixed: something broken now works (failures, wrong or missing data).
     Test: could a customer have filed a bug about the old behavior?
   - Removed: dropped options or support.

   **Voice rules:**
   - Each bullet is exactly one sentence. Say what changed for the
     customer, then stop.
   - Leave out the mechanism, the root cause, why the old behavior was
     wrong, and usage details (defaults, caveats, required follow-up
     actions). The connector docs and support cover those.
   - Don't name internal state, data structures or algorithms. Describe
     failures in plain words rather than quoting error strings.
   - Name the area and the effect, so even a short entry is concrete.
     "Improved performance in some setups" is never acceptable.
   - State fixes plainly. Hedge ("may", "on busy instances") only the size
     of an effect that depends on the customer's setup, never whether the
     change happened.
   - Plain, terse prose: no stacked qualifiers, parenthetical asides,
     dashes that add explanation, or clauses interjected mid-sentence.
   - One bullet per distinct effect. Active voice, present tense.
   - Put field names and config keys in backticks.

   **Examples** (illustrative; write each entry for its own change rather
   than copying these shapes):
   - Effect, not implementation
     - Bad: "Refactored field type mapping to use new strategy interface."
     - Good: "`NUMERIC(p, 0)` columns are now captured as integers instead
       of strings."
   - Changed
     - Bad: "Primary key discovery now reads the `sys` catalog views
       directly instead of `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`.
       Compiling a query against that view takes an instance-wide lock..."
     - Good: "Primary key discovery may cause less database locking on
       busy instances."
   - Fixed
     - Bad: "Captures of databases with pre-images enabled no longer fail
       permanently with `received fragment N without first fragment`.
       MongoDB delivers change events larger than 16MB as a series of
       fragments, and the connector could checkpoint a resume token
       pointing partway through one of them..."
     - Good: "Captures with pre-images enabled no longer fail after
       restarting during a very large change event."
   - Added
     - Bad: "New `additional_backfill_filter` advanced option on each
       binding. When set, the filter clause is applied to all backfill
       queries for that table, so rows which the filter excludes are never
       backfilled. Setting or changing the filter requires re-backfilling
       the binding, while clearing it does not..."
     - Good: "New `additional_backfill_filter` binding option skips
       backfilling rows that don't match a filter."

4. **Reread each bullet as a customer skimming the docs** and cut
   anything they wouldn't miss.

5. **Show the draft to the user** with the path it'll go to, e.g.

   > Draft for `source-postgres/CHANGELOG.md`:
   > ```
   > ## 2026-05-24
   > ### Fixed
   > - Replication slot is no longer dropped when ...
   > ```
   > Apply, or want me to revise?

   If the connector has no `CHANGELOG.md`, say so and that applying
   will create it.

6. **Apply on confirmation.** Insert the new entry below `# Changelog`
   and above the most recent existing entry. Don't delete or modify
   existing entries. For a connector without the file, create it with
   a `# Changelog` header followed by the entry.

7. **Multiple connectors.** If the PR touches multiple connectors,
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
- Don't create a CHANGELOG.md for a connector whose diff has no
  user-visible change.
