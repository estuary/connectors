# Contributing to estuary/connectors

This file collects the conventions a contributor needs to know when
submitting a PR. It's intentionally short; expand sections as patterns
solidify.

## Commits & pull requests

These conventions build on GitHub's
[Write Better Commits, Build Better Projects](https://github.blog/developer-skills/github/write-better-commits-build-better-projects/)
post. Commit messages follow the [scoped commits](https://scopedcommits.com/)
style.

### Commit messages

Commit subjects use a `<component>: <what changed>` format. The component
is a connector, a shared package like `sqlcapture` or `estuary-cdk`, or an
area like `docs` or `ci`. For changes spanning multiple connectors, use
shorthand like `{source,materialize}-mongodb` or `source-*`.

The subject should say what the commit does, and the body should explain
why it's needed. When it's useful, also describe the approach taken or how
the change was tested. If a commit is a refactor with no behavior change,
say so. Commits with a self-explanatory subject, like a changelog entry,
don't need a body.

```
source-postgres: Default 2m statement_timeout

For historical reasons we defaulted to an unlimited statement
timeout. This is probably not the right default behavior in the
general case since it overrides any database-default timeouts
_and_ an unlimited query timeout can cause issues in prod DBs.

After this change we will default to `2m` and if a user wants to
let backfill queries run longer than that they are free to raise
the timeout or set it to `0` for unlimited.
```

### Structuring commits

`main` should have a linear history where every commit is small and
atomic. A small commit has minimal scope and does one thing. An atomic
commit is a stable, independent unit of change. The repo should still
build and pass tests at every commit. That keeps `git bisect` usable and
makes it possible to revert a commit on its own if needed.

Larger changes should be split into a sequence of commits. Avoid mixing
different kinds of changes in the same commit. For example, a bug fix, a
refactor, and a formatting change should each be their own commit.
Changelog entries, docs updates, and re-encrypted test credentials can be
separate commits too. Bruno collections should go in their own commit
since they are often large and clutter the diff. When making
the same change across multiple connectors, use one commit per connector.

Order the commits so each one builds on the last. Refactors go before the
behavior change that depends on them, and test infrastructure like a
benchmark goes before the change it validates.

Some examples:

- [#5286](https://github.com/estuary/connectors/pull/5286) makes a couple
  of incidental fixes and a refactor before fixing the bug.
- [#5180](https://github.com/estuary/connectors/pull/5180) makes a
  multi-step behavior change where each commit is safe on its own.
- [#5119](https://github.com/estuary/connectors/pull/5119) adds a
  benchmark and then the optimization it measures.
- [#5312](https://github.com/estuary/connectors/pull/5312) applies the
  same fix to two connectors in separate commits.

### Pull requests

PR titles use the same `<component>: <summary>` format as commits. The
description should summarize what the PR changes and why at a high level.
Call out any effect on existing tasks. Use the template's "Notes for
reviewers" section for anything that helps with review, like how the
commits are organized, open questions, or planned follow-up work. Link the
related issue or Slack thread too.

Before merging, clean up your PR's commits so no "WIP", "fix typo", or
"address review comments" commits end up on `main`. Review feedback should
be folded into the commit it belongs to. You can use
`git commit --fixup=<sha>` and then
`git rebase -i --autosquash origin/main` to do this. CI only tests the
final commit in a PR, so check that the earlier commits build and pass
tests too. `git rebase --exec "<test command>" origin/main` runs a command
at each commit.

## Encrypting test credentials

In order to support rapid connector development, we would like to include encrypted credentials alongside each connector wherever feasible. This allows both easily automated testing, as well as allowing other people to quickly run all connectors that have credentials. Fortunately, Flow has built-in support for encrypted credentials through the use of [`sops`](https://github.com/getsops/sops).

Instead of defining connector configuration in `test.flow.yaml`, the `config` field can also take a filename containing an optionally `sops`-encrypted file. To create one from scratch:
1. Create a new `config.yaml`
  ```yaml
  client_id: exctatic_emu@service-accounts.estuary.dev
  client_secret_sops: super_secret_password
  ```
  > **Note**: the `_sops` suffix for encrypted field is convention here. Whatever you pick for the encrypted suffix, Flow will strip that suffix out of the decrypted config object to provide to the connector.
2. Run `sops` and overwrite the file you just created with the encrypted version:
  ``` bash
  $ sops --encrypt --input-type yaml --output-type yaml --gcp-kms projects/estuary-theatre/locations/us-central1/keyRings/connector-keyring/cryptoKeys/connector-repository --encrypted-suffix _sops path/to/config.yaml
  ```
  ```yaml
    client_id: exctatic_emu@service-accounts.estuary.dev
    client_secret_sops: ENC[AES256_GCM,data:Va8E8XVrZuqtq6M1gkC9xeXMgZqu,iv:+KZd8QwB6sl1XglQkV+Utka7I9JvKtRFYPeM4eDKx6I=,tag:XyGLMTIA44bDfdT2g7TYgQ==,type:str]
    sops:
        kms: []
        gcp_kms:
            - resource_id: projects/estuary-theatre/locations/us-central1/keyRings/connector-keyring/cryptoKeys/connector-repository
            created_at: "2026-09-14T20:16:06Z"
            enc: CiUAdmEdwvAzpeqhs3jyQ2B7SQ8tX6t/3wyQizC+W7d/+E59Jqw4EkkAvE+nk9znJYU6zs/jNjfDNKke9MUVHfe09C+vT5y17LFIpSwPA+PxLN2NRogZFg5ok/bWpjh+YRreROJV00R6aKyArXGRi9Kg
        azure_kv: []
        hc_vault: []
        age: []
        lastmodified: "2026-09-14T20:16:07Z"
        mac: ENC[AES256_GCM,data:iH/mwJXl2dADaelwdKJpjvgscjBWSvz3PffvOMaeiRPQaK+OsVAnb4+Bu8nZ5xzVBu5gy56gsUqt8NDn6CBL89HRBP8WJGP1ReA1g1XKmBRFApOpdSMoMt5Mu4jARtlkPrXw8Y06VjjfVd0VdvtIx39M/kvHz51BZsQBGk+B+B0=,iv:EuYtNKOhDvoLn5EA5TwsUF2fEK7079HpSFBnw13SEmo=,tag:SE0Kltr1t8T5yYRp3Kgqyg==,type:str]
        pgp: []
        encrypted_suffix: _sops
        version: 3.9.0
  ```
3. From here on, you must use sops to edit this encrypted file. Even if you only change an unencrypted field, the `mac` will no longer be valid and the file will fail to decrypt. To edit the file using your terminal's built-in editor, simply run `sops path/to/config.yaml`, make changes, save, and `sops` will re-encrypt the file for you.

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
- `VARCHAR` columns now use UTF-8 collation by default.

### Fixed
- Reconnect after `wal_sender_timeout` no longer loops indefinitely.
```

Write entries for **customers** skimming for changes that affect them, not
engineers — describe the user-visible effect, not the implementation.
"Refactored to use new strategy interface" is a bad entry. "Field type
detection now correctly handles NUMERIC(p,0) as integer" is a good one.

Keep each bullet to one sentence. Leave out the mechanism, the root cause
and usage details such as defaults, caveats or required follow-up actions;
those belong in the PR, the connector docs, and support.

Use **Changed** for behavior that differs but wasn't broken, and **Fixed** for something a customer could have filed a bug about.

### Seeding a connector's changelog

If a connector doesn't have a `CHANGELOG.md` yet, create the file with a
single header (`# Changelog`) alongside your first user-visible change to
it. The PR template checklist below will prompt you when that's warranted.

## Docs / CHANGELOG checklist

The PR template carries a checklist item for `CHANGELOG.md` and one for
documentation. There's no automated check: whether an entry is warranted
is a judgment call for the author, backstopped by whoever reviews the PR.

### Claude Code skill

If you use Claude Code, run `/changelog` in a session that has your PR
branch checked out. It reads the diff, identifies which connector(s) are
affected, and drafts an entry for each `CHANGELOG.md`. Review and edit
before committing.
