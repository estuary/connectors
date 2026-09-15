# Contributing to estuary/connectors

This file collects the conventions a contributor needs to know when
submitting a PR. It's intentionally short; expand sections as patterns
solidify.

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
