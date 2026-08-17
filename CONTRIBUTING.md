# Contributing to estuary/connectors

This file collects the conventions a contributor needs to know when
submitting a PR. It's intentionally short; expand sections as patterns
solidify.

## Encrypting test credentials

In order to support rapid connector development, we would like to include encrypted credentials alongside each connector wherever feasible. This allows both easily automated testing, as well as allowing other people to quickly run all connectors that have credentials. Fortunately, Flow has built-in support for encrypted credentials through the use of [`sops`](https://github.com/getsops/sops).

Instead of defining connector configuration in `test.flow.yaml`, the `config` field can also take a filename containing an optionally `sops`-encrypted file. To create one from scratch:
1. Create a new `connector_config.yaml`
  ```yaml
  client_id: exctatic_emu@service-accounts.estuary.dev
  client_secret_sops: super_secret_password
  ```
  > **Note**: the `_sops` suffix for encrypted field is convention here. Whatever you pick for the encrypted suffix, Flow will strip that suffix out of the decrypted config object to provide to the connector.
2. Run `sops` and overwrite the file you just created with the encrypted version:
  ``` bash
  $ sops --encrypt --input-type yaml --output-type yaml --gcp-kms projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/CI-estuary-flow --encrypted-suffix _sops path/to/connector_config.yaml
  ```
  ```yaml
  client_id: exctatic_emu@service-accounts.estuary.dev
  client_secret_sops: ENC[AES256_GCM,data:c3BEsuHJLjIt7+G1hwb6x29BU7CK,iv:6LfUthR8c5DFTmucFC5NnMiOGal7v+PYixadovIm2gw=,tag:uHtTuPXLxEi4HLunVeLjkQ==,type:str]
  sops:
    kms: []
    gcp_kms:
        - resource_id: projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/CI-estuary-flow
          created_at: "2023-11-06T22:16:37Z"
          enc: CiQAW8BC2JnhfMjWVLeRYPPQgnzBVM2MtLMlh/84pcfCRbQExBcSSQBgR/fKuXztEtnXLcNceSt9XGDi0A/9nqYQrFFqTD5d0R2HEATmH4Fyqg/Gn5/sYAdDegI0g3hHYZd91rJir0TaljFQ2YRAnYw=
    azure_kv: []
    hc_vault: []
    age: []
    lastmodified: "2023-11-06T22:16:37Z"
    mac: ENC[AES256_GCM,data:LzU+fTji6MHPFjXMNqnQAizwL3jBCvt9zltFz291u81ocIMSkdFiff+KRoTHz8kYvdoBVfJt8CesdCOqGiTgLKmea7teKiJuK5bBEOuzEY4lfC1fRYVkX+Dw6t1Mx5CvlLlS3ioisVtPG53eAGMcZDhZ7iJt7nm7qvo3Tkq7pSU=,iv:1OI2BJIyxo8DNnJmGn4lqU8TlFcdG9R9GhognGyyNY8=,tag:fPjklOLlS7OzDFszFK75hg==,type:str]
    pgp: []
    encrypted_suffix: _sops
    version: 3.7.3
  ```
3. From here on, you must use sops to edit this encrypted file. Even if you only change an unencrypted field, the `mac` will no longer be valid and the file will fail to decrypt. To edit the file using your terminal's built-in editor, simply run `sops path/to/connector_config.yaml`, make changes, save, and `sops` will re-encrypt the file for you.

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
