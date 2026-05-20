# D O C S

This directory contains two kinds of documentation:

## Engineering docs (this directory)

Internal references for connector developers.

- [Materializations](materialize/README.md) — materialization gRPC protocol reference
- [Inbound networking](inbound_networking.md) — Dockerfile `LABEL`s for exposed ports
- [Feature flags](feature_flags.md) — per-task feature-flag mechanism + bulk-publish workflow

## Customer-facing docs (`reference/Connectors/`)

Markdown sources for the connector pages published to
[docs.estuary.dev](https://docs.estuary.dev). These live alongside the
connector code so that a single PR can change both behavior and docs.

The directory layout mirrors the public URL exactly, e.g.

```
docs/reference/Connectors/capture-connectors/PostgreSQL/
  → docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
```

The Docusaurus site that hosts these pages lives in
[`estuary/documentation`](https://github.com/estuary/documentation), which
sources this directory via git submodule.

