# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **multi-context**: captures and materializations are separate domains with separate vocabulary, separate protocols, and separate failure modes. A term like "binding", "checkpoint", or "backfill" does not mean the same thing on both sides.

## Before exploring, read these

- **`CONTEXT-MAP.md`** at the repo root — it names each context and points at that context's `CONTEXT.md`. Read the `CONTEXT.md` for every context your topic touches, and only those.
- **`docs/adr/`** — system-wide decisions (things spanning both captures and materializations, or the repo as a whole).
- **`docs/contexts/<context>/adr/`** — decisions scoped to one context. Read the ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

Contexts live under `docs/contexts/` rather than beside the code, because a single context spans dozens of sibling top-level connector directories (`source-*`, `materialize-*`) with no shared parent.

```
/
├── CONTEXT-MAP.md                          ← names the contexts, maps code to them
├── docs/
│   ├── adr/                                ← system-wide decisions
│   └── contexts/
│       ├── captures/
│       │   ├── CONTEXT.md
│       │   └── adr/                        ← capture-specific decisions
│       └── materializations/
│           ├── CONTEXT.md
│           └── adr/                        ← materialization-specific decisions
├── source-*/                               ← captures context
├── materialize-*/                          ← materializations context
└── go/                                     ← shared; system-wide
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the relevant context's `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

When a term is defined in both contexts, use the definition from the context you are working in, and say which one you mean if the output crosses the boundary.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_

A capture-scoped ADR does not bind materialization work, or vice versa. If a decision needs to hold across both, it belongs in `docs/adr/`.
