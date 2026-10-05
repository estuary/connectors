# Evidence markers

Every behavioral claim about a provider's API carries one of these markers the moment it is drafted. They live in the Bruno collection's `docs:` blocks (`<connector>/bruno/`); connector code carries none of them (`DOC-FLAG-ONLY-UNVERIFIED`). An assumption written as fact is a latent bug; the same assumption written as UNOBSERVABLE with a fallback is a design decision a reader can audit.

| Marker | Meaning | Must state |
| ------ | ------- | ---------- |
| `**VERIFIED (YYYY-MM-DD):**` | A saved response example backs it. The date is the live run's date, so a reader can judge staleness. | The observed finding, never the refuted pre-run guess. |
| `**DOCUMENTED (YYYY-MM-DD):**` | Read (not recalled) in the provider's docs on that date at the cited URL; no saved example. Rests on the provider's word. | The URL. |
| `**PENDING:**` | Answerable by seeding data or probing further, not yet done. | The concrete path to close it: which seed to run, which probe to author. |
| `**UNOBSERVABLE:**` | Not practically verifiable: the state can't be seeded by us, or arises only outside our control (a soft bounce needing a real mailbox failure; a feature-gated object the account can't create). | The reason, the assumption proceeded on, the adjacent evidence that supports it, and the fallback if it proves wrong. |

`**LIMITATION**` is an additional tag, not a fifth status: an endpoint-specific constraint with connector consequences carries it alongside its status (`**LIMITATION (VERIFIED 2026-06-30):**`) so `grep -rl LIMITATION <connector>/bruno/` enumerates them. The collection root's `docs:` keeps an index of LIMITATION findings and the account-wide constraints block only; the marker definitions live here.

## Hand-off rule

No `**PENDING:**` claim survives hand-off: each resolves to VERIFIED, or is reclassified UNOBSERVABLE with its justification. Check with:

```bash
grep -rl '\*\*PENDING' <connector>/bruno/ | grep -v opencollection.yml
```

Two branches keep PENDING findings honestly: a **docs-only run** (the user declined credentials, so no evidence was ever collectable) and a **seeding gate that no one ran** (`GATE-SEEDING` resolved to hand-over or none). Relabeling either as UNOBSERVABLE misstates the evidence. Both list every remaining PENDING finding in the hand-off summary.
