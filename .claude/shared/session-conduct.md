# Session conduct

How to run a connector session. These leave no trace in a diff — see the `conduct-only` tag in [README.md](README.md); a reviewer must not comment on them.

---

### `CONDUCT-TODO-LIST` · conduct-only

Open the session with a TODO list: one task per phase of whatever skill you're running, plus any provider-specific work that comes up.

---

### `CONDUCT-CONSENT-DESTRUCTIVE` · conduct-only

Ask the user for consent before any destructive operation — deleting generated files or directories, rewriting bindings, mutating provider state.

Provider-state mutation additionally falls under `API-NEVER-MUTATE`.

---

### `CONDUCT-STATE-CONVENTIONS` · conduct-only

State out loud when you're deliberately following an existing convention. State equally out loud when you're deviating, and justify it. Silence is worse than confirmation — a reviewer cannot tell a considered deviation from an accident.

---

### `CONDUCT-CITE-SOURCE` · checkable-from-diff

When you copy a pattern from another stream or connector, cite the exact `file:line` you copied from so the user can sanity-check the transfer.

Checkable from a diff in one direction only: a new fetch function that structurally mirrors a sibling but cites nothing is a finding. The reviewer cannot verify a citation points at the _right_ line without reading it, so it should follow the citation and confirm rather than assume.

---

### `CONDUCT-GREP-BEFORE-CITE` · conduct-only

If you propose a function, field, or flag that you _remember_ seeing in the connector, grep for it before recommending it. The codebase may have changed since that memory formed, and a confidently-cited symbol that no longer exists costs more trust than an admitted uncertainty.

---

### `CONDUCT-VERIFY-FIRST` · conduct-only

Verify first, code second. For anything the provider's API does, the Bruno collection is the source of truth for "what does this endpoint actually return today" — not the docs, and not recall. See [provider-api-consent.md](provider-api-consent.md) for the rules on getting that evidence.
