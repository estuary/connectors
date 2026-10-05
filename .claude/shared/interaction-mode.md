# Interaction mode: human-in-the-loop vs. autonomous

The connector skills pause to ask the user at a handful of points — a stream list that's vague, an auth scheme to pick, a draft plan to review, a seeding request to run. Two kinds of user sit on the other side of those pauses, and they want opposite things from them:

- **`human-in-the-loop`** — a developer who wants to be consulted. Every gate is a real conversation: show the material, wait for their call.
- **`autonomous`** — someone who wants the connector built without supervision. They want to be interrupted **once**, at the credential checkpoint, and otherwise trust the skills to make the safe choice and tell them afterwards what was decided.

The phases are identical in both modes; only how each gate _resolves_ differs. Every gate has an ID (`GATE-*`). The table below is the index of them; where a skill owns a gate, that skill holds the reasoning and the table row just names the outcome.

## Resolving the mode

The orchestrating skill opens every run with the questionnaire below, for every connector. `CONDUCT-CONSENT-PER-CONNECTOR`: no answer outlives the run — not the mode, not the seeding answer, not the credentials or a decision to go without them. They are answers about one connector and one provider account; a user who let you seed a Mailchimp sandbox has said nothing about their Okta tenant. The one shortcut is an answer already in the prompt: `--autonomous` / `--human-in-the-loop` after the connector name, or plain language that clearly means one of them ("don't ask me anything", "just build it and tell me what you did" → `autonomous`; "check with me before …" → `human-in-the-loop`). That answers the mode question only. The seeding question is always asked, because it is the only thing that ever lets you mutate the user's account.

`CONDUCT-PERMISSIONS-FILE`: record the answers with `python3 .claude/scripts/permissions.py grant source-<name> --mode <m> --seeding <s>`. That writes `.claude/permissions/source-<name>.json` (gitignored), which the repo's `PreToolUse` hook (`.claude/hooks/permission-gate.py`) reads before any live provider call, so `API-MUTATE-ONLY-WITH-CONSENT` and `API-CONFIG-GATE` hold mechanically in every session, including background ones. After the credential checkpoint, `permissions.py stamp` binds the permission to the committed `config.yaml`; `permissions.py revoke` deletes it at hand-off. Only the orchestrating skill runs these, only from the user's live answers.

### The questionnaire

First-time users don't know what the run involves, so the mode question can't stand alone. The script is fixed so every user gets the same picture; don't paraphrase or trim it, only substitute the connector name. One `AskUserQuestion` carries both questions: the mode (_Autonomous_ first, _Interactive_ second) and who runs seeding requests (_I run them_, _You run them_, _Don't seed_ in that order — recorded as `seeding: assistant|user|none`; _Don't seed_ when unanswered).

> **Before we start.** The fastest route to a finished `source-<name>` is to let me work on my own.
>
> 1. I read the provider's API documentation, build the connector's skeleton and wire up the simplest form of authentication it offers — usually an API key.
> 2. I stop and you get a single message: which credentials go in `config.yaml`, how to encrypt them, and the few decisions I have taken so far, in case you want to change any. Credentials are optional: I can build from the documentation alone. But live data makes the result better in several ways: the connector is built on what the API actually returns, not on what the documentation says it should. Note that the API responses I read pass through Anthropic and are subject to its data-retention policy.
> 3. Then I finish the job. I implement the streams, run the test suite, write the changelog, documentation page and CI entry.
> 4. You get three things: a working connector; a Bruno collection under `source-<name>/bruno/` holding every API request I ran, with its response and what it proved, so you can rerun any probe in the Bruno app; and a decision ledger: every choice I made, and why.
>
> Would you rather stay hands-on? Choose **human-in-the-loop**. I will stop at each decision — authentication, replication strategy, draft plans, seeding of test data — show you the material and wait for your answer. This is the better choice if you know the API's internals, or care about its rate limits and the connector's performance: those are the calls where your judgment beats my safe default.
>
> **One more question.** Some endpoints may be empty in your account, and some checks need a record to be created or changed. Who should run those requests? **I run them** — only against a sandbox, and only the ones I file under `bruno/Seeding/`; **you run them**, when I hand them over; or **don't seed**, leaving those endpoints unverified. If you do not answer, I assume don't seed.
>
> I will ask this at the start of every connector; nothing is remembered between runs. Say "switch to human-in-the-loop" or "switch to autonomous" at any time to change it mid-run, or pass `--human-in-the-loop` or `--autonomous` with the connector name to skip the question.

`CONDUCT-FORWARD-MODE`: every dispatch you make — `Agent` prompts and `claude --bg` briefs alike — carries the resolved mode as a line `interaction mode: <human-in-the-loop|autonomous>` plus the seeding answer. Detached sessions can't see this conversation, and a sub-session that re-ran the questionnaire would interrupt a user who chose not to be interrupted. A dispatched agent that receives no mode line asks its dispatcher, not the user.

## Autonomous mode is not "fewer checks"

Removing the human from the gates removes the review that catches wrong plans. Autonomous mode buys that back with **more machine verification, not less**: a reviewer subagent grades every plan against [`rules-index.md`](rules-index.md) before it's integrated, the final diff gets a review pass, and every gate resolved without a human is written to a **decision ledger** the user reads afterwards. When a gate has a fast option and a correct-but-slower option, autonomous mode always takes the correct one — the user isn't there to accept a shortcut's risk, so nobody gets to accept it on their behalf.

## Gate table

Outcomes only; the owner named in each row holds the reasoning.

| Gate | `human-in-the-loop` | `autonomous` |
| ---- | ------------- | ------------ |
| `GATE-STREAM-LIST` — no or vague stream list (`create-capture-connector` Phase 0) | Ask. | Every resource the provider documents as listable; shown at the checkpoint. |
| `GATE-AUTH-SCHEME` — `configure-auth` Phase 2 | Confirm. | Simplest static scheme, one arm. |
| `GATE-CREDENTIALS` — `config.yaml` needs real values (`create-capture-connector` Phase 4) | Stop, hand off. | **The one checkpoint**, batched (below). "No credentials" → docs-only run. |
| `GATE-TIGHT-BUDGET` — a required endpoint allows ≤ 20 req/hr (`API-BUDGET-20RPH`) | Ask before each live run. | Declare the budget at the checkpoint and stay inside it; restore `disable: true` bindings before hand-off. |
| `GATE-STREAM-DESIGN` — `add-stream` Phase 3 | Confirm endpoint, grain, classification. | Proceed; ledger. Already settled when a reviewed plan supplies the design. |
| `GATE-INCREMENTAL-ONLY` — `classify-stream-types` | Confirm. | Never. |
| `GATE-STRATEGY-UNCLEAR` — `classify-stream-types` | Ask. | Decide on size and cursor; ledger as open. |
| `GATE-OPAQUE-CURSOR` — `bruno-probe-endpoint`, token stability undocumented | Surface as a blocker. | Reject the opaque cursor; derived cursor or snapshot. |
| `GATE-PARTITION-COVERAGE` — `bruno-probe-endpoint`, a default filter hides a partition | Decide together. | Sentinel sweep if offered, else sequential sweeps. |
| `GATE-PLAN-REVIEW` — `stream-builder` draft plan | Human review, inline. | Self-review; orchestrator's reviewer before integration. |
| `GATE-SEEDING` — `bruno-probe-endpoint` Phase 6 | Per the seeding answer. | `assistant` → run; otherwise PENDING. |
| `GATE-CONFIG-DIRTY` — the hook refused a live run (`API-CONFIG-GATE`) | Re-confirm, then `permissions.py stamp`. | No live calls until hand-off; PENDING; ledger. |
| `GATE-COMMIT-SPLIT` — connector diff mixed with an unrelated sweep | Recommend. | Perform. Push and PR always need a human (`CONDUCT-CONFIRM-BEFORE-WRITE-HISTORY`). |

## The batched checkpoint (`GATE-CREDENTIALS` in autonomous mode)

The credential stop is unavoidable — the user has to produce secrets you can't. So make it carry everything, in one message, so no second interruption is needed. Like the questionnaire, it's a fixed script: render it verbatim, filling the `<slots>`. Slot 3 is where every decision taken autonomously before this point is surfaced (`GATE-STREAM-LIST`, `GATE-AUTH-SCHEME`, `GATE-TIGHT-BUDGET`), and it restates the questionnaire's seeding answer so the user can change it while they are here.

> **One thing only you can do.** I have built the skeleton of `source-<name>` and set up <scheme> authentication. To go further I need real credentials.
>
> 1. **Credentials.** Put these values in `source-<name>/config.yaml`: <fields>. Then encrypt the file: `<sops command>`. Reply when it is done.
> 2. **Why it matters.** You may skip this: building from documentation alone is a supported mode. But with credentials I test every stream against the live API and the snapshot tests run on real data, which improves the result in several ways: the connector is built on what the API actually returns, not on what the documentation says it should. Without them the plans rest on documentation alone, nothing is verified, and your first production run is the first real test. Be aware that the API responses I read pass through Anthropic and are subject to its data-retention policy. Reply "no credentials" to continue without them.
> 3. **Decisions so far**: streams <list>; authentication <scheme> (the provider also offers <other schemes>, if you hold those credentials instead); API budget <plan>; seeding <assistant / user / none, as you answered — who runs seeding requests>. Say so if you want any of them changed.
>
> After this I will not ask again until the connector is finished.

Then wait. When they confirm — or decline credentials — don't ask anything else until the hand-off.

## Decision ledger

Every autonomous gate resolution is recorded as one line — `<GATE-ID> · <what was decided> · <why, incl. the alternative rejected>` — and the collection is rendered as `## Decision ledger` in the final hand-off summary. Stream-builders keep theirs under `## Decisions made without review` in `plan.md`; the orchestrator folds those in. The ledger is what makes autonomous mode auditable: the user didn't watch the run, so the summary is where they find out a stream became a snapshot or an endpoint stayed unverified.
