# Dispatching and monitoring stream-builders

Reference for `create-capture-connector` Phase 5.

## The command

```bash
claude --bg --worktree "source-$1-<cluster>" --name "source-$1-<cluster>" --agent stream-builder "<brief>"
```

- `--worktree` gives each session its own checkout branched from HEAD. That is what lets parallel sessions author `source-$1/bruno/` without colliding, and why the skeleton, auth and `config.yaml` must be committed first (Phase 4).
- `--add-dir` is never passed: it opens a folder-trust dialog the background session cannot answer.
- The command prints the session's short id. Record it next to the cluster name; the handoff directory is `~/.claude/jobs/<short-id>/tmp/`, which the session sees as `$CLAUDE_JOB_DIR/tmp/`.

## The brief

Everything stream-builder's "What you receive" section lists, as plain lines:

- connector `source-$1`, package `$PKG`;
- provider and API base URL;
- the auth scheme `configure-auth` wired;
- the rate-limit budget from Phase 1;
- the cluster's streams and the shared-pattern hypothesis that grouped them;
- `interaction mode: human-in-the-loop|autonomous` and `seeding: assistant|user|none` (`CONDUCT-FORWARD-MODE`);
- in a docs-only run: `credentials: none — plan from docs, mark PENDING`.

## Monitoring

Read `~/.claude/jobs/<short-id>/state.json` for each session on a fixed cadence. Five minutes suits sessions that run thirty to ninety minutes; a tighter loop only burns context.

| Field | Meaning | Your move |
| ----- | ------- | --------- |
| `tempo: blocked` | Waiting on the user in the peek panel (plan-review gate, seeding hand-over). | None. The user sees it in `claude agents`. |
| `state: done` | The session ended a turn. Also what a crash or an idle pause looks like. | Confirm the plan file exists and its final message names it; otherwise treat as failed. |
| `state: failed` | Crashed. | `claude logs <short-id>`, then re-dispatch or ledger the loss. |

Phase 5 ends when every dispatched session is `done` with a finalized plan in its handoff directory, or `failed` and either re-dispatched or written off in the ledger.

## What to say in this chat

Report orchestrator actions: a re-dispatch, a collected plan, an integration step, or a blocker the peek panel cannot show (an infrastructure outage, a hook denial). The user watches the sessions themselves in `claude agents`; human-in-the-loop users answer each session's plan-review gate there, and in autonomous mode there is nothing for them to answer.
