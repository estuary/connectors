---
name: regenerate-flow-discovery
description: Rebuild a connector's generated schemas directory and `test.flow.yaml` bindings via `flowctl raw discover`, then refresh pytest snapshots. Destructive — deletes the generated dir and rewrites the test config. Use after adding or removing a stream.
argument-hint: "[connector-name]"
allowed-tools: Agent
---

This work runs in an **isolated context on Haiku**, not inline. Do not perform the wipe/discover/snapshot steps here — the procedure lives in the `regenerate-flow-discovery` **agent** so it doesn't consume the main session's context or model budget.

Dispatch that agent (via the Agent tool, `subagent_type: regenerate-flow-discovery`) with a prompt that supplies:

1. The **connector name** — `source-$1` if an argument was given, otherwise the connector for the current working directory.
2. Whether the provider's **rate-limit budget is already cleared** (every required endpoint allows > 20 requests/hour). If the caller — e.g. the `add-stream` skill — already confirmed this, say so, so the agent runs the snapshot tests freely. Otherwise tell it consent is not cleared and it should ask before any API-hitting run.

When the agent returns, relay its report to the user: the `git diff --stat` summary, whether the new stream's schema file is present, whether the discover/capture snapshots now contain the new stream, and any CDK schema sweep it recommends splitting into its own commit.
