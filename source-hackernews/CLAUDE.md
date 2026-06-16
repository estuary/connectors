# source-hackernews

Estuary capture connector for the [HackerNews API](https://github.com/HackerNews/API), built on the Python `estuary-cdk`.

The HackerNews API is a public, unauthenticated Firebase REST API at
`https://hacker-news.firebaseio.com/v0/`. There is no rate limit and no
credentials, so `EndpointConfig` is empty.

## Streams

| Stream | Strategy | Key | Notes |
| --- | --- | --- | --- |
| `items` | backfill (walk IDs 1→maxitem) + incremental (`/updates.json`) | `/id` | Stories, comments, jobs, polls, pollopts. `parent`/`kids` reconstruct the comment tree; deleted/dead items are retained. |
| `users` | incremental (`/updates.json` profiles) | `/id` | Profiles of users with recent public activity. No backfill — see Limitations. |
| `updates` | polling (`/updates.json`) | `/_meta/row_id` | Append-only log of each change-feed payload (changed item IDs + profiles). |
| `topstories`, `newstories`, `beststories`, `askstories`, `showstories`, `jobstories` | polling | `/_meta/row_id` | Append-only ranking snapshots; a story's rank is its index in `story_ids`. Not derivable from items. |

## Layout

- `source_hackernews/__init__.py` — `Connector` (extends `BaseCaptureConnector`).
- `source_hackernews/models.py` — Pydantic document and config models.
- `source_hackernews/api.py` — pure HTTP functions (single item/user, concurrent batch, ranking lists).
- `source_hackernews/resources.py` — binds models + API into `Resource`s.

## Design notes

- **Incremental is updates-driven.** `/updates.json` reports items and profiles whose
  content changed; every reported entity is emitted, because an item's `time` is its
  immutable creation time and can't detect new comments / score / karma changes. The
  log cursor simply advances to "now".
- **Concurrency.** Items and users are fetched concurrently via `_fetch_concurrent`
  (semaphore-bounded, order-preserving via `estuary_cdk.buffer_ordered`).
- **Retries.** Handled by the CDK's `HTTPSession`. A literal `null` body (deleted or
  missing entity) is treated as "not found" and skipped.
- **First incremental poll** is delayed by the binding `interval` (the CDK waits until
  the cursor is `interval` old), so lower-interval streams surface fastest in a preview.

## Limitations

- **Users have no backfill.** The API has no "list all users" endpoint, so only users
  appearing in `/updates.json` are captured. Authorship is still captured via each
  item's `by` field; complete profiles for every author would require a derived capture
  seeded from item authors.

## Develop and test

```bash
cd source-hackernews
poetry env use python3.12
poetry install
poetry run pytest                 # spec / discover / capture snapshots
poetry run pytest --insta=update  # refresh snapshots
```

Preview against the live API (the venv's `python` must be on `PATH` so the `local`
command resolves):

```bash
source "$(poetry env info --path)/bin/activate"
flowctl preview --source test.flow.yaml --sessions=-1 | head
```

Incremental and ranking streams poll on their `interval`, and the first poll waits a
full interval, so to see them quickly in a preview use a temporary flow with short
intervals (e.g. `PT5S`).
