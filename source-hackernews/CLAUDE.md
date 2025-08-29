# CLAUDE.md — HackerNews Capture Connector

## Tech Stack

* **Language**: Go (Estuary connector standard)
* **Framework**: Estuary Flow Source Connector SDK
* **API**: HackerNews Firebase REST API (`https://hacker-news.firebaseio.com/v0/`)
* **Data Format**: JSON (nested structures supported)

---

## Connector Purpose

This connector ingests **HackerNews data** into Estuary Flow in near real time. It fetches items (stories, comments, jobs, polls), user profiles, and live updates, exposing them as Flow collections for downstream processing.

---

## API Endpoints

* **Items**

  * `/item/<id>.json` → Stories, comments, jobs, polls, pollopts
* **Users**

  * `/user/<id>.json` → User profiles and submitted content
* **Live Data**

  * `/maxitem.json` → Highest current item ID
  * `/topstories.json`, `/newstories.json`, `/beststories.json`
  * `/askstories.json`, `/showstories.json`, `/jobstories.json`
  * `/updates.json` → Changed items and profiles

> No rate limiting is currently enforced.

---

## Flow Collections

* `hackernews/items` → Generic collection for all item types (`story`, `comment`, `job`, `poll`, `pollopt`)
* `hackernews/users` → User metadata and submissions
* `hackernews/topstories`, `hackernews/newstories`, etc. → Lists of story IDs by category
* `hackernews/updates` → Change notifications for items and profiles

---

## Data Model

### Items

All HackerNews entities are modeled as *items*. Common fields:

* `id: int` (required, primary key)
* `type: string` (`story`, `comment`, `job`, `poll`, `pollopt`)
* `by: string` (author)
* `time: int` (Unix timestamp)
* `text: string?` (HTML body, nullable)
* `title: string?`
* `url: string?`
* `score: int?`
* `parent: int?` (for comments)
* `kids: array<int>?` (child comments or pollopts)
* `parts: array<int>?` (poll options)
* `descendants: int?`

### Users

* `id: string` (username, primary key)
* `created: int`
* `karma: int`
* `about: string?` (HTML bio)
* `submitted: array<int>`

---

## Connector Design

1. **Initialization**

   * Fetch `/maxitem.json` at startup for sync boundary.
   * Optionally backfill older items by walking backward from max ID.

2. **Ingestion Strategy**

   * Poll `/updates.json` for live changes.
   * Resolve changed IDs via `/item/<id>.json` or `/user/<id>.json`.
   * Ingest stories from category endpoints (`/topstories`, `/newstories`, etc.) periodically for consistency.

3. **Deduplication**

   * Use `id` as primary key across items.
   * Updates overwrite existing documents.

4. **Schema Evolution**

   * Allow additional fields (per HN docs: clients should ignore unexpected keys).
   * Mark nullable fields accordingly.

---

## Code Style & Conventions

* Use **snake\_case** for collection names and fields.
* Normalize all **timestamps to RFC3339** when emitting into Flow.
* Preserve HackerNews HTML fields as-is; do not sanitize or transform.
* Always check for **nullability** (`text`, `title`, `url` may be missing).

---

## Do Not Section

* Do not hardcode max item IDs; always query `/maxitem.json`.
* Do not flatten nested structures (e.g., `kids`, `parts`) into separate collections unless explicitly configured by user.
* Do not assume immutability of items — updates can mark items as `deleted` or `dead`.

---

## Terminology

* **Item**: Any entity in HN (story, comment, job, poll, pollopt).
* **Kids**: Child IDs of an item (usually comments).
* **Parts**: Options belonging to a poll.
* **Updates**: Incremental feed of changed item/user IDs.
