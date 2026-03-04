# Asana Source Connector

An Estuary Flow source connector for Asana.

## Captured Resources

| Resource         | Type        | Scope         | Description                            |
| ---------------- | ----------- | ------------- | -------------------------------------- |
| Workspaces       | Snapshot    | Global        | All workspaces                         |
| Users            | Snapshot    | Per-workspace | Deduplicated across workspaces         |
| Teams            | Snapshot    | Per-workspace | Organization workspaces only           |
| Projects         | Snapshot    | Per-workspace | Active (non-archived) projects         |
| Tasks            | Incremental | Per-project   | Sync-token events + backfill           |
| Sections         | Snapshot    | Per-project   | Project sections                       |
| Tags             | Snapshot    | Per-workspace | All tags                               |
| Portfolios       | Snapshot    | Per-workspace | Portfolios owned by authenticated user |
| Goals            | Snapshot    | Per-workspace | All goals                              |
| CustomFields     | Snapshot    | Per-workspace | Custom field definitions               |
| TimePeriods      | Snapshot    | Per-workspace | Time period definitions                |
| ProjectTemplates | Snapshot    | Per-workspace | Project templates                      |
| StatusUpdates    | Snapshot    | Per-project   | Project status updates                 |
| Attachments      | Snapshot    | Per-project   | Project attachments                    |
| Stories          | Snapshot    | Per-task      | Task comments and activity             |
| Memberships      | Snapshot    | Per-project   | Project memberships                    |
| TeamMemberships  | Snapshot    | Per-team      | Team memberships                       |

## Setup

### Prerequisites

- Python 3.12+
- Poetry
- flowctl (`brew install estuary/tap/flowctl`)

### Installation

1. **Install dependencies:**

   ```bash
   poetry install
   ```

2. **Configure credentials:**
   ```bash
   # Edit config.yaml with your Asana personal access token (gitignored)
   ```

## Development

### Run tests

```bash
# Unit tests (no credentials needed)
poetry run pytest tests/test_connector.py -v

# API connectivity tests (requires config.yaml)
poetry run pytest tests/test_api.py -v

# All tests
poetry run pytest -v
```

### Test with flowctl

```bash
# Test spec output
flowctl raw spec --source test.flow.yaml

# Test discover
flowctl raw discover --source test.flow.yaml -o json --emit-raw

# Preview capture
flowctl preview --source test.flow.yaml --sessions 1 --delay 15s
```

## Design Documentation

```mermaid
---
config:
  theme: redux-dark
---
graph TD
      subgraph CursorDict["Cursor Dict Structure"]
          direction LR
          CD["cursor_dict:<br/>{project_gid_1: sync_token_1,<br/> project_gid_2:
  sync_token_2,<br/> ...}"]
          CDNote["CDK manages each key as an<br/>independent concurrent subtask"]
          CD --- CDNote
      end
      subgraph Bootstrap["Bootstrap Phase"]
          direction TB
          B1["Fetch list of projects<br/>GET /projects"]
          B2["For each project_gid"]
          B3["GET /projects/{project_gid}/events<br/>(no sync token)"]
          B4["Asana returns 412<br/>Precondition Failed"]
          B5["Extract initial sync_token<br/>from 412 response body"]
          B6["Store in cursor dict:<br/>cursor_dict[project_gid] = sync_token"]
          B7["FetchPageFn runs concurrently:<br/>full paginated backfill of<br/>tasks for this
  project"]

          B1 --> B2
          B2 --> B3
          B3 --> B4
          B4 --> B5
          B5 --> B6
          B6 --> B7
      end
      subgraph Incremental["Incremental Phase"]
          direction TB
          I1["CDK spawns concurrent subtasks<br/>(one per project_gid in cursor dict)"]
          I2["GET /projects/{project_gid}/events<br/>?sync={token}"]
          I3{{"Asana returns 200?"}}
          I4["Receive events +<br/>has_more flag +<br/>new sync_token"]
          I5{"has_more?"}
          I6["Loop: call again<br/>with new token"]
          I7["Collect changed task GIDs<br/>from events, deduplicate"]
          I8{"Action type?"}
          I9["'deleted'<br/>Yield tombstone"]
          I10["'created' / 'changed' /<br/>'added' / 'removed'"]
          I11["Batch re-fetch full task docs<br/>GET /tasks/{gid}"]
          I12["Yield full documents"]
          I13["Yield new sync_token<br/>as updated cursor"]

          I1 --> I2
          I2 --> I3
          I3 -- "Yes (200)" --> I4
          I4 --> I5
          I5 -- "true" --> I6
          I6 --> I2
          I5 -- "false" --> I7
          I7 --> I8
          I8 -- "deleted" --> I9
          I8 -- "created / changed /<br/>added / removed" --> I10
          I10 --> I11
          I11 --> I12
          I9 --> I13
          I12 --> I13
      end
      subgraph Expiry["Token Expiry / Re-Backfill"]
          direction TB
          E1["Asana returns 412<br/>instead of 200"]
          E2["Extract new sync_token<br/>from 412 response body"]
          E3["Store new token in cursor dict:<br/>cursor_dict[project_gid] = new_token"]
          E4["Trigger re-backfill<br/>for that project scope"]

          E1 --> E2
          E2 --> E3
          E3 --> E4
      end
      B7 --> I1
      I3 -- "No (412)" --> E1
      E4 --> I1
      style CursorDict fill:#2d2d3d,stroke:#7c7ce0,color:#e0e0e0
      style Bootstrap fill:#1a2e1a,stroke:#4caf50,color:#e0e0e0
      style Incremental fill:#1a1a2e,stroke:#5c6bc0,color:#e0e0e0
      style Expiry fill:#2e1a1a,stroke:#ef5350,color:#e0e0e0
```
