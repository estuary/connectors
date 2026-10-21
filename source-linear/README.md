# source-linear

Captures issues, projects, initiatives, and labels from [Linear](https://linear.app) via its
GraphQL API.

**User documentation, including configuration and the archival limitations, lives at
[`docs/reference/Connectors/capture-connectors/linear.md`](../docs/reference/Connectors/capture-connectors/linear.md).**
Keep that page as the source of truth; this file covers development only.

## Development

```bash
poetry install
poetry run pytest                  # snapshot tests
poetry run pytest --insta=update   # refresh snapshots
poetry run mypy source_linear/
```

`config.yaml` is sops-encrypted with the repo's GCP KMS key and holds a working key for the
`estuary-test` workspace.

## Design notes

Two API behaviors drive the implementation, neither documented by Linear — both were
established by probe, and the requests that prove them are in `bruno/`:

- **`orderBy` is descending-only**, and its enum members carry null schema descriptions.
  The three incremental root fields also accept `sort`, which allows an ascending walk.
  Windows are `(cursor, horizon]` over a fully-elapsed tick.
- **Applying a label does not advance the label's `updatedAt`**, only its `lastAppliedAt`.
  `labels` is therefore a full snapshot each interval rather than an `updatedAt` cursor,
  which also lets it observe archival and deletion. `start_date` does not apply to it.
- **Archiving does not advance `updatedAt`.** Only `IssueFilter` exposes an `archivedAt`
  comparator, so Issues gets a second cursored pass; Projects and Initiatives cannot
  observe archival at all.

`bruno/` is the evidence record: account-wide constraints in `collection.bru`, per-endpoint
constraints on the request that proves them.
