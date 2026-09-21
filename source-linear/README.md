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
  Three of the four root fields also accept `sort`, which allows an ascending walk;
  `issueLabels` rejects it. Windows are therefore `(cursor, horizon]` over a fully-elapsed
  tick, which keeps the exclusive lower bound safe in either direction.
- **Archiving does not advance `updatedAt`.** Only `IssueFilter` exposes an `archivedAt`
  comparator, so Issues gets a second cursored pass and the other three streams cannot
  observe archival at all.

`bruno/` is the evidence record: account-wide constraints in `collection.bru`, per-endpoint
constraints on the request that proves them.
