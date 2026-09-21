# Changelog

## 2026-09-21

### Added

- Initial release of the Linear capture connector, with `issues`, `projects`,
  `initiatives` and `labels` streams.

### Known limitations

- Archival and deletion of Projects, Initiatives and Labels are not captured. Linear's API
  exposes no `archivedAt` filter on those types and archiving a record does not advance its
  `updatedAt`, so an archived record remains in the destination indefinitely with a null
  `archivedAt`. Issues are unaffected: they are swept on a second `archivedAt` cursor.
- `identifier` is not captured for Projects or Initiatives. The field is gated behind
  Linear's paid "Project IDs" / "Initiative IDs" add-ons, and requesting it would emit an
  error on every page for workspaces without them.
- Initiatives require a paid Linear plan; the stream is discovered but stays empty on
  workspaces without the entitlement.
