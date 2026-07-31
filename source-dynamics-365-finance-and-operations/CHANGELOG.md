# Changelog

## 2026-07-31

### Fixed
- Captures no longer fail when Synapse Link appends a column to a table partway through writing a CSV. Rows written before the append are narrower than the folder's `model.json` describes, and a single CSV can hold rows of both widths. The appended columns are now left absent from the older rows rather than failing the capture. Rows whose columns cannot be aligned for any other reason still fail. A row is rejected when it stops before `IsDelete`, when it has more columns than the schema names, or when it is narrower than an earlier row in the same file, which indicates an interrupted export truncated it rather than that it predates an appended column. Errors now identify the CSV and the row number within it.

### Added
- The connector now checks that a table's columns are only ever appended to, by requiring each folder's schema to begin with the whole of the previously seen one. Reading a row narrower than its schema depends on that being true, so a table whose columns are instead inserted, reordered or removed no longer has its narrower rows read, since their values could land under the wrong column names. Full width rows are unaffected and continue to be captured; a column rename, which changes no positions, is logged rather than treated as an error.

## 2026-07-17

### Fixed
- Captures no longer crash when a timestamp folder's `model.json` is present but does not list an entity for a given binding. The connector now falls back to the per-table `model.json` that Synapse Link writes under `Microsoft.Athena.TrickleFeedService/`.
- CSV rows whose column count doesn't match their table's schema now fail with a clear error instead of being silently padded or truncated, guarding against misaligned reads from a fallback per-table `model.json`.

## 2026-07-16

### Fixed
- Captures no longer crash when an older timestamp folder is missing its `model.json`. Such folders come from Synapse Link exports that never finalized; they are now skipped once enough time has passed for finalization, and their data is picked up from the later folder Synapse Link re-commits it to.
