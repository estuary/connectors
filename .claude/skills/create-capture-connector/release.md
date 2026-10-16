# Release requirements for a new connector

Reference for `create-capture-connector` Phase 8. Every new connector ships with all three; `source-zuora`'s introduction is the reference shape.

## CI registration

Add an entry to `.github/python-connectors.yaml`, the list `.github/workflows/python.yaml` builds its matrix from (`name`, `type: capture`, `version` equal to the connector's `VERSION` file, `usage_rate: "1.0"`). A connector missing from the list is silently skipped by CI.

## CHANGELOG.md

Create `source-$1/CHANGELOG.md`:

```markdown
# Changelog

## <today, UTC>

### Added
- Initial release of the <Provider> capture connector.
```

Convention: [CONTRIBUTING.md](../../../CONTRIBUTING.md#changelog-entries). Creating the file is this skill's job; the `changelog` skill refuses to add one to a connector that lacks it, because that opt-in happens here.

## Docs page

`docs/reference/Connectors/capture-connectors/<provider>.md`, with `-native` appended when a legacy connector owns the plain name. Boilerplate reference: `iterable-native.md`; change only where the provider's facts differ:

- `description:` frontmatter;
- supported-resources table with replication modes, each resource linked to its API reference page, URLs taken from the `**REFERENCE:**` lines in `source-$1/bruno/`;
- a `:::tip` for scheduled-backfill or cursor caveats;
- prerequisites;
- endpoint and bindings property tables, with the `credentials_title` discriminator row when auth is a union;
- a sample capture spec.
