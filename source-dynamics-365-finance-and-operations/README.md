# Dynamics 365 Finance and Operations Data Structure in ADLS Gen2

This document outlines our understanding of how Dynamics 365 Finance and Operations data is structured and organized within Azure Data Lake Storage Gen2 (ADLS Gen2) when using Azure Synapse Link.

## Overview

When Azure Synapse Link is configured for Dynamics 365 Finance and Operations with **CSV export format**, the system exports incremental data changes to ADLS Gen2 in a structured format. The data is organized using time-stamped folders containing CSV files with change data for each table, along with metadata files that describe the schema at different points in time.

**Note**: Azure Synapse Link supports multiple export formats (CSV, Parquet Delta Lake). This connector is designed for the CSV export option.

## Filesystem Structure

### Root Container
Data is stored in a container named: `dataverse-[environmentName]-[organizationUniqueName]`

### Directory Structure
```
/
├── model.json                   # Global schema metadata. Contains the most recent metadata for all tables.
├── Changelog/
│   └── changelog.info           # Contains the current in-progress timestamp folder's name.
└── [timestamp-folders]/         # Time-stamped incremental update folders. Each folder contains changes that occurred within a specific time interval.
    ├── model.json               # Schema metadata snapshot for tables in this folder. May differ from global schema if schema changes occurred. Empty while folder is being written to.
    ├── [TableName1]/            # The name of the Dynamics 365 table that was changed.
    │   ├── xxx.csv              # A headerless CSV containing changes made to the table. Multiple CSVs may exist per table.
    │   └── yyy.csv
    └── [TableName2]/
        └── zzz.csv
```

### Timestamp Folders

#### Naming Convention
- Format: `yyyy-MM-ddTHH:mm:ss.SSSz` (UTC timestamps)
- Examples: `2025-01-15T14:30:00.000Z`, `2025-01-15T15:00:00.000Z`
- Created at configurable intervals (minimum 5 minutes, maximum 24 hours)

#### Data Organization
- **Time Boundaries**: Each folder captures changes that occurred within its time interval
- **Selective Creation**: Only tables with changes get folders/files created within the timestamp folder
- **Active Folder Tracking**: The currently being written folder name is stored in `Changelog/changelog.info`
- **Retention Policy**: Historical folders may be automatically purged based on retention settings

## `model.json` Files

### Location and Purpose
Schema metadata is provided through `model.json` files at two levels:
- **Root Level**: Contains the current global schema for all tables, used for discovering available tables
- **Timestamp Folder Level**: Contains schema metadata as it existed during that time period, allowing for schema evolution tracking

The schema can evolve over time as tables are modified in Dynamics 365, so different timestamp folders may have different schemas for the same table. A folder level `model.json` is written when the folder finalizes, so it can also describe columns that rows written earlier in that same folder do not have - see [Schema Evolution Within a Folder](#schema-evolution-within-a-folder).

### General structure
```json
{
  "name": "dataset_name",
  "description": "description",
  "version": "version_string",
  "entities": [
    {
      "$type": "entity_type",
      "name": "TableName",
      "description": "Table description",
      "attributes": [
        {
          "name": "ColumnName",
          "dataType": "string|int64|boolean|dateTime|decimal|guid|dateTimeOffset"
        }
      ]
    }
  ]
}
```

## CSV File Structure

### Naming Convention
CSV files are located in subdirectories named after the table: `[timestamp-folder]/[TableName]/[filename].csv`
- Multiple CSV files may exist per table within a timestamp folder.
- Files contain no column headers. Column definitions are found only in the corresponding `model.json`.
- A file is appended to over the course of the folder's interval rather than written all at once. In one folder we observed `2026.csv` created before `2025.csv` but modified after it.

### Column Order

Cells are bound to column names by position, following the order in which `model.json` lists the table's attributes. Every table observed lays its rows out the same way:

```
Id, SinkCreatedOn, SinkModifiedOn, [table's own columns...], versionnumber, createdon, modifiedon, IsDelete, [appended columns...]
```

- `Id` - Unique identifier for the row.
- `SinkCreatedOn` / `SinkModifiedOn` - When the change was written to the data lake. These use a non-ISO format, e.g. `4/14/2026 8:21:50 PM`, unlike the table's own `dateTime` columns.
- `versionnumber` - Commit sequence number from the source system, and the ordering primitive for repeated changes to the same `Id`. Within one `Id` it never decreases down a file, though it can repeat, which is why `SinkModifiedOn` serves as a tiebreaker. `SinkModifiedOn` is what increases down a file regardless of `Id`, since it reflects the order Synapse Link wrote the rows, and that is the ordering the column-count invariant below relies on.
- `IsDelete` - Set to `True` for deletions and left empty otherwise. The column itself is always present.

`IsDelete` terminates this standard block.

### Schema Evolution Within a Folder

Columns added to a table in Dynamics 365 after its initial export are appended after `IsDelete`, so an entity's attribute list is effectively ordered by when each column was added.

Because a CSV is appended to across the folder's interval, a file can be written across one of these additions. **Rows within a single file can therefore have different widths**: rows written before the addition lack the new trailing columns, rows written after have them. The folder's `model.json` is finalized at the end of the interval and records only the post-addition schema, so it can describe more columns than some rows in that same folder contain.

### How the Connector Decides a Row Is Safe to Read

Since CSVs are headerless, a row carries no evidence of which column any given cell belongs to. The connector cannot tell a harmlessly absent trailing column from a column dropped mid-row, which would shift every subsequent value into the wrong field. Misalignment can only be inferred from signals a correctly aligned row would not produce:

1. **Width, per row.** A row may be narrower than `model.json` declares, but only down to `IsDelete` - making it a prefix of the declared schema, with the appended columns left absent. A row that stops before `IsDelete`, or has more cells than `model.json` names, fails the capture. See `api.bind_row`.
2. **Width, per file.** Row widths within a file may only increase, since rows are appended in the order Synapse Link writes them and a column is added once. A row narrower than one already seen therefore did not come from an append - it was truncated, most likely by an interrupted export, or a column was removed from the table partway through the file. See `api.read_csv_rows`.
3. **The append-only property itself.** Each folder's schema for a table must begin with the whole of the last one seen for it. An append satisfies that; an insertion, reorder or removal does not, since all three move a column to a different position. See `api.TableSchemaHistory`.

Checks 1 and 2 are width signals, which is all a headerless row offers, and both are only sound while columns are appended rather than inserted. Check 3 watches that premise directly rather than trying to infer a misaligned row from its contents, and withdraws permission to bind narrow rows once it stops holding. It costs no extra requests. The connector already walks a table's folders in order and builds each one's schema, so the previous schema is simply the last one it built.

A schema change that breaks the property is only *fatal* where the property is relied upon - binding a row narrower than the schema. A rename breaks the prefix relation while shifting nothing, so full-width rows still bind correctly and continue to be captured; the change is logged as a warning instead. Only a narrow row arriving after the property broke raises.

Two limits are worth stating. The check only observes schema changes that occur while the connector is running, so a table that stopped being append-only before the current cursor is already reflected in both schemas being compared and goes unnoticed. And the first folder of a sweep after a restart has no predecessor in memory, so it is unchecked.

A row that fails any of these stops that table's capture with an error naming the folder, file and row number. There is no skip path, unlike a folder missing its `model.json`: the same folder is re-read on the next attempt, so the failure persists until the underlying data or the connector changes. That is deliberate - the alternative is writing values whose column is unknown.

## Change Data Capture Mechanics

### How Changes Are Detected
Dynamics 365 Finance and Operations uses internal change tracking mechanisms to identify modifications to table data:
- **Insert/Update Operations**: New or modified records are captured with their current field values
- **Delete Operations**: Deleted records are captured with `IsDelete=true` and the record's ID
- **Soft Deletes**: The system uses logical deletes rather than physical removal, allowing change consumers to handle deletions appropriately

### Timestamp Folder Creation
- Folders are created at the configured export interval (5 minutes to 24 hours)
- Each folder captures all changes that occurred within its time window

## References

- [Export to Azure Data Lake overview](https://learn.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/data-entities/azure-data-lake-ga-version-overview)
- [Choosing finance & operations data in Azure Synapse Link for Dataverse](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data)
- [Incremental folder structure](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-incremental-updates)
- [ADLS Gen2 REST APIs documentation](https://learn.microsoft.com/en-us/rest/api/storageservices/data-lake-storage-gen2)
