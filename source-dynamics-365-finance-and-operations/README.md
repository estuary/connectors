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

The schema can evolve over time as tables are modified in Dynamics 365, so different timestamp folders may have different schemas for the same table.

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
          "dataType": "string|int64|boolean|dateTime|decimal|guid"
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

### Standard Columns
Every CSV file includes these system-generated metadata columns:
- `Id` - Unique identifier for the row
- `IsDelete` - Boolean flag set to `true` for deletions, absent for inserts/updates.

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
