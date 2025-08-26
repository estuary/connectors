# No Flow Document Feature Flag Implementation Plan

## Overview

This document outlines the implementation plan for a new feature flag `no_flow_document` that eliminates the need to persist the `flow_document` column in standard materializations while maintaining full compatibility with the Flow runtime.

## Current Behavior

**Standard Materializations**:
- Store phase: Persist complete JSON document in `flow_document` column
- Load phase: Retrieve `flow_document` from table and return to runtime
- Root document projection gets `FIELD_REQUIRED` constraint

**Problem**: `flow_document` storage creates overhead, especially for wide tables with many individual columns.

## New Behavior with `no_flow_document` Feature Flag

1. **No `flow_document` column** - Root document projection becomes optional
2. **All top-level properties get `LOCATION_REQUIRED` constraints** - Forces materialization of individual columns
3. **Load phase reconstructs JSON** - Dynamically build document from individual columns using original field names

## Implementation Details

### 1. Feature Flag Infrastructure

**Feature Flag Name**: `no_flow_document`
- Default: `false` (disabled for backward compatibility)  
- Integration: Use existing `go/common/feature_flags.go` parsing
- Storage: Add to `materialize-boilerplate/validate.go` `featureFlags map[string]bool`

**Configuration Example**:
```json
{
  "advanced": {
    "feature_flags": "no_flow_document"
  }
}
```

### 2. Store Phase Changes

**No Code Changes Required**

The constraint system automatically handles field selection:
- When `flow_document` has `FIELD_OPTIONAL` constraint and is not selected, no column is created
- Store queries remain unchanged - they already only reference selected fields
- No `flow_document` column means no `flow_document` data is persisted

### 3. Add Root-Level Property Detection

**New Method**: Add `IsRootLevelProjection()` method to projection objects (similar to existing `IsRootDocumentProjection()` and `IsPrimaryKey` methods)

**Location**: Flow protocols package where projection methods are defined

```go
// IsRootLevelProjection returns true if this projection represents a root-level 
// property of the document
func (p *Projection) IsRootLevelProjection() bool {
    return strings.Count(p.Ptr, "/") == 1
}
```

### 4. Add RootLevelColumns Function

**New Function**: Add `RootLevelColumns()` method to Table type (similar to existing `Columns()` method)

**Location**: Where Table type and `Columns()` method are defined

```go
// RootLevelColumns returns only columns that represent root-level properties
func (t *Table) RootLevelColumns() []Column {
    var rootLevelCols []Column
    for _, col := range t.Columns() {
        if col.IsRootLevelProjection() {
            rootLevelCols = append(rootLevelCols, col)
        }
    }
    return rootLevelCols
}
```

### 5. Validation Logic Changes

**File**: `materialize-boilerplate/validate.go`  
**Function**: `validateNewBinding()` (around line 152)

```go
if p.IsRootDocumentProjection() {
    if !v.featureFlags["flow_document"] && !deltaUpdates {
        // When no_flow_document is enabled, root document projection becomes optional
        c = &pm.Response_Validated_Constraint{
            Type:   pm.Response_Validated_Constraint_FIELD_OPTIONAL,
            Reason: "Root document projection is optional when flow_document is disabled",
        }
    } else if sawRoot && !deltaUpdates {
        c = &pm.Response_Validated_Constraint{
            Type:   pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
            Reason: "Only a single root document projection can be materialized for standard updates",
        }
    }
    sawRoot = true
} else if !deltaUpdates && !v.featureFlags["flow_document"] && p.IsRootLevelProjection() {
    // When no_flow_document is enabled, all root-level properties become LOCATION_REQUIRED
    c = &pm.Response_Validated_Constraint{
        Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
        Reason: "All root-level properties are required when flow_document is disabled",
    }
} else if !deltaUpdates && p.Ptr == boundCollection.Key[0] && isOptional(c.Type) {
    // Existing logic for first key component remains unchanged
    c = &pm.Response_Validated_Constraint{
        Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
        Reason: "The first collection key component is required to be included for standard updates",
    }
}
```

**Key Changes**:
- Root document projection: `FIELD_REQUIRED` → `FIELD_OPTIONAL` (when flag enabled)
- Root-level properties: `FIELD_OPTIONAL`/`LOCATION_RECOMMENDED` → `LOCATION_REQUIRED` (when flag enabled)
- Use `p.IsRootLevelProjection()` method consistently

### 6. Load Phase JSON Reconstruction

**Current Load Query Pattern**:
```sql
SELECT {{ $.Binding }}, r.{{$.Document.Identifier}}
FROM temp_load_table AS l  
JOIN target_table AS r ON l.key_columns = r.key_columns
```

**New Load Query Pattern** (when `no_flow_document` enabled):
```sql
SELECT {{ $.Binding }}, 
[DATABASE_SPECIFIC_JSON_CONSTRUCTION]
FROM temp_load_table AS l  
JOIN target_table AS r ON l.key_columns = r.key_columns
```

### 7. Database-Specific JSON Construction

**Critical Principle**: Use original field names (`p.Field`) as JSON keys, database column names (`column.Identifier`) as SQL references.

#### PostgreSQL (`materialize-postgres/sqlgen.go`)
```sql
SELECT {{ $.Binding }}, 
JSON_BUILD_OBJECT(
{{range $i, $col := $.RootLevelColumns}}{{if $i}},{{end}}
    '{{$col.Field}}', r.{{$col.Identifier}}{{end}}
) as reconstructed_document
FROM {{ $.Identifier}} AS r
JOIN flow_temp_table_{{ $.Binding }} AS l ON {{$.JoinCondition}}
```

#### BigQuery (`materialize-bigquery/sqlgen.go`)
```sql
SELECT {{ $.Binding }}, 
TO_JSON_STRING(STRUCT(
{{range $i, $col := $.RootLevelColumns}}{{if $i}}, {{end}}
    r.{{$col.Identifier}} AS {{$col.Field}}{{end}}
))
FROM {{ $.Identifier }} AS r
JOIN flow_temp_table_{{ $.Binding }} AS l ON {{$.JoinCondition}}
```

#### Snowflake (`materialize-snowflake/sqlgen.go`)
```sql
SELECT {{ $.Binding }}, 
OBJECT_CONSTRUCT(
{{range $i, $col := $.RootLevelColumns}}{{if $i}},{{end}}
    '{{$col.Field}}', r.{{$col.Identifier}}{{end}}
)
FROM {{ $.Identifier }} AS r
JOIN staging_table AS l ON {{$.JoinCondition}}
```

#### SQL Server (`materialize-sqlserver/sqlgen.go`)
```sql
SELECT {{ $.Binding }}, 
JSON_OBJECT(
{{range $i, $col := $.RootLevelColumns}}{{if $i}},{{end}}
    '{{$col.Field}}': r.{{$col.Identifier}}{{end}}
)
FROM {{ $.Identifier }} AS r
JOIN temp_load_table AS l ON {{$.JoinCondition}}
```

### 8. Field Name Mapping

**Example**:
- Original flow document: `{"user-name": "john", "special$field": 123}`
- Database columns: `user_name`, `special_field` (transformed for SQL compatibility)
- Reconstructed JSON: `{"user-name": "john", "special$field": 123}` (original field names preserved)

**Implementation**:
- `{{$field.Field}}` = Original field name from Flow document
- `{{$field.Identifier}}` = Database-safe column name
- JSON keys use `Field`, SQL references use `Identifier`

### 9. Migration and Compatibility

**Constraints**:
- Feature flag only applies to **new materializations**
- Existing materializations with `flow_document` continue unchanged
- **No migration path** - existing materializations cannot enable this flag
- Requires backfill to switch from normal to `no_flow_document` mode

**Validation Rules**:
- Prevent enabling flag on existing materializations that already use `flow_document`
- Add validation in `validateMatchesExistingResource()` to ensure consistency
- Delta-updates materializations unaffected (they don't use `flow_document` anyway)

### 10. Implementation Steps

1. **Add `IsRootLevelProjection()` method to projection interface**
   - Add method to Flow protocols package where other projection methods are defined
   - Implement logic: `strings.Count(p.Ptr, "/") == 1`

2. **Add `RootLevelColumns()` method to Table type**
   - Add method that filters columns using `IsRootLevelProjection()` method
   - Provides clean interface for templates: `{{range $.RootLevelColumns}}`

3. **Add feature flag to validation infrastructure**
   - Modify `materialize-boilerplate/validate.go` to accept and use the flag
   - Update `validateNewBinding()` constraint logic to use `p.IsRootLevelProjection()` method

4. **Modify SQL generation templates** for each connector:
   - Use `{{range $.RootLevelColumns}}` instead of filtering in templates
   - Implement database-specific JSON construction
   - Much cleaner templates without conditional logic

5. **Update connector drivers** to ensure Table types have access to `RootLevelColumns()`:
   - PostgreSQL: `materialize-postgres/driver.go`
   - BigQuery: `materialize-bigquery/transactor.go`
   - Snowflake: `materialize-snowflake/snowflake.go`
   - SQL Server: `materialize-sqlserver/driver.go`
   - MySQL: `materialize-mysql/driver.go`
   - Redshift: `materialize-redshift/driver.go`
   - Databricks: `materialize-databricks/driver.go`

6. **Add comprehensive test coverage**:
   - Unit tests for validation logic
   - Integration tests for each SQL connector
   - Snapshot tests for constraint generation
   - Load/Store cycle tests for document reconstruction

7. **Update documentation**:
   - Feature flag description and caveats
   - Performance implications
   - Migration considerations

### 11. Testing Strategy

**Test Cases**:
1. **New materialization with `no_flow_document`**:
   - Verify root document gets `FIELD_OPTIONAL`
   - Verify root-level properties get `LOCATION_REQUIRED`  
   - Verify no `flow_document` column is created
   - Verify Load phase reconstructs correct JSON

2. **Field name preservation**:
   - Test with special characters in field names
   - Verify reconstructed JSON matches original document structure
   - Test with nested objects (only root-level should be reconstructed)

3. **Backward compatibility**:
   - Existing materializations continue unchanged
   - Cannot enable flag on existing materializations with `flow_document`

4. **`IsRootLevelProjection()` method testing**:
   - Test various pointer path patterns: `/field`, `/nested/field`, ``, `/`
   - Verify correct identification of root-level vs nested properties
   - Test edge cases and boundary conditions

5. **`RootLevelColumns()` method testing**:
   - Verify method correctly filters only root-level columns
   - Test with mixed column types (root-level, nested, keys, document)
   - Ensure proper integration with template system

6. **Error cases**:
   - Missing required root-level fields
   - Invalid feature flag combinations

**Test Files to Update**:
- `materialize-boilerplate/validate_test.go`
- Each connector's driver test file
- Snapshot tests for constraint validation

### 12. Performance Implications

**Benefits**:
- Eliminates `flow_document` storage overhead
- Reduces table width for wide documents
- Potentially better query performance (no large TEXT/JSON column)

**Trade-offs**:
- Load phase requires JSON reconstruction (CPU cost)
- More complex SQL queries during Load
- All root-level properties must be materialized (no selective field inclusion)

### 13. Limitations and Considerations

1. **Root-level properties only**: Feature only affects root-level fields, nested objects still stored as JSON columns

2. **All-or-nothing**: When enabled, ALL root-level properties become required - no selective field inclusion

3. **Database compatibility**: Each database needs specific JSON construction syntax

4. **No mixed mode**: Cannot have some bindings with `flow_document` and others without in same materialization

5. **Migration complexity**: No automatic migration path from existing materializations

## Summary

The `no_flow_document` feature flag provides an optimization for standard materializations by eliminating `flow_document` storage while maintaining full compatibility through dynamic JSON reconstruction. The implementation focuses on constraint modification and Load phase query changes, with no Store phase modifications required.
