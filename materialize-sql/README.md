# materialize-sql

A shared library for building SQL-based materialization connectors for Estuary Flow.

## What it does

This package provides a common framework and abstraction layer for materializing Flow collections into SQL databases. It handles the core protocol interactions, type mapping, schema management, and SQL generation patterns that are common across SQL-based destinations.

## Architecture

The library is built around several key abstractions:

### Driver
The `Driver` struct (`driver.go:21`) implements the `boilerplate.Connector` interface and orchestrates the entire materialization lifecycle:
- **Spec**: Generates configuration schemas for endpoints and resources
- **Validate**: Validates configurations and builds constraints for collections
- **Apply**: Creates/alters tables based on binding changes  
- **NewTransactor**: Creates transactors for handling data flow

### Endpoint & Client
- **Endpoint** (`endpoint.go:87`): Represents a SQL destination with its dialect, configuration, and capabilities
- **Client** (`endpoint.go:14`): Interface for database-specific operations like creating tables, installing fences, and querying schemas

### Dialect System
The `Dialect` struct (`dialect.go:11`) encapsulates database-specific SQL variations:
- **Type Mapping**: Maps Flow projections to SQL column types via `TypeMapper`
- **Identifiers**: Handles quoting and escaping of table/column names
- **Literals**: Formats SQL literals and placeholders
- **Schema Location**: Maps logical paths to physical database locations

### Type Mapping
The type system (`type_mapping.go`) converts Flow's JSON Schema projections to SQL columns:
- **FlatType**: Simplified type representation (STRING, INTEGER, BOOLEAN, etc.)
- **MappedType**: Contains DDL, nullability constraints, and value converters
- **DDLMapper**: Configurable mapper with format-specific handlers

### Table Management
Tables are represented through a progression of abstractions:
- **TableShape** (`table_mapping.go:14`): Logical table structure from Flow specs
- **Table** (`table_mapping.go:32`): Resolved table with dialect-specific identifiers and columns
- **Column** (`table_mapping.go:50`): Resolved column with type mapping and converters

## Limitations

- No built-in support for transactions spanning multiple tables
- Schema migration capabilities depend on underlying database support
- Type compatibility checking relies on exact DDL string matching
- Column migration using temporary suffixes may leave artifacts on crashes

## Essential Types

### Driver Configuration
```go
type Driver struct {
    DocumentationURL string
    EndpointSpecType interface{}
    ResourceSpecType Resource
    StartTunnel      func(context.Context, any) error
    NewEndpoint      func(context.Context, json.RawMessage, string) (*Endpoint, error)
    PreReqs          func(context.Context, any, string) *cerrors.PrereqErr
}
```

### Core Interfaces
- `Resource`: Represents a SQL resource (table) binding
- `Client`: Database-specific operations interface  
- `TypeMapper`: Maps Flow projections to SQL types
- `SchemaManager`: Optional schema creation support

## Implementation Notes

### Table Creation
Tables are created using Go templates (`CreateTableTemplate`) that are rendered with resolved `Table` objects. This allows database-specific DDL generation while maintaining common structure.

### Schema Information  
The library uses `boilerplate.InfoSchema` to represent the current state of materialized tables, queried via the SQL `INFORMATION_SCHEMA` views.

### Column Migration
Type changes are handled through temporary column creation with the suffix `_flowtmp1`, allowing for data migration before dropping the original column.

### Fencing
Optional checkpoints table support provides transaction isolation through monotonic fence values, preventing concurrent materialization instances from interfering.

## Development

To implement a new SQL-based materializer:

1. Define endpoint and resource configuration types
2. Implement the `Client` interface for your database
3. Configure a `Dialect` with appropriate type mappings and SQL transforms  
4. Create SQL templates for table creation
5. Wire everything together in a `Driver` instance

Key files to understand:
- `driver.go`: Main connector implementation
- `dialect.go`: Database-specific SQL handling
- `type_mapping.go`: Flow type to SQL type conversion
- `apply.go`: Table creation and alteration logic