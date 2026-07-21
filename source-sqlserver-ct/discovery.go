package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// discoveryChunkSize is the number of tables per chunk in DiscoverTableDetails.
//
// This is deliberately kept modest. The per-table detail queries filter using a
// disjunction of (schema, table) equalities (see tableIDsPredicate) which SQL
// Server plans as efficient per-table index seeks, but only when the number of
// terms is small enough.
const discoveryChunkSize = 100

// discoveryOptions controls how discovery queries are constructed.
type discoveryOptions struct {
	DiscoverOnlyEnabled bool     // Only discover tables with Change Tracking enabled
	IncludeSchemas      []string // If non-empty, discovery is restricted to these schemas
}

// tableIDsPredicate builds a WHERE-clause predicate matching any of the
// specified tables, using the given column expressions for the schema and
// table names. The returned clause has the form
//
//	((<schemaCol> = @p1 AND <tableCol> = @p2) OR (<schemaCol> = @p3 AND <tableCol> = @p4) ...)
//
// along with the corresponding positional argument slice. Placeholder
// numbering starts at startArg so the predicate can follow any arguments the
// query already binds.
//
// This disjunction-of-equalities form plans as per-table index seeks on the
// system catalogs, but only so long as discoveryChunkSize is kept smallish.
// Currently we use 100 and that's probably in the sweet spot.
func tableIDsPredicate(schemaCol, tableCol string, tables []sqlcapture.TableID, startArg int) (string, []any) {
	var terms = make([]string, len(tables))
	var args = make([]any, 0, len(tables)*2)
	for i, t := range tables {
		terms[i] = fmt.Sprintf("(%s = @p%d AND %s = @p%d)", schemaCol, startArg+2*i, tableCol, startArg+2*i+1)
		args = append(args, t.Schema, t.Table)
	}
	return "(" + strings.Join(terms, " OR ") + ")", args
}

// ListTables returns identifiers for all tables visible for capture after the
// connector's configured discovery filters are applied.
func (db *sqlserverDatabase) ListTables(ctx context.Context) ([]sqlcapture.TableID, error) {
	var opts = discoveryOptions{
		DiscoverOnlyEnabled: !db.config.Advanced.DiscoverNonEnabled,
	}
	var tables, err = listTables(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	return tables, nil
}

// DiscoverTableDetails queries the database for detailed schema information
// about the specified tables. The per-table metadata queries are chunked into
// batches so that discovery scales to databases with very large catalogs.
func (db *sqlserverDatabase) DiscoverTableDetails(ctx context.Context, requested []sqlcapture.TableID) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, len(requested))
	for i := 0; i < len(requested); i += discoveryChunkSize {
		var end = min(i+discoveryChunkSize, len(requested))
		if err := db.extendTableDetails(ctx, tableMap, requested[i:end]); err != nil {
			return nil, err
		}
	}

	// Change Tracking requires primary keys. Mark tables without a primary key as omitted.
	for streamID, info := range tableMap {
		if len(info.PrimaryKey) == 0 {
			log.WithField("table", streamID).Debug("omitting table without primary key (required for Change Tracking)")
			info.OmitBinding = true
		}
	}

	if log.IsLevelEnabled(log.DebugLevel) {
		for id, info := range tableMap {
			log.WithFields(log.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	return tableMap, nil
}

// extendTableDetails runs the per-chunk discovery queries (tables, columns,
// primary keys, computed columns) for one batch of requested tables and merges
// the results into dst.
func (db *sqlserverDatabase) extendTableDetails(ctx context.Context, dst map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, requested []sqlcapture.TableID) error {
	tables, err := getTables(ctx, db.conn, requested)
	if err != nil {
		return fmt.Errorf("unable to list database tables: %w", err)
	}
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)

		// Depending on feature flag settings, we may normalize multiple table names
		// to the same StreamID. This is a problem and other parts of discovery won't
		// be able to handle it gracefully, so it's a fatal error.
		if other, ok := dst[streamID]; ok {
			return fmt.Errorf("table name collision between %q and %q",
				fmt.Sprintf("%s.%s", table.Schema, table.Name),
				fmt.Sprintf("%s.%s", other.Schema, other.Name),
			)
		}

		table.UseSchemaInference = true
		table.EmitSourcedSchemas = true
		dst[streamID] = table
	}

	columns, err := getColumns(ctx, db.conn, requested)
	if err != nil {
		return fmt.Errorf("unable to list database columns: %w", err)
	}
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = dst[streamID]
		if !ok {
			continue
		}
		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		dst[streamID] = info
	}

	primaryKeys, err := getPrimaryKeys(ctx, db.conn, requested)
	if err != nil {
		return fmt.Errorf("unable to list database primary keys: %w", err)
	}
	for id, key := range primaryKeys {
		var info, ok = dst[id]
		if !ok {
			continue
		}
		info.PrimaryKey = key
		dst[id] = info
	}

	computedColumns, err := getComputedColumns(ctx, db.conn, requested)
	if err != nil {
		return fmt.Errorf("unable to list database computed columns: %w", err)
	}
	// Computed columns are recorded for informational purposes only; CT can capture them.
	for streamID, table := range dst {
		if details, ok := table.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
			details.ComputedColumns = computedColumns[streamID]
		}
	}

	// Determine whether the database sorts the keys of each table in a
	// predictable order or not. The term "predictable" here specifically
	// means "able to be reproduced using bytewise lexicographic ordering of
	// the serialized row keys generated by this connector".
	for _, info := range dst {
		for _, colName := range info.PrimaryKey {
			var dataType = info.Columns[colName].DataType
			if _, ok := dataType.(*datatypes.TextColumnType); ok {
				info.UnpredictableKeyOrdering = true
			} else if dataType == "numeric" || dataType == "decimal" {
				info.UnpredictableKeyOrdering = true
			}
		}
	}

	return nil
}

type sqlserverTableDiscoveryDetails struct {
	ComputedColumns []string // List of the names of computed columns in this table, in no particular order.
}

// listTables returns the identifiers of all tables visible for capture,
// optionally restricted to the given set of schemas. Schema filtering is
// pushed down into the query so it can be evaluated efficiently even against
// large system catalogs.
func listTables(ctx context.Context, conn *sql.DB, opts discoveryOptions) ([]sqlcapture.TableID, error) {
	var query = new(strings.Builder)
	var args []any
	var schemaCol string

	if opts.DiscoverOnlyEnabled {
		schemaCol = "IST.TABLE_SCHEMA"
		query.WriteString(`
SELECT DISTINCT IST.TABLE_SCHEMA, IST.TABLE_NAME
  FROM sys.change_tracking_tables ctt
  JOIN sys.tables tbl ON ctt.object_id = tbl.object_id
  JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
  JOIN INFORMATION_SCHEMA.TABLES IST
    ON IST.TABLE_SCHEMA = sch.name AND IST.TABLE_NAME = tbl.name
  WHERE IST.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA', 'SYS')
    AND IST.TABLE_NAME != 'SYSTRANSCHEMAS'`)
	} else {
		schemaCol = "TABLE_SCHEMA"
		query.WriteString(`
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' AND TABLE_SCHEMA != 'PERFORMANCE_SCHEMA'
    AND TABLE_SCHEMA != 'SYS'
    AND TABLE_NAME != 'SYSTRANSCHEMAS'`)
	}

	if len(opts.IncludeSchemas) > 0 {
		var placeholders = make([]string, len(opts.IncludeSchemas))
		for i, s := range opts.IncludeSchemas {
			placeholders[i] = fmt.Sprintf("@p%d", i+1)
			args = append(args, s)
		}
		fmt.Fprintf(query, "\n    AND %s IN (%s)", schemaCol, strings.Join(placeholders, ", "))
	}
	query.WriteString(";")

	rows, err := conn.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer rows.Close()

	var tables []sqlcapture.TableID
	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		tables = append(tables, sqlcapture.TableID{Schema: tableSchema, Table: tableName})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered tables: %w", err)
	}
	return tables, nil
}

// getTables fetches table-level discovery metadata for a specific set of
// tables, pushing the requested set down into the query predicate.
func getTables(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) ([]*sqlcapture.DiscoveryInfo, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	var predicate, args = tableIDsPredicate("TABLE_SCHEMA", "TABLE_NAME", requested, 1)
	var query = fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE %s;", predicate)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer rows.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:       tableSchema,
			Name:         tableName,
			BaseTable:    strings.EqualFold(tableType, "BASE TABLE"),
			ExtraDetails: &sqlserverTableDiscoveryDetails{},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered tables: %w", err)
	}
	return tables, nil
}

func queryDiscoverColumns(predicate string) string {
	return fmt.Sprintf(`
  SELECT TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLLATION_NAME, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE %s
  ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;`, predicate)
}

func getColumns(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) ([]sqlcapture.ColumnInfo, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	var predicate, args = tableIDsPredicate("TABLE_SCHEMA", "TABLE_NAME", requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverColumns(predicate), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	var columns []sqlcapture.ColumnInfo
	for rows.Next() {
		var ci sqlcapture.ColumnInfo
		var isNullable, typeName string
		var collationName *string
		var maxCharLength *int
		if err := rows.Scan(&ci.TableSchema, &ci.TableName, &ci.Index, &ci.Name, &isNullable, &typeName, &collationName, &maxCharLength); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		ci.IsNullable = isNullable != "NO"
		if slices.Contains([]string{"char", "varchar", "nchar", "nvarchar", "text", "ntext"}, typeName) {
			// The collation name should never be null for a text column type, but if it
			// is we'll just default to the string 'NULL' so it's clear what the error is.
			var collation = "NULL"
			if collationName != nil {
				collation = *collationName
			}
			var fullType = typeName
			var columnSize int = 64 // For historical reasons we want to overestimate if CHAR_MAX_LENGTH is null
			if maxCharLength != nil {
				columnSize = *maxCharLength
			}
			if slices.Contains([]string{"char", "varchar", "nchar", "nvarchar"}, typeName) {
				fullType = fmt.Sprintf("%s(%d)", typeName, columnSize)
			}
			ci.DataType = &datatypes.TextColumnType{
				Type:      typeName,
				Collation: collation,
				FullType:  fullType,
				MaxLength: columnSize,
			}
		} else if slices.Contains([]string{"binary", "varbinary"}, typeName) {
			var columnSize int
			if maxCharLength != nil {
				columnSize = *maxCharLength
			}
			ci.DataType = &datatypes.BinaryColumnType{
				Type:      typeName,
				MaxLength: columnSize,
			}
		} else {
			ci.DataType = typeName
		}
		columns = append(columns, ci)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered columns: %w", err)
	}
	return columns, nil
}

// Joining on the 6-tuple {CONSTRAINT,TABLE}_{CATALOG,SCHEMA,NAME} is probably
// overkill but shouldn't hurt, and helps to make absolutely sure that we're
// matching up the constraint type with the column names/positions correctly.
func queryDiscoverPrimaryKeys(predicate string) string {
	return fmt.Sprintf(`
SELECT KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.COLUMN_NAME, KCU.ORDINAL_POSITION
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
  JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TCS
    ON  TCS.CONSTRAINT_CATALOG = KCU.CONSTRAINT_CATALOG
    AND TCS.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA
    AND TCS.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
    AND TCS.TABLE_CATALOG = KCU.TABLE_CATALOG
    AND TCS.TABLE_SCHEMA = KCU.TABLE_SCHEMA
    AND TCS.TABLE_NAME = KCU.TABLE_NAME
  WHERE TCS.CONSTRAINT_TYPE = 'PRIMARY KEY' AND %s
  ORDER BY KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.ORDINAL_POSITION;`, predicate)
}

func getPrimaryKeys(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) (map[sqlcapture.StreamID][]string, error) {
	if len(requested) == 0 {
		return make(map[sqlcapture.StreamID][]string), nil
	}
	var predicate, args = tableIDsPredicate("KCU.TABLE_SCHEMA", "KCU.TABLE_NAME", requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverPrimaryKeys(predicate), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying primary keys: %w", err)
	}
	defer rows.Close()

	var keys = make(map[sqlcapture.StreamID][]string)
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var index int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &index); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		keys[streamID] = append(keys[streamID], columnName)
		if index != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, streamID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered primary keys: %w", err)
	}
	return keys, nil
}

func queryDiscoverComputedColumns(predicate string) string {
	return fmt.Sprintf(`
SELECT sch.name, tbl.name, col.name
  FROM sys.columns col
    JOIN sys.tables tbl ON tbl.object_id = col.object_id
	JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
  WHERE col.is_computed = 1 AND %s;`, predicate)
}

func getComputedColumns(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) (map[sqlcapture.StreamID][]string, error) {
	if len(requested) == 0 {
		return make(map[sqlcapture.StreamID][]string), nil
	}
	var predicate, args = tableIDsPredicate("sch.name", "tbl.name", requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverComputedColumns(predicate), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying computed columns: %w", err)
	}
	defer rows.Close()

	var computedColumns = make(map[sqlcapture.StreamID][]string)
	for rows.Next() {
		var tableSchema, tableName, columnName string
		if err := rows.Scan(&tableSchema, &tableName, &columnName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		computedColumns[streamID] = append(computedColumns[streamID], columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered computed columns: %w", err)
	}
	return computedColumns, nil
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *sqlserverDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, _isPrimaryKey bool) (*jsonschema.Schema, error) {
	return datatypes.TranslateDBToJSONType(db.datatypesConfig, column)
}
