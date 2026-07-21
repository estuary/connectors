package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
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
	UppercaseQueries    bool     // Use uppercase identifiers for compatibility with tricky collations
	DiscoverOnlyEnabled bool     // Only discover tables with CDC capture instances
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
		UppercaseQueries:    db.featureFlags["uppercase_discovery_queries"],
		DiscoverOnlyEnabled: db.config.Advanced.DiscoverOnlyEnabled,
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
	var opts = discoveryOptions{
		UppercaseQueries:    db.featureFlags["uppercase_discovery_queries"],
		DiscoverOnlyEnabled: db.config.Advanced.DiscoverOnlyEnabled,
	}

	// Replication needs the watermarks table in the result map even when no binding
	// references it, so we ensure it's in the requested list.
	if !db.featureFlags["read_only"] {
		var w = db.WatermarksTable()
		if !slices.ContainsFunc(requested, func(t sqlcapture.TableID) bool {
			return sqlcapture.JoinStreamID(t.Schema, t.Table) == w
		}) {
			requested = append([]sqlcapture.TableID{{Schema: w.Schema, Table: w.Table}}, requested...)
		}
	}

	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, len(requested))
	for i := 0; i < len(requested); i += discoveryChunkSize {
		var end = min(i+discoveryChunkSize, len(requested))
		if err := db.extendTableDetails(ctx, opts, tableMap, requested[i:end]); err != nil {
			return nil, err
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
// primary keys, secondary indexes, computed columns) for one batch of
// requested tables and merges the results into dst. Because a table appears in
// exactly one chunk, all of its metadata is fetched together and the per-table
// derivations (fallback keys, key-ordering predictability) can be finalized
// here rather than after all chunks complete.
func (db *sqlserverDatabase) extendTableDetails(ctx context.Context, opts discoveryOptions, dst map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, requested []sqlcapture.TableID) error {
	tables, err := getTables(ctx, db.conn, opts, requested)
	if err != nil {
		return fmt.Errorf("unable to list database tables: %w", err)
	}
	var watermarks = db.WatermarksTable()
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

		if streamID == watermarks {
			// We want to exclude the watermarks table from the output bindings, but we still discover it
			table.OmitBinding = true
		}
		table.UseSchemaInference = db.featureFlags["use_schema_inference"]
		table.EmitSourcedSchemas = db.featureFlags["emit_sourced_schemas"]
		dst[streamID] = table
	}

	columns, err := getColumns(ctx, db.conn, opts, requested)
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

	primaryKeys, err := getPrimaryKeys(ctx, db.conn, opts, requested)
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
	for streamID, table := range dst {
		if details, ok := table.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
			details.ComputedColumns = computedColumns[streamID]
		}
		for _, columnName := range computedColumns[streamID] {
			var info, ok = table.Columns[columnName]
			if !ok {
				continue
			}
			info.OmitColumn = true
			table.Columns[columnName] = info
		}
	}

	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn, requested)
	if err != nil {
		return fmt.Errorf("unable to list database secondary indexes: %w", err)
	}
	// For tables which have no primary key but have a valid secondary index,
	// fill in that secondary index as the 'primary key' for our purposes.
	for streamID, indexColumns := range secondaryIndexes {
		var info, ok = dst[streamID]
		if !ok || info.PrimaryKey != nil {
			continue
		}

		// Make a list of all usable indexes.
		log.WithFields(log.Fields{
			"table":   streamID,
			"indices": len(indexColumns),
		}).Debug("checking for suitable secondary indexes")
		var suitableIndexes []string
		for indexName, columns := range indexColumns {
			if columnsNonNullable(info.Columns, columns) {
				log.WithFields(log.Fields{
					"table":   streamID,
					"index":   indexName,
					"columns": columns,
				}).Debug("secondary index could be used as primary key")
				suitableIndexes = append(suitableIndexes, indexName)
			}
		}

		// Sort the list by index name and pick the first one, if there are multiple.
		// This helps ensure stable selection, although it could still change due to
		// the creation of a new secondary index.
		sort.Strings(suitableIndexes)
		if len(suitableIndexes) > 0 {
			var selectedIndex = suitableIndexes[0]
			log.WithFields(log.Fields{
				"table": streamID,
				"index": selectedIndex,
			}).Debug("selected secondary index as table key")
			info.PrimaryKey = indexColumns[selectedIndex]
			info.FallbackKey = true
		} else {
			log.WithField("table", streamID).Debug("no secondary index is suitable")
		}
	}

	// Determine whether the database sorts the keys of each table in a
	// predictable order or not. The term "predictable" here specifically
	// means "able to be reproduced using bytewise lexicographic ordering of
	// the serialized row keys generated by this connector".
	//
	// TODO(wgd): Our handling of predictable column ordering here is actually
	// terrible and doesn't work if the user overrides the backfill key to be
	// something other than what we've identified as the table's primary key.
	// But in practice that never comes up and it would be a fairly large bit
	// of refactoring to go and do this check using the actual collection key
	// when the capture starts up, so here it stays until it becomes an issue.
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

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
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
  FROM cdc.change_tables ct
  JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
  JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
  JOIN INFORMATION_SCHEMA.TABLES IST
    ON IST.TABLE_SCHEMA = sch.name AND IST.TABLE_NAME = tbl.name
  WHERE IST.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA', 'SYS', 'CDC')
    AND IST.TABLE_NAME != 'SYSTRANSCHEMAS'`)
	} else if opts.UppercaseQueries {
		schemaCol = "TABLE_SCHEMA"
		query.WriteString(`
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' AND TABLE_SCHEMA != 'PERFORMANCE_SCHEMA'
    AND TABLE_SCHEMA != 'SYS' AND TABLE_SCHEMA != 'CDC'
    AND TABLE_NAME != 'SYSTRANSCHEMAS'`)
	} else {
		schemaCol = "table_schema"
		query.WriteString(`
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
    AND table_schema != 'sys' AND table_schema != 'cdc'
    AND table_name != 'systranschemas'`)
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
func getTables(ctx context.Context, conn *sql.DB, opts discoveryOptions, requested []sqlcapture.TableID) ([]*sqlcapture.DiscoveryInfo, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	var tableRef, schemaCol, nameCol, typeCol = "information_schema.tables", "table_schema", "table_name", "table_type"
	if opts.UppercaseQueries {
		tableRef, schemaCol, nameCol, typeCol = "INFORMATION_SCHEMA.TABLES", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"
	}
	var predicate, args = tableIDsPredicate(schemaCol, nameCol, requested, 1)
	var query = fmt.Sprintf("SELECT %s, %s, %s FROM %s WHERE %s;", schemaCol, nameCol, typeCol, tableRef, predicate)

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

func queryDiscoverColumns(opts discoveryOptions, predicate string) string {
	if opts.UppercaseQueries {
		return fmt.Sprintf(`
  SELECT TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLLATION_NAME, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE %s
  ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;`, predicate)
	}
	return fmt.Sprintf(`
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, collation_name, character_maximum_length
  FROM information_schema.columns
  WHERE %s
  ORDER BY table_schema, table_name, ordinal_position;`, predicate)
}

func getColumns(ctx context.Context, conn *sql.DB, opts discoveryOptions, requested []sqlcapture.TableID) ([]sqlcapture.ColumnInfo, error) {
	if len(requested) == 0 {
		return nil, nil
	}
	var schemaCol, nameCol = "table_schema", "table_name"
	if opts.UppercaseQueries {
		schemaCol, nameCol = "TABLE_SCHEMA", "TABLE_NAME"
	}
	var predicate, args = tableIDsPredicate(schemaCol, nameCol, requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverColumns(opts, predicate), args...)
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
func queryDiscoverPrimaryKeys(opts discoveryOptions, predicate string) string {
	if opts.UppercaseQueries {
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
	return fmt.Sprintf(`
SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position
  FROM information_schema.key_column_usage kcu
  JOIN information_schema.table_constraints tcs
    ON  tcs.constraint_catalog = kcu.constraint_catalog
    AND tcs.constraint_schema = kcu.constraint_schema
    AND tcs.constraint_name = kcu.constraint_name
    AND tcs.table_catalog = kcu.table_catalog
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  WHERE tcs.constraint_type = 'PRIMARY KEY' AND %s
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;`, predicate)
}

func getPrimaryKeys(ctx context.Context, conn *sql.DB, opts discoveryOptions, requested []sqlcapture.TableID) (map[sqlcapture.StreamID][]string, error) {
	if len(requested) == 0 {
		return make(map[sqlcapture.StreamID][]string), nil
	}
	var schemaCol, nameCol = "kcu.table_schema", "kcu.table_name"
	if opts.UppercaseQueries {
		schemaCol, nameCol = "KCU.TABLE_SCHEMA", "KCU.TABLE_NAME"
	}
	var predicate, args = tableIDsPredicate(schemaCol, nameCol, requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverPrimaryKeys(opts, predicate), args...)
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

func queryDiscoverSecondaryIndexes(predicate string) string {
	return fmt.Sprintf(`
SELECT sch.name, tbl.name, COALESCE(idx.name, 'null'), COL_NAME(ic.object_id,ic.column_id), ic.key_ordinal
FROM sys.indexes idx
     JOIN sys.tables tbl ON tbl.object_id = idx.object_id
	 JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
	 JOIN sys.index_columns ic ON ic.index_id = idx.index_id AND ic.object_id = tbl.object_id
WHERE ic.key_ordinal != 0 AND idx.is_unique = 1 AND sch.name NOT IN ('cdc') AND %s
ORDER BY sch.name, tbl.name, idx.name, ic.key_ordinal;`, predicate)
}

func getSecondaryIndexes(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) (map[sqlcapture.StreamID]map[string][]string, error) {
	if len(requested) == 0 {
		return make(map[sqlcapture.StreamID]map[string][]string), nil
	}
	var predicate, args = tableIDsPredicate("sch.name", "tbl.name", requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverSecondaryIndexes(predicate), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying secondary indexes: %w", err)
	}
	defer rows.Close()

	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[sqlcapture.StreamID]map[string][]string)
	for rows.Next() {
		var tableSchema, tableName, indexName, columnName string
		var keySequence int
		if err := rows.Scan(&tableSchema, &tableName, &indexName, &columnName, &keySequence); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		var indexColumns = streamIndexColumns[streamID]
		if indexColumns == nil {
			indexColumns = make(map[string][]string)
			streamIndexColumns[streamID] = indexColumns
		}
		indexColumns[indexName] = append(indexColumns[indexName], columnName)
		if len(indexColumns[indexName]) != keySequence {
			return nil, fmt.Errorf("internal error: secondary index key ordering failure: index %q on stream %q: column %q appears out of order", indexName, streamID, columnName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered secondary indexes: %w", err)
	}
	return streamIndexColumns, nil
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
func (db *sqlserverDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	return datatypes.TranslateDBToJSONType(db.datatypesConfig, column)
}
