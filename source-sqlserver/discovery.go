package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/estuary/connectors/go/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// discoveryOptions controls how discovery queries are constructed.
type discoveryOptions struct {
	UppercaseQueries    bool // Use uppercase identifiers for compatibility with tricky collations
	DiscoverOnlyEnabled bool // Only discover tables with CDC capture instances
}

// DiscoverTables queries the database for information about tables available for capture.
func (db *sqlserverDatabase) DiscoverTables(ctx context.Context) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	var opts = discoveryOptions{
		UppercaseQueries:    db.featureFlags["uppercase_discovery_queries"],
		DiscoverOnlyEnabled: db.config.Advanced.DiscoverOnlyEnabled,
	}

	// Get lists of all tables, columns and primary keys in the database
	var tables, err = getTables(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	columns, err := getColumns(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}
	computedColumns, err := getComputedColumns(ctx, db.conn, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to list database computed columns: %w", err)
	}

	// Aggregate column information into DiscoveryInfo structs using a map
	// from fully-qualified table names to the corresponding info.
	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)

		// Depending on feature flag settings, we may normalize multiple table names
		// to the same StreamID. This is a problem and other parts of discovery won't
		// be able to handle it gracefully, so it's a fatal error.
		if other, ok := tableMap[streamID]; ok {
			return nil, fmt.Errorf("table name collision between %q and %q",
				fmt.Sprintf("%s.%s", table.Schema, table.Name),
				fmt.Sprintf("%s.%s", other.Schema, other.Name),
			)
		}

		if streamID == db.WatermarksTable() {
			// We want to exclude the watermarks table from the output bindings, but we still discover it
			table.OmitBinding = true
		}
		table.UseSchemaInference = db.featureFlags["use_schema_inference"]
		table.EmitSourcedSchemas = db.featureFlags["emit_sourced_schemas"]
		tableMap[streamID] = table
	}
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}

		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		tableMap[streamID] = info
	}

	// Add primary-key information to the tables map
	for id, key := range primaryKeys {
		var info, ok = tableMap[id]
		if !ok {
			continue
		}
		info.PrimaryKey = key
		tableMap[id] = info
	}

	// Add computed columns information
	for streamID, table := range tableMap {
		if details, ok := table.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
			details.ComputedColumns = computedColumns[streamID]
		}
		for _, columnName := range computedColumns[streamID] {
			var info = table.Columns[columnName]
			info.OmitColumn = true
			table.Columns[columnName] = info
		}
	}

	// For tables which have no primary key but have a valid secondary index,
	// fill in that secondary index as the 'primary key' for our purposes.
	for streamID, indexColumns := range secondaryIndexes {
		var info, ok = tableMap[streamID]
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

	// Determine whether the database sorts the keys of a each table in a
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
	for _, info := range tableMap {
		for _, colName := range info.PrimaryKey {
			var dataType = info.Columns[colName].DataType
			if _, ok := dataType.(*datatypes.TextColumnType); ok {
				info.UnpredictableKeyOrdering = true
			} else if dataType == "numeric" || dataType == "decimal" {
				info.UnpredictableKeyOrdering = true
			}
		}
	}

	// If we've been asked to only discover tables which are already enabled
	// for CDC (== had capture instances created), go and and mark any tables
	// without a CDC instance as omitted.
	//
	// TODO(wgd): This should be a no-op once query filter pushdown is fully
	// implemented, but we're keeping it for now as a safety net.
	if opts.DiscoverOnlyEnabled {
		var captureInstances, err = cdcListCaptureInstances(ctx, db.conn)
		if err != nil {
			return nil, fmt.Errorf("unable to list capture instances for 'discover_only_enabled' option: %w", err)
		}
		for streamID, info := range tableMap {
			if len(captureInstances[streamID]) == 0 {
				info.OmitBinding = true
			}
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

func queryDiscoverTables(opts discoveryOptions) string {
	if opts.DiscoverOnlyEnabled {
		return `
SELECT DISTINCT IST.TABLE_SCHEMA, IST.TABLE_NAME, IST.TABLE_TYPE
  FROM cdc.change_tables ct
  JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
  JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
  JOIN INFORMATION_SCHEMA.TABLES IST
    ON IST.TABLE_SCHEMA = sch.name AND IST.TABLE_NAME = tbl.name
  WHERE IST.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA', 'SYS', 'CDC')
    AND IST.TABLE_NAME != 'SYSTRANSCHEMAS';`
	}
	if !opts.UppercaseQueries {
		return `
  SELECT table_schema, table_name, table_type
  FROM information_schema.tables
  WHERE table_schema != 'information_schema' AND table_schema != 'performance_schema'
    AND table_schema != 'sys' AND table_schema != 'cdc'
	AND table_name != 'systranschemas';`
	}
	return `
  SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' AND TABLE_SCHEMA != 'PERFORMANCE_SCHEMA'
    AND TABLE_SCHEMA != 'SYS' AND TABLE_SCHEMA != 'CDC'
	AND TABLE_NAME != 'SYSTRANSCHEMAS';`
}

func getTables(ctx context.Context, conn *sql.DB, opts discoveryOptions) ([]*sqlcapture.DiscoveryInfo, error) {
	rows, err := conn.QueryContext(ctx, queryDiscoverTables(opts))
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
	return tables, nil
}

func queryDiscoverColumns(opts discoveryOptions) string {
	if opts.DiscoverOnlyEnabled {
		return `
SELECT DISTINCT ISC.TABLE_SCHEMA, ISC.TABLE_NAME, ISC.ORDINAL_POSITION, ISC.COLUMN_NAME,
       ISC.IS_NULLABLE, ISC.DATA_TYPE, ISC.COLLATION_NAME, ISC.CHARACTER_MAXIMUM_LENGTH
  FROM cdc.change_tables ct
  JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
  JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
  JOIN INFORMATION_SCHEMA.COLUMNS ISC
    ON ISC.TABLE_SCHEMA = sch.name AND ISC.TABLE_NAME = tbl.name
  ORDER BY ISC.TABLE_SCHEMA, ISC.TABLE_NAME, ISC.ORDINAL_POSITION;`
	}
	if !opts.UppercaseQueries {
		return `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, collation_name, character_maximum_length
  FROM information_schema.columns
  ORDER BY table_schema, table_name, ordinal_position;`
	}
	return `
  SELECT TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLLATION_NAME, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;`
}

func getColumns(ctx context.Context, conn *sql.DB, opts discoveryOptions) ([]sqlcapture.ColumnInfo, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverColumns(opts))
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
	return columns, nil
}


// Joining on the 6-tuple {CONSTRAINT,TABLE}_{CATALOG,SCHEMA,NAME} is probably
// overkill but shouldn't hurt, and helps to make absolutely sure that we're
// matching up the constraint type with the column names/positions correctly.
func queryDiscoverPrimaryKeys(opts discoveryOptions) string {
	if opts.DiscoverOnlyEnabled {
		return `
SELECT DISTINCT KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.COLUMN_NAME, KCU.ORDINAL_POSITION
  FROM cdc.change_tables ct
  JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
  JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
    ON KCU.TABLE_SCHEMA = sch.name AND KCU.TABLE_NAME = tbl.name
  JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TCS
    ON  TCS.CONSTRAINT_CATALOG = KCU.CONSTRAINT_CATALOG
    AND TCS.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA
    AND TCS.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
    AND TCS.TABLE_CATALOG = KCU.TABLE_CATALOG
    AND TCS.TABLE_SCHEMA = KCU.TABLE_SCHEMA
    AND TCS.TABLE_NAME = KCU.TABLE_NAME
  WHERE TCS.CONSTRAINT_TYPE = 'PRIMARY KEY'
  ORDER BY KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.ORDINAL_POSITION;`
	}
	if !opts.UppercaseQueries {
		return `
SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position
  FROM information_schema.key_column_usage kcu
  JOIN information_schema.table_constraints tcs
    ON  tcs.constraint_catalog = kcu.constraint_catalog
    AND tcs.constraint_schema = kcu.constraint_schema
    AND tcs.constraint_name = kcu.constraint_name
    AND tcs.table_catalog = kcu.table_catalog
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  WHERE tcs.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;`
	}
	return `
SELECT KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.COLUMN_NAME, KCU.ORDINAL_POSITION
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
  JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TCS
    ON  TCS.CONSTRAINT_CATALOG = KCU.CONSTRAINT_CATALOG
    AND TCS.CONSTRAINT_SCHEMA = KCU.CONSTRAINT_SCHEMA
    AND TCS.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
    AND TCS.TABLE_CATALOG = KCU.TABLE_CATALOG
    AND TCS.TABLE_SCHEMA = KCU.TABLE_SCHEMA
    AND TCS.TABLE_NAME = KCU.TABLE_NAME
  WHERE TCS.CONSTRAINT_TYPE = 'PRIMARY KEY'
  ORDER BY KCU.TABLE_SCHEMA, KCU.TABLE_NAME, KCU.ORDINAL_POSITION;`
}

func getPrimaryKeys(ctx context.Context, conn *sql.DB, opts discoveryOptions) (map[sqlcapture.StreamID][]string, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverPrimaryKeys(opts))
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
	return keys, nil
}

func queryDiscoverSecondaryIndexes(opts discoveryOptions) string {
	if opts.DiscoverOnlyEnabled {
		return `
SELECT DISTINCT sch.name, tbl.name, COALESCE(idx.name, 'null'), COL_NAME(ic.object_id,ic.column_id), ic.key_ordinal
FROM cdc.change_tables ct
     JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
	 JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
	 JOIN sys.indexes idx ON idx.object_id = tbl.object_id
	 JOIN sys.index_columns ic ON ic.index_id = idx.index_id AND ic.object_id = tbl.object_id
WHERE ic.key_ordinal != 0 AND idx.is_unique = 1 AND sch.name NOT IN ('cdc')
ORDER BY sch.name, tbl.name, COALESCE(idx.name, 'null'), ic.key_ordinal;`
	}
	return `
SELECT sch.name, tbl.name, COALESCE(idx.name, 'null'), COL_NAME(ic.object_id,ic.column_id), ic.key_ordinal
FROM sys.indexes idx
     JOIN sys.tables tbl ON tbl.object_id = idx.object_id
	 JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
	 JOIN sys.index_columns ic ON ic.index_id = idx.index_id AND ic.object_id = tbl.object_id
WHERE ic.key_ordinal != 0 AND idx.is_unique = 1 AND sch.name NOT IN ('cdc')
ORDER BY sch.name, tbl.name, idx.name, ic.key_ordinal;`
}

func getSecondaryIndexes(ctx context.Context, conn *sql.DB, opts discoveryOptions) (map[sqlcapture.StreamID]map[string][]string, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverSecondaryIndexes(opts))
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
	return streamIndexColumns, err
}

func queryDiscoverComputedColumns(opts discoveryOptions) string {
	if opts.DiscoverOnlyEnabled {
		return `
SELECT DISTINCT sch.name, tbl.name, col.name
  FROM cdc.change_tables ct
    JOIN sys.tables tbl ON ct.source_object_id = tbl.object_id
	JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
	JOIN sys.columns col ON col.object_id = tbl.object_id
  WHERE col.is_computed = 1;`
	}
	return `
SELECT sch.name, tbl.name, col.name
  FROM sys.columns col
    JOIN sys.tables tbl ON tbl.object_id = col.object_id
	JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
  WHERE col.is_computed = 1;`
}

func getComputedColumns(ctx context.Context, conn *sql.DB, opts discoveryOptions) (map[sqlcapture.StreamID][]string, error) {
	var rows, err = conn.QueryContext(ctx, queryDiscoverComputedColumns(opts))
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
	return computedColumns, nil
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *sqlserverDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	return datatypes.TranslateDBToJSONType(db.datatypesConfig, column)
}
