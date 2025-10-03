package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *postgresDatabase) DiscoverTables(ctx context.Context) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all tables, columns and primary keys in the database
	var tables, err = getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas, db.config.Advanced.CaptureAsPartitions)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	columns, err := getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}
	generatedColumns, err := getGeneratedColumns(ctx, db.conn)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42703" {
			// Failure is expected on pre-12 versions of PostgreSQL which don't have
			// the 'attgenerated' column. Other errors should still be logged but
			// also probably don't warrant a fatal error.
			logrus.WithError(err).Warn("unable to list database generated columns")
		}
		generatedColumns = make(map[sqlcapture.StreamID][]string) // Empty map makes downstream logic simpler
	}

	// Column descriptions just add a bit of user-friendliness. They're so unimportant
	// that failure to list them shouldn't even be a fatal error.
	columnDescriptions, err := getColumnDescriptions(ctx, db.conn)
	if err != nil {
		logrus.WithField("err", err).Warn("error fetching column descriptions")
	}

	// Aggregate column and primary key information into DiscoveryInfo structs
	// using a map from fully-qualified "<schema>.<name>" table names to
	// the corresponding DiscoveryInfo.
	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
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
	for _, desc := range columnDescriptions {
		var streamID = sqlcapture.JoinStreamID(desc.TableSchema, desc.TableName)
		if info, ok := tableMap[streamID]; ok {
			if column, ok := info.Columns[desc.ColumnName]; ok {
				logrus.WithFields(logrus.Fields{
					"table":  streamID,
					"column": desc.ColumnName,
					"desc":   desc.Description,
				}).Trace("got column description")
				var description = desc.Description
				column.Description = &description
				info.Columns[desc.ColumnName] = column
			}
			tableMap[streamID] = info
		}
	}

	for streamID, key := range primaryKeys {
		// The `getColumns()` query implements the "exclude system schemas" logic,
		// so here we ignore primary key information for tables we don't care about.
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}
		logrus.WithFields(logrus.Fields{
			"table": streamID,
			"key":   key,
		}).Trace("queried primary key")
		info.PrimaryKey = key
		tableMap[streamID] = info
	}

	// Add generated columns information
	for streamID, table := range tableMap {
		if details, ok := table.ExtraDetails.(*postgresTableDiscoveryDetails); ok {
			details.GeneratedColumns = generatedColumns[streamID]
		}
		for _, columnName := range generatedColumns[streamID] {
			logrus.WithFields(logrus.Fields{
				"table":  streamID,
				"column": columnName,
			}).Debug("omitting generated column from schema generation")
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
		logrus.WithFields(logrus.Fields{
			"table":   streamID,
			"indices": len(indexColumns),
		}).Debug("checking for suitable secondary indexes")
		var suitableIndexes []string
		for indexName, columns := range indexColumns {
			if columnsNonNullable(info.Columns, columns) {
				logrus.WithFields(logrus.Fields{
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
			logrus.WithFields(logrus.Fields{
				"table": streamID,
				"index": selectedIndex,
			}).Debug("selected secondary index as table key")
			info.PrimaryKey = indexColumns[selectedIndex]
			info.FallbackKey = true
		} else {
			logrus.WithField("table", streamID).Debug("no secondary index is suitable")
		}
	}

	// Determine whether the database sorts the keys of a each table in a
	// predictable order or not. The term "predictable" here specifically
	// means "able to be reproduced using bytewise lexicographic ordering of
	// the serialized row keys generated by this connector".
	for _, info := range tableMap {
		for _, colName := range info.PrimaryKey {
			if !predictableColumnOrder(info.Columns[colName].DataType) {
				info.UnpredictableKeyOrdering = true
			}
		}
	}

	// If we've been asked to only discover tables which are already in the
	// publication, go through and mark any non-published tables as omitted.
	if db.config.Advanced.DiscoverOnlyPublished {
		var publicationStatus, err = listPublishedTables(ctx, db.conn, db.config.Advanced.PublicationName)
		if err != nil {
			return nil, err
		}
		for streamID, info := range tableMap {
			if !publicationStatus[streamID] {
				info.OmitBinding = true
			}
		}
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for id, info := range tableMap {
			logrus.WithFields(logrus.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	return tableMap, nil
}

// Returns true if the bytewise lexicographic ordering of serialized row keys for
// this column type matches the database backfill ordering.
func predictableColumnOrder(colType any) bool {
	// Currently all textual primary key columns are considered to be 'unpredictable' so that backfills
	// will default to using the 'imprecise' ordering semantics which avoids full-table sorts. Refer to
	// https://github.com/estuary/connectors/issues/1343 for more details.
	if colType == "varchar" || colType == "bpchar" || colType == "text" || colType == "citext" {
		return false
	} else if _, ok := colType.(postgresEnumType); ok {
		return false
	}

	return true
}

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *postgresDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	if !db.featureFlags["date_as_date"] {
		// Historically DATE columns were captured as RFC3339 timestamps.
		postgresTypeToJSON["date"] = columnSchema{jsonTypes: []string{"string"}, format: "date-time"}
	}
	if !db.featureFlags["time_as_time"] {
		// Historically TIME columns were captured as Unix seconds.
		postgresTypeToJSON["time"] = columnSchema{jsonTypes: []string{"integer"}}
	}

	var arrayColumn = false
	var colSchema columnSchema

	if columnType, ok := column.DataType.(string); ok {
		// If the column type looks like `_foo` then it's an array of elements of type `foo`.
		if strings.HasPrefix(columnType, "_") {
			columnType = strings.TrimPrefix(columnType, "_")
			arrayColumn = true
		}

		// Translate the basic value/element type into a JSON Schema type
		colSchema, ok = postgresTypeToJSON[columnType]
		if !ok {
			return nil, fmt.Errorf("unable to translate PostgreSQL type %q into JSON schema", columnType)
		}

		// If the column is a primary key and there's a special-case for this type as a primary
		// key (as in the case of floatN columns which always get stringified when used as keys),
		// then we use that instead of the normal-value type mapping above.
		if s, ok := postgresPrimaryKeyTypes[columnType]; isPrimaryKey && ok {
			colSchema = s
		}
	} else if dataType, ok := column.DataType.(postgresComplexType); ok {
		var schema, err = dataType.toColumnSchema(column)
		if err != nil {
			return nil, err
		}
		colSchema = schema
	} else {
		return nil, fmt.Errorf("unable to translate PostgreSQL type %q into JSON schema", column.DataType)
	}

	var jsonType *jsonschema.Schema
	if arrayColumn && db.featureFlags["multidimensional_arrays"] {
		// When 'multidimensional_arrays' is enabled, we don't specify an element type
		// in order to avoid the mess of trying to describe the recursive scalar-or-self
		// nature of the array elements. It's just an array of unspecified values.
		jsonType = &jsonschema.Schema{
			Extras: map[string]any{"type": "array"},
		}
		if column.IsNullable {
			// The column value itself may be null if the column is nullable.
			jsonType.Extras["type"] = []string{"array", "null"}
		}
	} else if arrayColumn && db.featureFlags["flatten_arrays"] {
		colSchema.nullable = true // Array elements might be null

		// New behavior: If the column is an array, turn the element type into a JSON array.
		jsonType = &jsonschema.Schema{
			Items:  colSchema.toType(),
			Extras: map[string]any{"type": "array"},
		}
		if column.IsNullable {
			// The column value itself may be null if the column is nullable.
			jsonType.Extras["type"] = []string{"array", "null"}
		}
	} else if arrayColumn && !db.featureFlags["flatten_arrays"] {
		// Nullability applies to the array itself, not the items. Items are always allowed to be
		// null unless additional checks are imposed on the column which we currently can't look
		// for.
		colSchema.nullable = true

		// Old behavior: If the column is an array, wrap the element type in a multidimensional array structure.
		jsonType = &jsonschema.Schema{
			Extras: map[string]interface{}{
				"properties": map[string]*jsonschema.Schema{
					"dimensions": {
						Type:  "array",
						Items: &jsonschema.Schema{Type: "integer"},
					},
					"elements": {
						Type:  "array",
						Items: colSchema.toType(),
					},
				},
				"additionalProperties": false,
			},
			Required: []string{"dimensions", "elements"},
		}

		// The column value itself may be null if the column is nullable.
		if column.IsNullable {
			jsonType.Extras["type"] = []string{"object", "null"}
		} else {
			jsonType.Type = "object"
		}
	} else {
		colSchema.nullable = column.IsNullable
		jsonType = colSchema.toType()
	}

	// Pass-through a postgres column description.
	if column.Description != nil {
		jsonType.Description = *column.Description
	}
	return jsonType, nil
}

type columnSchema struct {
	contentEncoding string
	format          string
	nullable        bool
	jsonTypes       []string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format: s.format,
		Extras: make(map[string]interface{}),
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}

	if s.jsonTypes != nil {
		var types = append([]string(nil), s.jsonTypes...)
		if s.nullable {
			types = append(types, "null")
		}
		if len(types) == 1 {
			out.Type = types[0]
		} else {
			out.Extras["type"] = types
		}
	}
	return out
}

var postgresPrimaryKeyTypes = map[string]columnSchema{
	"float4": {jsonTypes: []string{"string"}, format: "number"},
	"float8": {jsonTypes: []string{"string"}, format: "number"},
}

var postgresTypeToJSON = map[string]columnSchema{
	"bool": {jsonTypes: []string{"boolean"}},

	"int2": {jsonTypes: []string{"integer"}},
	"int4": {jsonTypes: []string{"integer"}},
	"int8": {jsonTypes: []string{"integer"}},
	"oid":  {jsonTypes: []string{"integer"}},

	"numeric": {jsonTypes: []string{"string"}, format: "number"},
	"float4":  {jsonTypes: []string{"number", "string"}, format: "number"},
	"float8":  {jsonTypes: []string{"number", "string"}, format: "number"},

	"varchar": {jsonTypes: []string{"string"}},
	"bpchar":  {jsonTypes: []string{"string"}},
	"text":    {jsonTypes: []string{"string"}},
	"citext":  {jsonTypes: []string{"string"}}, // From the 'citext' extension, but common enough we ought to support it properly
	"bytea":   {jsonTypes: []string{"string"}, contentEncoding: "base64"},
	"xml":     {jsonTypes: []string{"string"}},
	"bit":     {jsonTypes: []string{"string"}},
	"varbit":  {jsonTypes: []string{"string"}},

	"json":     {},
	"jsonb":    {},
	"jsonpath": {jsonTypes: []string{"string"}},

	// Domain-Specific Types
	"date":        {jsonTypes: []string{"string"}, format: "date"},
	"timestamp":   {jsonTypes: []string{"string"}, format: "date-time"},
	"timestamptz": {jsonTypes: []string{"string"}, format: "date-time"},
	"time":        {jsonTypes: []string{"string"}, format: "time"},
	"timetz":      {jsonTypes: []string{"string"}, format: "time"},
	"interval":    {jsonTypes: []string{"string"}},
	"money":       {jsonTypes: []string{"string"}},
	"point":       {jsonTypes: []string{"string"}},
	"line":        {jsonTypes: []string{"string"}},
	"lseg":        {jsonTypes: []string{"string"}},
	"box":         {jsonTypes: []string{"string"}},
	"path":        {jsonTypes: []string{"string"}},
	"polygon":     {jsonTypes: []string{"string"}},
	"circle":      {jsonTypes: []string{"string"}},
	"inet":        {jsonTypes: []string{"string"}},
	"cidr":        {jsonTypes: []string{"string"}},
	"macaddr":     {jsonTypes: []string{"string"}},
	"macaddr8":    {jsonTypes: []string{"string"}},
	"tsvector":    {jsonTypes: []string{"string"}},
	"tsquery":     {jsonTypes: []string{"string"}},
	"uuid":        {jsonTypes: []string{"string"}, format: "uuid"},
}

type postgresTableDiscoveryDetails struct {
	GeneratedColumns []string // List of the names of generated columns in this table, in no particular order.
}

func getTables(ctx context.Context, conn *pgxpool.Pool, selectedSchemas []string, captureAsPartitions bool) ([]*sqlcapture.DiscoveryInfo, error) {
	logrus.Debug("listing all tables in the database")

	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT n.nspname, c.relname")
	fmt.Fprintf(query, "  FROM pg_catalog.pg_class c")
	fmt.Fprintf(query, "  JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)")
	fmt.Fprintf(query, "  WHERE n.nspname NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron', 'pglogical')")
	fmt.Fprintf(query, "    AND c.relpersistence = 'p'") // exclude temporary tables ('t') and unlogged tables ('u')
	if captureAsPartitions {
		// When capturing as partitions, we want partition leaves only
		fmt.Fprintf(query, "    AND c.relispartition")
		fmt.Fprintf(query, "    AND c.relkind = 'r'") // Only ordinary tables (includes partitions but not the root of a partitioned table)
	} else {
		// Default behavior: exclude partitions and include root tables
		fmt.Fprintf(query, "    AND NOT c.relispartition")
		fmt.Fprintf(query, "    AND c.relkind IN ('r', 'p')") // Both ordinary and partitioned tables
	}
	fmt.Fprintf(query, ";")

	var tables []*sqlcapture.DiscoveryInfo
	var rows, err = conn.Query(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var omitBinding = false
		if len(selectedSchemas) > 0 && !slices.Contains(selectedSchemas, tableSchema) {
			logrus.WithFields(logrus.Fields{
				"schema": tableSchema,
				"table":  tableName,
			}).Debug("table in filtered schema")
			omitBinding = true
		}
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:       tableSchema,
			Name:         tableName,
			BaseTable:    true, // PostgreSQL discovery queries only ever list 'BASE TABLE' entities
			OmitBinding:  omitBinding,
			ExtraDetails: &postgresTableDiscoveryDetails{},
		})
	}
	return tables, rows.Err()
}

const queryDiscoverColumns = `
  SELECT nc.nspname as table_schema,
         c.relname as table_name,
		 a.attnum as ordinal_position,
		 a.attname as column_name,
		 NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,
		 COALESCE(bt.typname, t.typname) AS udt_name,
		 t.typtype::text AS typtype
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
	JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
	JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid
	LEFT JOIN (pg_catalog.pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)
	       ON t.typtype = 'd'::"char" AND t.typbasetype = bt.oid
    WHERE NOT pg_is_other_temp_schema(nc.oid)
	  AND a.attnum > 0
	  AND NOT a.attisdropped
	  AND (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"]))
	ORDER BY nc.nspname, c.relname, a.attnum;`

func getColumns(ctx context.Context, conn *pgxpool.Pool) ([]sqlcapture.ColumnInfo, error) {
	logrus.Debug("listing all columns in the database")
	var columns []sqlcapture.ColumnInfo
	var rows, err = conn.Query(ctx, queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var col sqlcapture.ColumnInfo
		var typtype string
		if err := rows.Scan(&col.TableSchema, &col.TableName, &col.Index, &col.Name, &col.IsNullable, &col.DataType, &typtype); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// Special cases for user-defined types where we must resolve the columnSchema directly.
		switch typtype {
		case "c": // composite
			// TODO(whb): We don't currently do any kind of special handling for composite
			// (tuple) types and just output whatever we get from from pgx's GenericText
			// decoder. The generated text for these isn't very usable and we may be able to
			// improve this by discovering composite types, building decoders for them, and
			// registering the decoders with the pgx connection. pgx v5 has new utility methods
			// specifically for doing this.
			col.DataType = postgresCompositeType{}
		case "e": // enum
			// Enum values are always strings corresponding to an enum label.
			col.DataType = postgresEnumType{}
		case "r", "m": // range, multirange
			// Capture ranges in their text form to retain inclusive (like `[`) & exclusive
			// (like `(`) bounds information. For example, the text form of a range representing
			// "integers greater than or equal to 1 but less than 5" is '[1,5)'
			col.DataType = postgresRangeType{multirange: typtype == "m"}
		}

		columns = append(columns, col)
	}
	return columns, rows.Err()
}

type postgresComplexType interface {
	String() string
	toColumnSchema(info sqlcapture.ColumnInfo) (columnSchema, error)
}

type postgresEnumType struct{}

func (t postgresEnumType) String() string { return "enum" }
func (t postgresEnumType) toColumnSchema(_ sqlcapture.ColumnInfo) (columnSchema, error) {
	return columnSchema{jsonTypes: []string{"string"}}, nil
}

type postgresRangeType struct {
	multirange bool
}

func (t postgresRangeType) String() string {
	if t.multirange {
		return "multirange"
	}
	return "range"
}

func (t postgresRangeType) toColumnSchema(_ sqlcapture.ColumnInfo) (columnSchema, error) {
	return columnSchema{jsonTypes: []string{"string"}}, nil
}

type postgresCompositeType struct{}

func (t postgresCompositeType) String() string { return "composite" }
func (t postgresCompositeType) toColumnSchema(_ sqlcapture.ColumnInfo) (columnSchema, error) {
	return columnSchema{}, nil
}

// Query copied from pgjdbc's method PgDatabaseMetaData.getPrimaryKeys() with
// the always-NULL `TABLE_CAT` column omitted.
//
// See: https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/PgDatabaseMetaData.java#L2134
const queryDiscoverPrimaryKeys = `
  SELECT result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ
  FROM (
    SELECT n.nspname AS TABLE_SCHEM,
      ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,
      (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,
      information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
    FROM pg_catalog.pg_class ct
      JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
      JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
      JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
      JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
    WHERE i.indisprimary
  ) result
  WHERE result.A_ATTNUM = (result.KEYS).x
  ORDER BY result.table_name, result.pk_name, result.key_seq;
`

// getPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func getPrimaryKeys(ctx context.Context, conn *pgxpool.Pool) (map[sqlcapture.StreamID][]string, error) {
	logrus.Debug("listing all primary-key columns in the database")
	var keys = make(map[sqlcapture.StreamID][]string)
	var rows, err = conn.Query(ctx, queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var columnIndex int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &columnIndex); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		keys[streamID] = append(keys[streamID], columnName)
		if columnIndex != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q of table %q appears out of order (expected index %d, in context %q)", columnName, streamID, columnIndex, keys[streamID])
		}
	}
	return keys, rows.Err()
}

const queryDiscoverSecondaryIndices = `
SELECT r.table_schema, r.table_name, r.index_name, (r.index_keys).n, r.column_name
FROM (
  SELECT tn.nspname AS table_schema,
         tc.relname AS table_name,
	     ic.relname AS index_name,
	     information_schema._pg_expandarray(ix.indkey) AS index_keys,
		 a.attnum AS column_number,
		 a.attname AS column_name
  FROM pg_catalog.pg_index ix
       JOIN pg_catalog.pg_class ic ON (ic.oid = ix.indexrelid)
	   JOIN pg_catalog.pg_class tc ON (tc.oid = ix.indrelid)
	   JOIN pg_catalog.pg_namespace tn ON (tn.oid = tc.relnamespace)
	   JOIN pg_catalog.pg_attribute a ON (a.attrelid = tc.oid)
  WHERE ix.indisunique AND ix.indexprs IS NULL AND tc.relkind = 'r'
    AND NOT ix.indisprimary
  ORDER BY tc.relname, ic.relname
) r
WHERE r.column_number = (r.index_keys).x
ORDER BY r.table_schema, r.table_name, r.index_name, (r.index_keys).n
`

func getSecondaryIndexes(ctx context.Context, conn *pgxpool.Pool) (map[sqlcapture.StreamID]map[string][]string, error) {
	logrus.Debug("listing secondary indexes")
	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[sqlcapture.StreamID]map[string][]string)
	var rows, err = conn.Query(ctx, queryDiscoverSecondaryIndices)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName, indexName, columnName string
		var keySequence int
		if err := rows.Scan(&tableSchema, &tableName, &indexName, &keySequence, &columnName); err != nil {
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

	return streamIndexColumns, rows.Err()
}

const queryListGeneratedColumns = `
	SELECT n.nspname AS table_schema,
       c.relname AS table_name,
       a.attname AS column_name
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
	JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
	WHERE a.attgenerated = 's'
	ORDER BY n.nspname, c.relname, a.attname;
`

func getGeneratedColumns(ctx context.Context, conn *pgxpool.Pool) (map[sqlcapture.StreamID][]string, error) {
	logrus.Debug("listing generated columns")
	var rows, err = conn.Query(ctx, queryListGeneratedColumns)
	if err != nil {
		return nil, fmt.Errorf("error querying generated columns: %w", err)
	}
	defer rows.Close()

	var generatedColumns = make(map[sqlcapture.StreamID][]string)
	for rows.Next() {
		var tableSchema, tableName, columnName string
		if err := rows.Scan(&tableSchema, &tableName, &columnName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		generatedColumns[streamID] = append(generatedColumns[streamID], columnName)
	}
	return generatedColumns, nil
}

const queryColumnDescriptions = `
    SELECT * FROM (
		SELECT
		    isc.table_schema,
			isc.table_name,
			isc.column_name,
			pg_catalog.col_description(format('"%s"."%s"',isc.table_schema,isc.table_name)::regclass::oid,isc.ordinal_position) description
		FROM information_schema.columns isc
		WHERE isc.table_schema NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron', 'pglogical')
	) as descriptions WHERE description != '';
`

type columnDescription struct {
	TableSchema string
	TableName   string
	ColumnName  string
	Description string
}

func getColumnDescriptions(ctx context.Context, conn *pgxpool.Pool) ([]columnDescription, error) {
	var descriptions []columnDescription
	var rows, err = conn.Query(ctx, queryColumnDescriptions)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var desc columnDescription
		if err := rows.Scan(&desc.TableSchema, &desc.TableName, &desc.ColumnName, &desc.Description); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		descriptions = append(descriptions, desc)
	}
	return descriptions, rows.Err()
}
