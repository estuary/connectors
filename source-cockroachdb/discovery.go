package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// System schemas which should never be discovered. In addition to the PostgreSQL system schemas,
// CockroachDB exposes `crdb_internal` and `pg_extension`, each containing many tables which would
// otherwise appear as junk bindings.
var systemSchemas = []string{"pg_catalog", "pg_internal", "information_schema", "crdb_internal", "pg_extension"}

func systemSchemaList() string {
	var quoted = make([]string, len(systemSchemas))
	for i, s := range systemSchemas {
		quoted[i] = "'" + s + "'"
	}
	return strings.Join(quoted, ", ")
}

// hashShardColumnRegexp matches the hidden virtual computed column which CockroachDB adds to
// hash-sharded indexes (`crdb_internal_<columns>_shard_<buckets>`). Its value is a pure function
// of the visible key columns, so a primary key remains unique with the shard column removed.
var hashShardColumnRegexp = regexp.MustCompile(`^crdb_internal_.+_shard_[0-9]+$`)

// cockroachdbTableDetails carries connector-specific facts about a table from discovery to the
// backfill and replication logic, via DiscoveryInfo.ExtraDetails.
type cockroachdbTableDetails struct {
	// fullPrimaryKey is the complete primary key including hidden columns, in primary-key column
	// order. Changefeed key arrays always contain the values of these columns in this order,
	// regardless of what the collection is keyed on.
	fullPrimaryKey []string

	// docMergeColumns lists hidden stored key columns (in practice: the synthesized rowid) which
	// appear in changefeed `after` documents but not in `to_jsonb(row)`, and which the backfill
	// must therefore merge into its documents to keep the two byte-for-byte consistent.
	docMergeColumns []string

	// unusableKeyReason is non-empty when the table has no key this connector can capture with,
	// in which case validation fails with this explanation.
	unusableKeyReason string
}

// ListTables returns identifiers for all tables visible for capture.
func (db *cockroachdbDatabase) ListTables(ctx context.Context) ([]sqlcapture.TableID, error) {
	var tables, err = getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	var ids = make([]sqlcapture.TableID, 0, len(tables))
	for _, table := range tables {
		ids = append(ids, sqlcapture.TableID{Schema: table.Schema, Table: table.Name})
	}
	return ids, nil
}

// DiscoverTableDetails queries the database for detailed schema information about the
// specified tables.
func (db *cockroachdbDatabase) DiscoverTableDetails(ctx context.Context, requested []sqlcapture.TableID) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	tables, err := getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
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

	var tableDetails = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, len(tables))
	for _, table := range tables {
		tableDetails[sqlcapture.JoinStreamID(table.Schema, table.Name)] = table
	}
	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, len(requested))
	for _, req := range requested {
		var streamID = sqlcapture.JoinStreamID(req.Schema, req.Table)
		if table, ok := tableDetails[streamID]; ok {
			table.UseSchemaInference = db.featureFlags["use_schema_inference"]
			table.EmitSourcedSchemas = db.featureFlags["emit_sourced_schemas"]
			table.ExtraDetails = &cockroachdbTableDetails{}
			tableMap[streamID] = table
		}
	}

	var hiddenColumns = make(map[sqlcapture.StreamID]map[string]cockroachdbColumn)
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}
		if column.Hidden {
			// Hidden columns don't appear in `to_jsonb(row)` and so can't be captured directly,
			// but primary keys may contain them, so keep them around for the key logic below.
			if hiddenColumns[streamID] == nil {
				hiddenColumns[streamID] = make(map[string]cockroachdbColumn)
			}
			hiddenColumns[streamID][column.Name] = column
			continue
		}
		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column.ColumnInfo
		info.ColumnNames = append(info.ColumnNames, column.Name)
	}

	// Work out the collection key for each table. The changefeed's key array always holds the
	// full primary key (hidden columns included), but the collection key can only use columns
	// which actually appear in captured documents, and Flow keys can't be floating-point:
	//
	//   - Hash-shard columns are hidden virtual columns computed from the visible key columns,
	//     so they're dropped and the visible remainder is used as the key.
	//   - The synthesized `rowid` of a table with no user-defined key is a hidden stored column
	//     which the changefeed emits in `after`; it's adopted as a real discovered column and
	//     becomes the key, with the backfill merging it into its documents to match.
	//   - FLOAT and DECIMAL key columns are unusable: their document values are raw JSON numbers,
	//     which Flow collections can't be keyed on.
	//   - Any other hidden key column (e.g. `crdb_region` of a REGIONAL BY ROW table) makes the
	//     key unusable.
	//
	// Tables without a usable key are still discovered (so they show up with a clear error at
	// validation time, via SetupTablePrerequisites) but can't be captured. A unique secondary
	// index is NOT used as a fallback key: changefeed delete events carry only the primary key,
	// so a collection keyed on other columns could never process deletes.
	for streamID, key := range primaryKeys {
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}
		var details = info.ExtraDetails.(*cockroachdbTableDetails)
		details.fullPrimaryKey = key

		var visibleKey []string
		var unusableReason string
		for _, col := range key {
			if columnInfo, ok := info.Columns[col]; ok {
				if typeDesc, ok := columnInfo.DataType.(cockroachdbTypeDescription); ok {
					switch typeDesc.TypeName() {
					case "float4", "float8", "numeric":
						unusableReason = fmt.Sprintf("primary key column %q has floating-point type %q, which cannot be used as a collection key", col, typeDesc.TypeName())
					}
				}
				visibleKey = append(visibleKey, col)
			} else if hashShardColumnRegexp.MatchString(col) {
				continue // Derived from the visible key columns; the key is unique without it.
			} else if col == "rowid" && len(key) == 1 {
				var hiddenColumn, ok = hiddenColumns[streamID][col]
				if !ok {
					unusableReason = fmt.Sprintf("primary key column %q is not a discoverable column", col)
					break
				}
				info.Columns[col] = hiddenColumn.ColumnInfo
				info.ColumnNames = append(info.ColumnNames, col)
				details.docMergeColumns = append(details.docMergeColumns, col)
				visibleKey = append(visibleKey, col)
			} else {
				unusableReason = fmt.Sprintf("primary key column %q is a hidden column which cannot be captured", col)
			}
			if unusableReason != "" {
				break
			}
		}
		if unusableReason == "" && len(visibleKey) == 0 {
			unusableReason = "primary key has no capturable columns"
		}
		if unusableReason != "" {
			details.unusableKeyReason = unusableReason
			logrus.WithFields(logrus.Fields{"table": streamID, "key": key, "reason": unusableReason}).Debug("table has no usable key")
			continue
		}
		info.PrimaryKey = visibleKey
	}

	// This connector always uses unfiltered backfill streaming rather than the "precise" mode
	// in which replication events for not-yet-scanned rows are filtered out by row key. Precise
	// filtering is incompatible with our snapshot/stream handoff: backfill chunks read `AS OF
	// SYSTEM TIME` the latest resolved timestamp, so a filtered-out change to a not-yet-scanned
	// row would be lost if the row's chunk was read before the change committed. Setting
	// UnpredictableKeyOrdering makes the generic backfill logic select the unfiltered strategy
	// by default. See backfill.go and replication.go.
	for _, info := range tableMap {
		info.UnpredictableKeyOrdering = true
	}

	return tableMap, nil
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *cockroachdbDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	var dataType, ok = column.DataType.(cockroachdbTypeDescription)
	if !ok {
		return nil, fmt.Errorf("unable to translate CockroachDB type %v into JSON schema", column.DataType)
	}
	var jsonSchema, err = dataType.JSONSchema(column.IsNullable, isPrimaryKey)
	if err != nil {
		return nil, err
	}
	if column.Description != nil {
		jsonSchema.Description = *column.Description
	}
	return jsonSchema, nil
}

func getTables(ctx context.Context, conn *pgxpool.Pool, selectedSchemas []string) ([]*sqlcapture.DiscoveryInfo, error) {
	logrus.Debug("listing all tables in the database")

	// `relispartition IS NOT TRUE` rather than `NOT relispartition` is required because CockroachDB
	// reports relispartition as NULL, and `NOT NULL` is NULL (not true), which would otherwise
	// silently filter out every table.
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT n.nspname, c.relname")
	fmt.Fprintf(query, "  FROM pg_catalog.pg_class c")
	fmt.Fprintf(query, "  JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)")
	fmt.Fprintf(query, "  WHERE n.nspname NOT IN (%s)", systemSchemaList())
	fmt.Fprintf(query, "    AND c.relkind IN ('r', 'p')")
	fmt.Fprintf(query, "    AND c.relispartition IS NOT TRUE;")

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
			omitBinding = true
		}
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:      tableSchema,
			Name:        tableName,
			BaseTable:   true,
			OmitBinding: omitBinding,
		})
	}
	return tables, rows.Err()
}

// getColumns lists the columns of every table. The `END::text` cast (rather than PostgreSQL's
// `::information_schema.character_data`) is required because CockroachDB doesn't implement that
// internal domain type. Hidden columns (the synthesized `rowid`, hash-shard columns, and
// `crdb_region`) are included but flagged, because primary keys may contain them and the key
// logic in DiscoverTableDetails has to reason about which ones it can capture; they are never
// added to the discovered column set directly.
const queryDiscoverColumns = `
  SELECT nc.nspname as table_schema,
         c.relname as table_name,
         a.attnum as ordinal_position,
         a.attname as column_name,
         NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,
         COALESCE(bt.typname, t.typname) AS udt_name,
         t.typtype::text AS typtype,
         a.attishidden AS is_hidden
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid
    LEFT JOIN (pg_catalog.pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)
           ON t.typtype = 'd'::"char" AND t.typbasetype = bt.oid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND c.relkind IN ('r', 'p')
    ORDER BY nc.nspname, c.relname, a.attnum;`

type cockroachdbColumn struct {
	sqlcapture.ColumnInfo
	Hidden bool
}

func getColumns(ctx context.Context, conn *pgxpool.Pool) ([]cockroachdbColumn, error) {
	logrus.Debug("listing all columns in the database")
	var columns []cockroachdbColumn
	var rows, err = conn.Query(ctx, queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var col cockroachdbColumn
		var columnType, typtype string
		if err := rows.Scan(&col.TableSchema, &col.TableName, &col.Index, &col.Name, &col.IsNullable, &columnType, &typtype, &col.Hidden); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var typeDescription cockroachdbTypeDescription
		switch typtype {
		case "e": // enum
			typeDescription = cockroachdbEnumType{}
		default:
			// Strip the array prefix so we can describe the element type, then wrap it.
			var isArrayType bool
			columnType, isArrayType = strings.CutPrefix(columnType, "_")
			typeDescription = cockroachdbBasicType{Name: columnType}
			if isArrayType {
				typeDescription = cockroachdbArrayType{Items: typeDescription}
			}
		}
		col.DataType = typeDescription
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// Query copied from pgjdbc's PgDatabaseMetaData.getPrimaryKeys() with the always-NULL TABLE_CAT
// column omitted. It works unmodified on CockroachDB, including `information_schema._pg_expandarray`
// and composite primary keys.
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
