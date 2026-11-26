package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
)

const discoveryChunkSize = 100

// ListTables returns user tables with active SQL Replication registrations.
func (db *db2Database) ListTables(ctx context.Context) ([]sqlcapture.TableID, error) {
	var register = quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER")
	var query = fmt.Sprintf(`
SELECT DISTINCT T.TABSCHEMA, T.TABNAME
  FROM SYSCAT.TABLES T
  JOIN %s R ON R.SOURCE_OWNER = T.TABSCHEMA AND R.SOURCE_TABLE = T.TABNAME
  WHERE T.TYPE IN ('T', 'U') AND R.STATE = 'A'
    AND NOT EXISTS (SELECT 1 FROM %s CD WHERE CD.CD_OWNER = T.TABSCHEMA AND CD.CD_TABLE = T.TABNAME)
  ORDER BY T.TABSCHEMA, T.TABNAME`, register, register)
	var rows, err = db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("unable to list SQL Replication registered tables: %w; ensure %s exists and run GRANT SELECT ON TABLE %s TO USER %s", err, register, register, quoteIdentifier(db.config.User))
	}
	defer rows.Close()

	var tables []sqlcapture.TableID
	for rows.Next() {
		var table sqlcapture.TableID
		if err := rows.Scan(&table.Schema, &table.Table); err != nil {
			return nil, fmt.Errorf("error scanning discovered table: %w", err)
		}
		if db.discoverableTable(table) {
			tables = append(tables, table)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating discovered tables: %w", err)
	}
	return tables, nil
}

func (db *db2Database) discoverableTable(table sqlcapture.TableID) bool {
	var schema = strings.ToUpper(table.Schema)
	if strings.HasPrefix(schema, "SYS") || schema == "ASN" || schema == "SQLJ" || table.Schema == db.config.Advanced.CaptureSchema {
		return false
	}
	if sqlcapture.JoinStreamID(table.Schema, table.Table) == db.WatermarksTable() {
		return false
	}
	return len(db.config.Advanced.DiscoverSchemas) == 0 || slices.Contains(db.config.Advanced.DiscoverSchemas, table.Schema)
}

type captureRegistration struct {
	CDSchema             string
	CDTable              string
	State                string
	UpdateAsDeleteInsert bool
	BeforeImagePrefix    string
}

type db2TableDiscoveryDetails struct {
	DataCaptureChanges bool
	Registration       *captureRegistration
}

// DiscoverTableDetails reads types from the source tables, not their CD tables.
func (db *db2Database) DiscoverTableDetails(ctx context.Context, requested []sqlcapture.TableID) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	// Watermarks are hidden from discovered bindings but needed for backfill handover.
	var watermark = db.WatermarksTable()
	var tables = slices.Clone(requested)
	if !slices.ContainsFunc(tables, func(table sqlcapture.TableID) bool {
		return sqlcapture.JoinStreamID(table.Schema, table.Table) == watermark
	}) {
		tables = append(tables, sqlcapture.TableID{Schema: watermark.Schema, Table: watermark.Table})
	}

	var discovered = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	for start := 0; start < len(tables); start += discoveryChunkSize {
		var chunk = tables[start:min(start+discoveryChunkSize, len(tables))]
		if err := db.discoverTableChunk(ctx, discovered, chunk); err != nil {
			return nil, err
		}
	}
	return discovered, nil
}

func tableIDsPredicate(schemaColumn, tableColumn string, tables []sqlcapture.TableID) (string, []any) {
	var terms = make([]string, len(tables))
	var args = make([]any, 0, len(tables)*2)
	for i, table := range tables {
		terms[i] = fmt.Sprintf("(%s = ? AND %s = ?)", schemaColumn, tableColumn)
		args = append(args, table.Schema, table.Table)
	}
	return "(" + strings.Join(terms, " OR ") + ")", args
}

func (db *db2Database) discoverTableChunk(ctx context.Context, discovered map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, requested []sqlcapture.TableID) error {
	var tables, err = db.getTables(ctx, requested)
	if err != nil {
		return err
	}
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
		if _, ok := discovered[streamID]; ok {
			return fmt.Errorf("multiple discovery results for table %q", streamID)
		}
		table.OmitBinding = streamID == db.WatermarksTable()
		table.UseSchemaInference = db.featureFlags["use_schema_inference"]
		table.EmitSourcedSchemas = db.featureFlags["emit_sourced_schemas"]
		discovered[streamID] = table
	}

	columns, err := getColumns(ctx, db.conn, requested)
	if err != nil {
		return err
	}
	for _, column := range columns {
		if table := discovered[sqlcapture.JoinStreamID(column.TableSchema, column.TableName)]; table != nil {
			table.Columns[column.Name] = column
			table.ColumnNames = append(table.ColumnNames, column.Name)
		}
	}
	keys, err := getPrimaryKeys(ctx, db.conn, requested)
	if err != nil {
		return err
	}
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
		table.PrimaryKey = keys[streamID]
		for _, name := range db.tableKey(table) {
			var column, ok = table.Columns[name]
			if !ok {
				return fmt.Errorf("key column %q missing from table %q", name, streamID)
			}
			if !column.DataType.(db2ColumnType).predictableKeyOrdering() {
				table.UnpredictableKeyOrdering = true
			}
		}
	}
	return nil
}

func (db *db2Database) getTables(ctx context.Context, requested []sqlcapture.TableID) ([]*sqlcapture.DiscoveryInfo, error) {
	var predicate, args = tableIDsPredicate("T.TABSCHEMA", "T.TABNAME", requested)
	var query = fmt.Sprintf(`
SELECT T.TABSCHEMA, T.TABNAME, T.TYPE, T.DATACAPTURE,
       R.CD_OWNER, R.CD_TABLE, R.STATE, R.CHG_UPD_TO_DEL_INS, R.BEFORE_IMG_PREFIX
  FROM SYSCAT.TABLES T
  LEFT JOIN %s R ON R.SOURCE_OWNER = T.TABSCHEMA AND R.SOURCE_TABLE = T.TABNAME
  WHERE %s`, quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER"), predicate)
	var rows, err = db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables and SQL Replication registrations: %w", err)
	}
	defer rows.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for rows.Next() {
		var table sqlcapture.DiscoveryInfo
		var tableType, dataCapture string
		var cdSchema, cdTable, state, updates, prefix sql.NullString
		if err := rows.Scan(&table.Schema, &table.Name, &tableType, &dataCapture, &cdSchema, &cdTable, &state, &updates, &prefix); err != nil {
			return nil, fmt.Errorf("error scanning table details: %w", err)
		}
		var details = &db2TableDiscoveryDetails{DataCaptureChanges: dataCapture != "N"}
		if cdSchema.Valid && cdTable.Valid {
			details.Registration = &captureRegistration{
				CDSchema:             strings.TrimRight(cdSchema.String, " "),
				CDTable:              strings.TrimRight(cdTable.String, " "),
				State:                strings.TrimSpace(state.String),
				UpdateAsDeleteInsert: strings.TrimSpace(updates.String) == "Y",
				BeforeImagePrefix:    strings.TrimRight(prefix.String, " "),
			}
		}
		table.BaseTable = tableType == "T" || tableType == "U"
		table.Columns = make(map[string]sqlcapture.ColumnInfo)
		table.ExtraDetails = details
		tables = append(tables, &table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table details: %w", err)
	}
	return tables, nil
}

func getColumns(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) ([]sqlcapture.ColumnInfo, error) {
	var predicate, args = tableIDsPredicate("C.TABSCHEMA", "C.TABNAME", requested)
	var query = fmt.Sprintf(`
SELECT C.TABSCHEMA, C.TABNAME, C.COLNAME, C.COLNO, C.NULLS, C.TYPENAME, C.LENGTH, C.SCALE, C.CODEPAGE, D.SOURCENAME
  FROM SYSCAT.COLUMNS C
  LEFT JOIN SYSCAT.DATATYPES D ON D.TYPESCHEMA = C.TYPESCHEMA AND D.TYPENAME = C.TYPENAME
    AND D.METATYPE = 'D' AND D.SOURCESCHEMA = 'SYSIBM'
  WHERE %s
  ORDER BY C.TABSCHEMA, C.TABNAME, C.COLNO`, predicate)
	var rows, err = conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error discovering source columns: %w", err)
	}
	defer rows.Close()

	var columns []sqlcapture.ColumnInfo
	for rows.Next() {
		var column sqlcapture.ColumnInfo
		var nullable string
		var columnType db2ColumnType
		var sourceType sql.NullString
		if err := rows.Scan(&column.TableSchema, &column.TableName, &column.Name, &column.Index, &nullable, &columnType.Name, &columnType.Length, &columnType.Scale, &columnType.Codepage, &sourceType); err != nil {
			return nil, fmt.Errorf("error scanning source column: %w", err)
		}
		if sourceType.Valid {
			columnType.Name = sourceType.String
		}
		columnType.Name = strings.ToUpper(strings.TrimSpace(columnType.Name))
		column.IsNullable = nullable == "Y"
		column.DataType = columnType
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating source columns: %w", err)
	}
	return columns, nil
}

func getPrimaryKeys(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) (map[sqlcapture.StreamID][]string, error) {
	var predicate, args = tableIDsPredicate("K.TABSCHEMA", "K.TABNAME", requested)
	var query = fmt.Sprintf(`
SELECT K.TABSCHEMA, K.TABNAME, K.COLNAME, K.COLSEQ
  FROM SYSCAT.KEYCOLUSE K
  JOIN SYSCAT.TABCONST C ON C.TABSCHEMA = K.TABSCHEMA AND C.TABNAME = K.TABNAME AND C.CONSTNAME = K.CONSTNAME
  WHERE C.TYPE = 'P' AND %s
  ORDER BY K.TABSCHEMA, K.TABNAME, K.COLSEQ`, predicate)
	var rows, err = conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error discovering primary keys: %w", err)
	}
	defer rows.Close()

	var keys = make(map[sqlcapture.StreamID][]string)
	for rows.Next() {
		var schema, table, column string
		var index int
		if err := rows.Scan(&schema, &table, &column, &index); err != nil {
			return nil, fmt.Errorf("error scanning primary key column: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(schema, table)
		keys[streamID] = append(keys[streamID], column)
		if index != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q of table %q appears out of order", column, streamID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %w", err)
	}
	return keys, nil
}
