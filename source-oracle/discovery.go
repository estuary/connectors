package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

const (
	infinityTimestamp         = "9999-12-31T23:59:59Z"
	negativeInfinityTimestamp = "0000-01-01T00:00:00Z"
	RFC3339TimeFormat         = "15:04:05.999999999Z07:00"
	truncateColumnThreshold   = 8 * 1024 * 1024 // Arbitrarily selected value
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *oracleDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all tables, columns and primary keys in the database
	var tables, err = getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	columns, primaryKeys, err := getColumns(ctx, db.conn, tables)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}

	// Aggregate column and primary key information into DiscoveryInfo structs
	// using a map from fully-qualified "<schema>.<name>" table names to
	// the corresponding DiscoveryInfo.
	var tableMap = make(map[string]*sqlcapture.DiscoveryInfo)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
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

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *oracleDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var col = column.DataType.(oracleColumnType)

	var jsonType *jsonschema.Schema
	jsonType = col.toJSONSchemaType()

	// Pass-through Oracle column description
	if column.Description != nil {
		jsonType.Description = *column.Description
	}
	return jsonType, nil
}

func translateRecordField(column *sqlcapture.ColumnInfo, val interface{}) (interface{}, error) {
	var dataType oracleColumnType
	if column != nil {
		dataType = column.DataType.(oracleColumnType)
	} else {
		return val, nil
	}

	logrus.WithFields(logrus.Fields{
		"col":               column,
		"val":               val,
		"dataType.jsonType": dataType.jsonType,
		"dataType.t":        dataType.t,
	}).Trace("translateRecordField")
	var out = reflect.New(dataType.t).Interface()
	switch v := val.(type) {
	case nil:
		return nil, nil
	case string:
		if dataType.jsonType != "string" {
			if err := json.Unmarshal([]byte(v), out); err != nil {
				return nil, err
			}
		} else {
			out = val
		}
	default:
		var rv = reflect.ValueOf(val)
		if rv.CanConvert(dataType.t) {
			out = rv.Convert(dataType.t).Interface()
		} else {
			out = val
		}
	}

	return out, nil
}

func (ct *oracleColumnType) toJSONSchemaType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format: ct.format,
		Extras: make(map[string]interface{}),
	}

	if ct.jsonType == "" {
		// No type constraint.
	} else if ct.nullable {
		out.Extras["type"] = []string{ct.jsonType, "null"} // Use variadic form.
	} else {
		out.Type = ct.jsonType
	}
	return out
}

func translateRecordFields(table *sqlcapture.DiscoveryInfo, f map[string]interface{}) error {
	if f == nil {
		return nil
	}
	for id, val := range f {
		var columnInfo *sqlcapture.ColumnInfo
		if table != nil {
			if info, ok := table.Columns[id]; ok {
				columnInfo = &info
			}
		}

		var translated, err = translateRecordField(columnInfo, val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func oversizePlaceholderJSON(orig []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(orig)))
}

func formatRFC3339(t time.Time) (any, error) {
	if t.Year() < 0 || t.Year() > 9999 {
		// We could in theory clamp excessively large years to positive infinity, but this
		// is of limited usefulness since these are never real dates, they're mostly just
		// dumb typos like `20221` and so we might as well normalize errors consistently.
		return negativeInfinityTimestamp, nil
	}
	return t.Format(time.RFC3339Nano), nil
}

type columnSchema struct {
	contentEncoding string
	format          string
	nullable        bool
	jsonType        string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format: s.format,
		Extras: make(map[string]interface{}),
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}

	if s.jsonType == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.jsonType, "null"} // Use variadic form.
	} else {
		out.Type = s.jsonType
	}
	return out
}

const queryDiscoverTables = `
  SELECT DISTINCT(NVL(IOT_NAME, TABLE_NAME)) AS table_name, owner FROM all_tables WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA') AND owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS') and table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')
` // 'r' means "Ordinary Table" and 'p' means "Partitioned Table"

func getTables(ctx context.Context, conn *sql.DB, selectedSchemas []string) ([]*sqlcapture.DiscoveryInfo, error) {
	logrus.Debug("listing all tables in the database")
	var tables []*sqlcapture.DiscoveryInfo
	var q = queryDiscoverTables
	if len(selectedSchemas) > 0 {
		var schemasComma = strings.Join(selectedSchemas, "','")
		q = q + fmt.Sprintf(" AND owner IN ('%s')", schemasComma)
	}
	var rows, err = conn.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("fetching tables: %w", err)
	}
	defer rows.Close()

	var owner, tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName, &owner); err != nil {
			return nil, fmt.Errorf("scanning table row: %w", err)
		}

		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:      owner,
			Name:        tableName,
			BaseTable:   true,
			OmitBinding: false,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

const queryDiscoverColumns = `
SELECT t.owner, t.table_name, t.column_id, t.column_name, t.nullable, t.data_type, t.data_precision, t.data_scale, t.data_length, NVL2(c.constraint_type, 1, 0) as COL_IS_PK FROM all_tab_columns t
    LEFT JOIN (
            SELECT c.owner, c.table_name, c.constraint_type, ac.column_name FROM all_constraints c
                INNER JOIN all_cons_columns ac ON (
                    c.constraint_name = ac.constraint_name
                    AND c.table_name = ac.table_name
                    AND c.owner = ac.owner
                    AND c.constraint_type = 'P'
                )
            ) c
    ON (t.owner = c.owner AND t.table_name = c.table_name AND t.column_name = c.column_name)`

type oracleColumnType struct {
	original string
	length   int
	scale    int16
	t        reflect.Type
	format   string
	jsonType string
	nullable bool
}

func getColumns(ctx context.Context, conn *sql.DB, tables []*sqlcapture.DiscoveryInfo) ([]sqlcapture.ColumnInfo, map[string][]string, error) {
	var pks = make(map[string][]string)
	var columns []sqlcapture.ColumnInfo
	var sc sqlcapture.ColumnInfo

	var ownersMap = make(map[string]bool)
	for _, t := range tables {
		ownersMap[t.Schema] = true
	}
	var owners []string
	for k, _ := range ownersMap {
		owners = append(owners, k)
	}
	var ownersCondition = " WHERE t.owner IN ('" + strings.Join(owners, "','") + "')"

	var rows, err = conn.QueryContext(ctx, queryDiscoverColumns+ownersCondition)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var isPrimaryKey bool
		var isNullableStr string
		var dataScale sql.NullInt16
		var dataLength int
		var dataPrecision sql.NullInt16
		var dataType string
		if err := rows.Scan(&sc.TableSchema, &sc.TableName, &sc.Index, &sc.Name, &isNullableStr, &dataType, &dataPrecision, &dataScale, &dataLength, &isPrimaryKey); err != nil {
			return nil, nil, fmt.Errorf("scanning column: %w", err)
		}

		if isNullableStr == "Y" {
			sc.IsNullable = true
		}

		var t reflect.Type
		var format string
		var jsonType string
		if dataType == "NUMBER" && (dataScale.Valid && dataScale.Int16 == 0 && dataPrecision.Valid && dataPrecision.Int16 <= 18) {
			t = reflect.TypeFor[int64]()
			jsonType = "integer"
		} else if slices.Contains([]string{"NUMBER", "DOUBLE", "FLOAT"}, dataType) {
			if isPrimaryKey || dataPrecision.Int16 == 0 || dataPrecision.Int16 > 18 {
				t = reflect.TypeFor[string]()
				format = "number"
				jsonType = "string"
			} else {
				t = reflect.TypeFor[float64]()
				jsonType = "number"
			}
		} else if slices.Contains([]string{"INTEGER", "SMALLINT"}, dataType) {
			t = reflect.TypeFor[string]()
			format = "number"
			jsonType = "string"
		} else if slices.Contains([]string{"CHAR", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"}, dataType) {
			t = reflect.TypeFor[string]()
			jsonType = "string"
		} else if strings.Contains(dataType, "WITH TIME ZONE") {
			t = reflect.TypeFor[string]()
			jsonType = "string"
			format = "date-time"
		} else if dataType == "DATE" || strings.Contains(dataType, "TIMESTAMP") {
			t = reflect.TypeFor[string]()
			jsonType = "string"
		} else if strings.Contains(dataType, "INTERVAL") {
			t = reflect.TypeFor[string]()
			jsonType = "string"
		} else if slices.Contains([]string{"CLOB", "RAW"}, dataType) {
			t = reflect.TypeFor[[]byte]()
			jsonType = "string"
		} else {
			return nil, nil, fmt.Errorf("unsupported data type: %s", dataType)
		}

		sc.DataType = oracleColumnType{
			original: dataType,
			scale:    dataScale.Int16,
			length:   dataLength,
			t:        t,
			format:   format,
			jsonType: jsonType,
			nullable: sc.IsNullable,
		}

		if isPrimaryKey {
			var streamID = sqlcapture.JoinStreamID(sc.TableSchema, sc.TableName)

			pks[streamID] = append(pks[streamID], sc.Name)
		}

		columns = append(columns, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, pks, nil
}
