package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
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

	objectMapping, err := getTableObjectMappings(ctx, db.config.Advanced.WatermarksTable, db.conn, tables)
	if err != nil {
		return nil, fmt.Errorf("unable to get table object identifiers: %w", err)
	}
	db.tableObjectMapping = objectMapping

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

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *oracleDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var col = column.DataType.(oracleColumnType)

	var jsonType = col.toJSONSchemaType()

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

	switch v := val.(type) {
	case nil:
		return nil, nil
	case string:
		if dataType.jsonType != "string" {
			var out = reflect.New(dataType.t).Interface()
			if err := json.Unmarshal([]byte(v), out); err != nil {
				return nil, err
			}
			return out, nil
		} else {
			return val, nil
		}
	default:
	}

	var rv = reflect.ValueOf(val)
	if rv.CanConvert(dataType.t) {
		return rv.Convert(dataType.t).Interface(), nil
	}
	return val, nil
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

		if columnInfo == nil {
			delete(f, id)
			continue
		}

		var translated, err = translateRecordField(columnInfo, val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

const queryDiscoverTables = `
  SELECT DISTINCT(NVL(IOT_NAME, TABLE_NAME)) AS table_name, owner FROM all_tables WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA') AND owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN') and table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')
`

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

const queryTableObjectIdentifiers = `SELECT OWNER, OBJECT_NAME, OBJECT_ID, DATA_OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE'`

type tableObject struct {
	objectID     int
	dataObjectID int
}

func getTableObjectMappings(ctx context.Context, watermarksTable string, conn *sql.DB, tables []*sqlcapture.DiscoveryInfo) (map[string]tableObject, error) {
	var watermarksTableSplit = strings.Split(watermarksTable, ".")
	var watermarkSchema = watermarksTableSplit[0]
	var watermarkTableName = watermarksTableSplit[0]

	var mapping = make(map[string]tableObject, len(tables))
	var tablesCondition = ""
	for i, table := range tables {
		if i > 0 {
			tablesCondition += " OR "
		}
		tablesCondition += fmt.Sprintf("(OWNER = '%s' AND OBJECT_NAME = '%s')", table.Schema, table.Name)
	}
	tablesCondition += fmt.Sprintf(" OR (OWNER = '%s' AND OBJECT_NAME = '%s')", watermarkSchema, watermarkTableName)

	var fullQuery = fmt.Sprintf("%s AND (%s)", queryTableObjectIdentifiers, tablesCondition)
	logrus.WithField("query", fullQuery).Debug("fetching object identifiers for tables")

	var rows, err = conn.QueryContext(ctx, fullQuery)
	if err != nil {
		return nil, fmt.Errorf("fetching table identifiers: %w", err)
	}
	defer rows.Close()

	var owner, tableName string
	var objectID, dataObjectID int
	for rows.Next() {
		if err := rows.Scan(&owner, &tableName, &objectID, &dataObjectID); err != nil {
			return nil, fmt.Errorf("scanning table object identifier row: %w", err)
		}

		mapping[joinObjectID(objectID, dataObjectID)] = tableObject{objectID: objectID, dataObjectID: dataObjectID}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mapping, nil
}

func joinObjectID(objectID, dataObjectID int) string {
	return fmt.Sprintf("%d.%d", objectID, dataObjectID)
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
	original  string
	length    int
	scale     int16
	precision int16
	t         reflect.Type
	format    string
	jsonType  string
	nullable  bool
}

func (ct oracleColumnType) String() string {
	return ct.original
}

// SMALLINT, INT and INTEGER have a default precision 38 which is not included in the column information
const defaultNumericPrecision = 38

func getColumns(ctx context.Context, conn *sql.DB, tables []*sqlcapture.DiscoveryInfo) ([]sqlcapture.ColumnInfo, map[string][]string, error) {
	var pks = make(map[string][]string)
	var columns []sqlcapture.ColumnInfo
	var sc sqlcapture.ColumnInfo

	var ownersMap = make(map[string]bool)
	for _, t := range tables {
		ownersMap[t.Schema] = true
	}
	var owners []string
	for k := range ownersMap {
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
		var precision int16
		if dataPrecision.Valid {
			precision = dataPrecision.Int16
		} else {
			precision = defaultNumericPrecision
		}

		var t reflect.Type
		var format string
		var jsonType string
		var isInteger = dataScale.Int16 == 0
		if dataType == "NUMBER" && isInteger {
			if precision <= 18 {
				t = reflect.TypeFor[int64]()
				jsonType = "integer"
			} else {
				t = reflect.TypeFor[string]()
				format = "integer"
				jsonType = "string"
			}
		} else if slices.Contains([]string{"FLOAT", "NUMBER"}, dataType) {
			if precision <= 18 {
				t = reflect.TypeFor[float64]()
				jsonType = "number"
			} else {
				t = reflect.TypeFor[string]()
				format = "number"
				jsonType = "string"
			}
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
			logrus.WithFields(logrus.Fields{
				"owner":    sc.TableSchema,
				"table":    sc.TableName,
				"dataType": dataType,
				"column":   sc.Name,
			}).Warn("skipping column, data type is not supported")
			continue
		}

		sc.DataType = oracleColumnType{
			original:  dataType,
			scale:     dataScale.Int16,
			precision: precision,
			length:    dataLength,
			t:         t,
			format:    format,
			jsonType:  jsonType,
			nullable:  sc.IsNullable,
		}

		if isPrimaryKey {
			var streamID = sqlcapture.JoinStreamID(sc.TableSchema, sc.TableName)
			if format == "number" {
				return nil, nil, fmt.Errorf("floating point numbers cannot be primary keys: %s.%s", streamID, sc.Name)
			}

			pks[streamID] = append(pks[streamID], sc.Name)
		}

		columns = append(columns, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columns, pks, nil
}
