package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for thos tables.
func (drv *BatchSQLDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var db, err = drv.Connect(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tables, err := discoverTables(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	keys, err := discoverPrimaryKeys(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing primary keys: %w", err)
	}

	// We rebuild this map even though `discoverPrimaryKeys` already built and
	// discarded it, in order to keep the `discoverTables` / `discoverPrimaryKeys`
	// interface as stupidly simple as possible, which ought to make it just that
	// little bit easier to do this generically for multiple databases.
	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for _, key := range keys {
		keysByTable[key.Owner+"."+key.Table] = key
	}

	var bindings []*pc.Response_Discovered_Binding
	for _, table := range tables {
		var tableID = table.Owner + "." + table.Name

		var recommendedName = recommendedCatalogName(table.Owner, table.Name)
		var res, err = drv.GenerateResource(recommendedName, table.Owner, table.Name, "TABLE")
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
				"table":  tableID,
			}).Warn("unable to generate resource spec")
			continue
		}
		resourceConfigJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		// Try to generate a useful collection schema, but on error fall back to the
		// minimal schema with the default key [/_meta/polled, /_meta/index].
		var collectionSchema = minimalSchema
		var collectionKey = fallbackKey
		if tableKey, ok := keysByTable[tableID]; ok {
			if generatedSchema, err := generateCollectionSchema(tableKey.Columns, tableKey.ColumnTypes); err == nil {
				collectionSchema = generatedSchema
				collectionKey = nil
				for _, colName := range tableKey.Columns {
					collectionKey = append(collectionKey, primaryKeyToCollectionKey(colName))
				}
			} else {
				log.WithFields(log.Fields{"table": tableID, "err": err}).Warn("unable to generate collection schema")
			}
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: collectionSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

// The fallback collection key just refers to the polling iteration and result index of each document.
var fallbackKey = []string{"/_meta/polled", "/_meta/index"}

func generateCollectionSchema(keyColumns []string, columnTypes map[string]*jsonschema.Schema) (json.RawMessage, error) {
	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

	var required = []string{"_meta"}
	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}

	for _, colName := range keyColumns {
		var columnType = columnTypes[colName]
		if columnType == nil {
			return nil, fmt.Errorf("unable to add key column %q to schema: type unknown", colName)
		}
		properties[colName] = columnType
		required = append(required, colName)
	}

	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             required,
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties":     properties,
			"x-infer-schema": true,
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("error serializing schema: %w", err)
	}
	return json.RawMessage(bs), nil
}

var minimalSchema = func() json.RawMessage {
	var schema, err = generateCollectionSchema(nil, nil)
	if err != nil {
		panic(err)
	}
	return schema
}()

// primaryKeyToCollectionKey converts a database primary key column name into a Flow collection key
// JSON pointer with escaping for '~' and '/' applied per RFC6901.
func primaryKeyToCollectionKey(key string) string {
	// Any encoded '~' must be escaped first to prevent a second escape on escaped '/' values as
	// '~1'.
	key = strings.ReplaceAll(key, "~", "~0")
	key = strings.ReplaceAll(key, "/", "~1")
	return "/" + key
}

type discoveredTable struct {
	Owner string
	Name  string
}

func discoverTables(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredTable, error) {
	var query = new(strings.Builder)

	fmt.Fprintf(query, "SELECT DISTINCT(NVL(IOT_NAME, TABLE_NAME)) AS table_name,")
	fmt.Fprintf(query, "owner FROM all_tables")
	fmt.Fprintf(query, " WHERE tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'SAMPLESCHEMA')")
	fmt.Fprintf(query, " AND owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN', 'GSMADMIN_INTERNAL')")
	fmt.Fprintf(query, " AND table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')")

	if len(discoverSchemas) > 0 {
		var schemasComma = strings.Join(discoverSchemas, "','")
		fmt.Fprintf(query, " AND owner IN ('%s')", schemasComma)
	}

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var tables []*discoveredTable
	for rows.Next() {
		var tableOwner, tableName string
		if err := rows.Scan(&tableName, &tableOwner); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		tables = append(tables, &discoveredTable{
			Owner: tableOwner,
			Name:  tableName,
		})
	}
	return tables, nil
}

type discoveredPrimaryKey struct {
	Owner       string
	Table       string
	Columns     []string
	ColumnTypes map[string]*jsonschema.Schema
}

// SMALLINT, INT and INTEGER have a default precision 38 which is not included in the column information
const defaultNumericPrecision = 38

func discoverPrimaryKeys(ctx context.Context, db *sql.DB, discoverSchemas []string) ([]*discoveredPrimaryKey, error) {
	var query = new(strings.Builder)

	fmt.Fprintf(query, "SELECT t.owner, t.table_name, c.position, t.column_name,")
	fmt.Fprintf(query, "t.data_type, t.data_precision, t.data_scale, t.data_length")
	fmt.Fprintf(query, " FROM all_tab_columns t")
	fmt.Fprintf(query, " INNER JOIN (")
	fmt.Fprintf(query, "   SELECT c.owner, c.table_name, c.constraint_type, ac.column_name, ac.position FROM all_constraints c")
	fmt.Fprintf(query, "     INNER JOIN all_cons_columns ac ON (")
	fmt.Fprintf(query, "         c.constraint_name = ac.constraint_name")
	fmt.Fprintf(query, "         AND c.table_name = ac.table_name")
	fmt.Fprintf(query, "         AND c.owner = ac.owner")
	fmt.Fprintf(query, "         AND c.constraint_type = 'P'")
	fmt.Fprintf(query, "       )")
	fmt.Fprintf(query, "     ) c")
	fmt.Fprintf(query, " ON (t.owner = c.owner AND t.table_name = c.table_name AND t.column_name = c.column_name)")
	fmt.Fprintf(query, " WHERE t.owner NOT IN ('SYS', 'SYSTEM', 'AUDSYS', 'CTXSYS', 'DVSYS', 'DBSFWUSER', 'DBSNMP', 'QSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RDSADMIN', 'OUTLN', 'WMSYS', 'XDB', 'RMAN$CATALOG', 'MTSSYS', 'OML$METADATA', 'ODI_REPO_USER', 'RQSYS', 'PYQSYS', 'GGS_ADMIN', 'GSMADMIN_INTERNAL')")
	fmt.Fprintf(query, " AND t.table_name NOT IN ('DBTOOLS$EXECUTION_HISTORY')")
	fmt.Fprintf(query, " ORDER BY t.table_name, c.position")

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query %q: %w", query.String(), err)
	}
	defer rows.Close()

	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for rows.Next() {
		var tableOwner, tableName, columnName, dataType string
		var dataScale sql.NullInt16
		var dataLength int
		var dataPrecision sql.NullInt16
		var ordinalPosition int

		if err := rows.Scan(&tableOwner, &tableName, &ordinalPosition, &columnName, &dataType, &dataPrecision, &dataScale, &dataLength); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var precision int16
		if dataPrecision.Valid {
			precision = dataPrecision.Int16
		} else {
			precision = defaultNumericPrecision
		}

		var format string
		var jsonType string
		var isInteger = dataScale.Int16 == 0
		if dataType == "NUMBER" && !dataScale.Valid && !dataPrecision.Valid {
			// when scale and precision are both null, both have the maximum value possible
			// equivalent to NUMBER(38, 127)
			format = "number"
			jsonType = "string"
		} else if dataType == "NUMBER" && isInteger {
			// data_precision null defaults to precision 38
			if precision > 18 || !dataPrecision.Valid {
				format = "integer"
				jsonType = "string"
			} else {
				jsonType = "integer"
			}
		} else if slices.Contains([]string{"FLOAT", "NUMBER"}, dataType) {
			if precision > 18 || !dataPrecision.Valid {
				format = "number"
				jsonType = "string"
			} else {
				jsonType = "number"
			}
		} else if slices.Contains([]string{"CHAR", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"}, dataType) {
			jsonType = "string"
		} else if strings.Contains(dataType, "WITH TIME ZONE") {
			jsonType = "string"
			format = "date-time"
		} else if dataType == "DATE" || strings.Contains(dataType, "TIMESTAMP") {
			jsonType = "string"
		} else if strings.Contains(dataType, "INTERVAL") {
			jsonType = "string"
		} else if slices.Contains([]string{"CLOB", "RAW"}, dataType) {
			jsonType = "string"
		} else {
			log.WithFields(log.Fields{
				"owner":    tableOwner,
				"table":    tableName,
				"dataType": dataType,
				"column":   columnName,
			}).Warn("skipping column, data type is not supported")
			continue
		}

		var tableID = tableOwner + "." + tableName
		var keyInfo = keysByTable[tableID]
		if keyInfo == nil {
			keyInfo = &discoveredPrimaryKey{
				Owner:       tableOwner,
				Table:       tableName,
				ColumnTypes: make(map[string]*jsonschema.Schema),
			}
			keysByTable[tableID] = keyInfo
		}
		keyInfo.Columns = append(keyInfo.Columns, columnName)
		keyInfo.ColumnTypes[columnName] = &jsonschema.Schema{
			Type:   jsonType,
			Format: format,
		}
		if ordinalPosition != len(keyInfo.Columns) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, tableID)
		}
	}

	var keys []*discoveredPrimaryKey
	for _, key := range keysByTable {
		keys = append(keys, key)
	}
	return keys, nil
}

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	var catalogName = schema + "_" + table
	return catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(catalogName), "_")
}
