package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func (snowflakeDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg = new(config)
	if err := pf.UnmarshalStrict(req.ConfigJson, cfg); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	cfg.SetDefaults()

	log.Info("performing discovery")

	var db, err = connectSnowflake(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to snowflake: %w", err)
	}
	defer db.Close()

	discoveryResults, err := performSnowflakeDiscovery(ctx, cfg, db)
	if err != nil {
		return nil, fmt.Errorf("error discovering snowflake collections: %w", err)
	}

	bindings, err := catalogFromDiscovery(cfg, discoveryResults)
	if err != nil {
		return nil, fmt.Errorf("error converting discovery info to bindings: %w", err)
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

type snowflakeDiscoveryResults struct {
	Tables      []*snowflakeDiscoveryTable
	Columns     []*snowflakeDiscoveryColumn
	PrimaryKeys []*snowflakeDiscoveryPrimaryKey
}

type snowflakeDiscoveryTable struct {
	Database string `db:"database_name"`
	Schema   string `db:"schema_name"`
	Name     string `db:"name"`
	Kind     string `db:"kind"`
	Comment  string `db:"comment"`
}

type snowflakeDiscoveryColumn struct {
	Database         string  `db:"TABLE_CATALOG"`
	Schema           string  `db:"TABLE_SCHEMA"`
	Table            string  `db:"TABLE_NAME"`
	Name             string  `db:"COLUMN_NAME"`
	Index            int     `db:"ORDINAL_POSITION"`
	DataType         string  `db:"DATA_TYPE"`
	Comment          *string `db:"COMMENT"`
	IsNullable       string  `db:"IS_NULLABLE"`
	NumericScale     *int    `db:"NUMERIC_SCALE"`
	NumericPrecision *int    `db:"NUMERIC_PRECISION"`
}

type snowflakeDiscoveryPrimaryKey struct {
	Database   string `db:"database_name"`
	Schema     string `db:"schema_name"`
	Table      string `db:"table_name"`
	ColumnName string `db:"column_name"`
	KeySeq     int    `db:"key_sequence"`
}

func performSnowflakeDiscovery(ctx context.Context, cfg *config, db *sql.DB) (*snowflakeDiscoveryResults, error) {
	log.Debug("performing discovery")

	// We are forced to use an inconsistent mix of `SELECT ... FROM information_schema` and
	// `SHOW FOO` queries to gather required discovery information, because:
	//
	//   1. The query `SHOW COLUMNS` doesn't give us all the information we need,
	//      so we're forced to query 'information_schema.columns' for that.
	//   2. There is no 'information_schema' source from which we can discover the
	//      list of primary-key columns corresponding to a table, so we're forced
	//      to use `SHOW PRIMARY KEYS` for that info.
	//
	// In principle the list of tables names could be gotten either way and the choice to
	// use `SHOW TABLES` is entirely arbitrary.
	//
	// We wrap the database with SQLX so we can use convenient struct-tag reflection rather
	// than hard-coded tuple indices to translate query results into lists-of-structs.
	var xdb = sqlx.NewDb(db, "snowflake").Unsafe()
	var tables []*snowflakeDiscoveryTable
	if err := xdb.Select(&tables, "SHOW TABLES;"); err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	var columns []*snowflakeDiscoveryColumn
	if err := xdb.Select(&columns, "SELECT * FROM information_schema.columns;"); err != nil {
		return nil, fmt.Errorf("error listing columns: %w", err)
	}
	var primaryKeysQuery = fmt.Sprintf("SHOW PRIMARY KEYS IN DATABASE %s;", quoteSnowflakeIdentifier(cfg.Database))
	var primaryKeys []*snowflakeDiscoveryPrimaryKey
	if err := xdb.Select(&primaryKeys, primaryKeysQuery); err != nil {
		return nil, fmt.Errorf("error listing primaryKeys: %w", err)
	}
	// The results of `SHOW PRIMARY KEYS` are not guaranteed to be sequential in
	// key order (empirically they appear to be in sequential *table* order), so
	// we have to sort that ourselves before processing.
	slices.SortStableFunc(primaryKeys, func(a, b *snowflakeDiscoveryPrimaryKey) int {
		return cmp.Compare(a.KeySeq, b.KeySeq)
	})
	return &snowflakeDiscoveryResults{
		Tables:      tables,
		Columns:     columns,
		PrimaryKeys: primaryKeys,
	}, nil
}

func recommendCollectionName(table snowflakeObject) string {
	if table.Schema != "PUBLIC" {
		return strings.ToLower(table.Schema + "_" + table.Name)
	}
	return strings.ToLower(table.Name)
}

type snowflakeDiscoveryInfo struct {
	Table      *snowflakeDiscoveryTable
	Columns    []*snowflakeDiscoveryColumn
	PrimaryKey []string
}

func catalogFromDiscovery(cfg *config, info *snowflakeDiscoveryResults) ([]*pc.Response_Discovered_Binding, error) {
	log.Debug("translating discovery results into bindings")

	// Aggregate information about the available streams.
	var streams = make(map[snowflakeObject]*snowflakeDiscoveryInfo)
	for _, discoveredTable := range info.Tables {
		if discoveredTable.Database != cfg.Database {
			return nil, fmt.Errorf("internal error: discovery results from other databases (this should never happen)")
		}
		if discoveredTable.Schema == cfg.Advanced.FlowSchema {
			// Ignore objects in the 'Flow' schema so we don't accidentally discover staging tables.
			continue
		}
		var tableID = snowflakeObject{discoveredTable.Schema, discoveredTable.Name}
		if !strings.EqualFold(discoveredTable.Kind, "TABLE") {
			log.WithFields(log.Fields{"table": tableID.String(), "kind": discoveredTable.Kind}).Trace("ignoring non-table entity")
			continue
		}
		streams[tableID] = &snowflakeDiscoveryInfo{Table: discoveredTable}
	}
	for _, discoveredColumn := range info.Columns {
		if discoveredColumn.Database != cfg.Database {
			return nil, fmt.Errorf("internal error: discovery results from other databases (this should never happen)")
		}
		var tableID = snowflakeObject{discoveredColumn.Schema, discoveredColumn.Table}
		if streams[tableID] == nil {
			continue
		}
		streams[tableID].Columns = append(streams[tableID].Columns, discoveredColumn)
	}
	for _, discoveredPK := range info.PrimaryKeys {
		if discoveredPK.Database != cfg.Database {
			return nil, fmt.Errorf("internal error: discovery results from other databases (this should never happen)")
		}
		var tableID = snowflakeObject{discoveredPK.Schema, discoveredPK.Table}
		if streams[tableID] == nil {
			continue
		}
		streams[tableID].PrimaryKey = append(streams[tableID].PrimaryKey, discoveredPK.ColumnName)
		if len(streams[tableID].PrimaryKey) != discoveredPK.KeySeq {
			log.WithFields(log.Fields{
				"stream": tableID.String(),
				"pkey":   streams[tableID].PrimaryKey,
				"seq":    discoveredPK.KeySeq,
			}).Warn("primary key sequence mismatch")
			return nil, fmt.Errorf("internal error: primary key sequence mismatch")
		}
	}

	// Translate aggregated information into pc.Response_Discovered_Binding structs.
	var bindings []*pc.Response_Discovered_Binding
	for tableID, info := range streams {
		var recommendedName = recommendCollectionName(tableID)

		// If no primary key is identified by discovery, the suggested collection key
		// here will remain nil.
		var primaryKey []string
		for _, column := range info.PrimaryKey {
			primaryKey = append(primaryKey, primaryKeyToCollectionKey(column))
		}

		resourceConfigJSON, err := json.Marshal(resource{
			Schema: info.Table.Schema,
			Table:  info.Table.Name,
		})
		if err != nil {
			return nil, fmt.Errorf("internal error serializing resource config: %w", err)
		}

		documentSchemaJSON, err := schemaFromDiscovery(info)
		if err != nil {
			return nil, fmt.Errorf("error generating document schema for %q: %w", info.Table.Name, err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: documentSchemaJSON,
			Key:                primaryKey,
		})
	}

	return bindings, nil
}

// primaryKeyToCollectionKey converts a database primary key column name into a Flow collection key
// JSON pointer with escaping for '~' and '/' applied per RFC6901.
func primaryKeyToCollectionKey(key string) string {
	return "/" + escapeTildes(key)
}

func schemaFromDiscovery(info *snowflakeDiscoveryInfo) (json.RawMessage, error) {
	// The anchor by which we'll reference the table schema.
	var anchor = strings.Title(info.Table.Schema) + strings.Title(info.Table.Name)
	if info.Table.Schema == "PUBLIC" {
		anchor = strings.Title(info.Table.Name)
	}

	// Schema of the embedded "source metadata" property
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: true,
	}).Reflect(new(snowflakeSourceMetadata))
	sourceSchema.Version = ""

	// Build `properties` schemas for each table column.
	var properties = make(map[string]*jsonschema.Schema)
	for _, column := range info.Columns {
		var jsonType, err = translateDBToJSONType(column)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": err,
				"type":  column.DataType,
			}).Warn("error translating column type to JSON schema")

			// Logging an error from the connector is nice, but can be swallowed by `flowctl-go`.
			// Putting an error in the generated schema is ugly, but makes the failure visible.
			properties[column.Name] = &jsonschema.Schema{
				Description: fmt.Sprintf("using catch-all schema: %v", err),
			}
		} else {
			properties[column.Name] = jsonType
		}
	}

	// Schema.Properties is a weird OrderedMap thing, which doesn't allow for inline
	// literal construction. Instead, use the Schema.Extras mechanism with "properties"
	// to generate the properties keyword with an inline map.
	var schema = jsonschema.Schema{
		Definitions: jsonschema.Definitions{
			anchor: &jsonschema.Schema{
				Type: "object",
				Extras: map[string]interface{}{
					"$anchor":    anchor,
					"properties": properties,
				},
				Required: info.PrimaryKey,
			},
		},
		AllOf: []*jsonschema.Schema{
			{
				Extras: map[string]interface{}{
					"properties": map[string]*jsonschema.Schema{
						metadataProperty: {
							Type: "object",
							Extras: map[string]interface{}{
								"properties": map[string]*jsonschema.Schema{
									"op": {
										Enum:        []interface{}{"c", "d", "u"},
										Description: "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
									},
									"source": sourceSchema,
									"before": {
										Ref:         "#" + anchor,
										Description: "Record state immediately before this change was applied.",
										Extras: map[string]interface{}{
											"reduce": map[string]interface{}{
												"strategy": "firstWriteWins",
											},
										},
									},
								},
								"reduce": map[string]interface{}{
									"strategy": "merge",
								},
							},
							Required: []string{"op", "source"},
						},
					},
					"reduce": map[string]interface{}{
						"strategy": "merge",
					},
				},
				Required: []string{metadataProperty},
			},
			{Ref: "#" + anchor},
		},
	}

	var documentSchema, err = schema.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("error marshalling schema JSON: %w", err)
	}
	return documentSchema, nil
}

func translateDBToJSONType(column *snowflakeDiscoveryColumn) (*jsonschema.Schema, error) {
	var schema columnSchema
	switch column.DataType {
	case "NUMBER":
		if column.NumericScale != nil && *column.NumericScale == 0 {
			schema = columnSchema{jsonType: "integer"}
		} else {
			schema = columnSchema{jsonType: "number"}
		}
	default:
		if s, ok := snowflakeTypeToJSON[column.DataType]; ok {
			schema = s
		} else {
			return nil, fmt.Errorf("unhandled Snowflake type %q (found on column %q of table %q)", column.DataType, column.Name, column.Table)
		}
	}

	// Pass-through the column description and nullability.
	schema.nullable = column.IsNullable != "NO"
	if column.Comment != nil {
		schema.description = *column.Comment
	}
	return schema.toType(), nil
}

type columnSchema struct {
	contentEncoding string
	description     string
	format          string
	nullable        bool
	extras          map[string]interface{}
	jsonType        string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]interface{}),
	}
	for k, v := range s.extras {
		out.Extras[k] = v
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

var snowflakeTypeToJSON = map[string]columnSchema{
	// "NUMBER":  {jsonType: "number"}, // The 'NUMBER' column type is handled in code
	"TEXT":          {jsonType: "string"},
	"FLOAT":         {jsonType: "number"},
	"BOOLEAN":       {jsonType: "boolean"},
	"BINARY":        {jsonType: "string", contentEncoding: "base64"},
	"TIME":          {jsonType: "string", format: "date-time"},
	"DATE":          {jsonType: "string", format: "date-time"},
	"TIMESTAMP_TZ":  {jsonType: "string", format: "date-time"},
	"TIMESTAMP_NTZ": {jsonType: "string", format: "date-time"},
	"TIMESTAMP_LTZ": {jsonType: "string", format: "date-time"},

	"VARIANT": {},
	"OBJECT":  {jsonType: "object"},
	"ARRAY":   {jsonType: "array"},
}
