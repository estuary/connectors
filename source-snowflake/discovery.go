package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		log.WithField("err", err).Fatal("error discovering snowflake collections")
	}

	bindings, err := catalogFromDiscovery(cfg, discoveryResults)
	if err != nil {
		log.WithField("err", err).Fatal("error converting discovery info to bindings")
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

	// We have to use `SHOW FOO` queries to get some of the information we need, such
	// as primary keys. But there's no simple way to select just the columns we need
	// from these nonstandard not-a-table queries, and that makes them unstable if
	// new columns were to be added in the future.
	//
	// So we wrap the database connection with SQLX and use its struct-tag reflection
	// to select the columns of interest *by name*, and set Unsafe() so that it will
	// ignore unmatched columns.
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
	// Any encoded '~' must be escaped first to prevent a second escape on escaped '/' values as
	// '~1'.
	key = strings.ReplaceAll(key, "~", "~0")
	key = strings.ReplaceAll(key, "/", "~1")
	return "/" + key
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
						"_meta": {
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
				Required: []string{"_meta"},
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
	"TEXT":    {jsonType: "string"},
	"FLOAT":   {jsonType: "number"},
	"BOOLEAN": {jsonType: "boolean"},
}
