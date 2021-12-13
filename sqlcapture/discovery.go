package sqlcapture

import (
	"context"
	"fmt"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/protocols/airbyte"
	"github.com/sirupsen/logrus"
)

// DiscoverCatalog queries the database and generates an Airbyte Catalog
// describing the available tables and their columns.
func DiscoverCatalog(ctx context.Context, db Database) (*airbyte.Catalog, error) {
	if err := db.Connect(ctx); err != nil {
		return nil, err
	}
	defer db.Close(ctx)

	tables, err := db.DiscoverTables(ctx)
	if err != nil {
		return nil, err
	}

	// Shared schema of the embedded "source" property.
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}).Reflect(db.EmptySourceMetadata()).Type
	sourceSchema.Version = ""

	var catalog = new(airbyte.Catalog)
	for _, table := range tables {
		logrus.WithFields(logrus.Fields{
			"table":      table.Name,
			"namespace":  table.Schema,
			"primaryKey": table.PrimaryKey,
		}).Debug("discovered table")

		// The anchor by which we'll reference the table schema.
		var anchor = strings.Title(table.Schema) + strings.Title(table.Name)

		// Build `properties` schemas for each table column.
		var properties = make(map[string]*jsonschema.Type)
		for _, column := range table.Columns {
			var jsonType, err = db.TranslateDBToJSONType(column.DataType)
			if err != nil {
				return nil, fmt.Errorf("error translating column type %q to JSON schema: %w", column.DataType, err)
			}
			properties[column.Name] = jsonType
		}

		// Schema.Properties is a weird OrderedMap thing, which doesn't allow for inline
		// literal construction. Instead, use the Schema.Extras mechanism with "properties"
		// to generate the properties keyword with an inline map.
		var schema = jsonschema.Schema{
			Definitions: jsonschema.Definitions{
				anchor: &jsonschema.Type{
					Type: "object",
					Extras: map[string]interface{}{
						"$anchor":    anchor,
						"properties": properties,
					},
					Required: table.PrimaryKey,
				},
			},
			Type: &jsonschema.Type{
				AllOf: []*jsonschema.Type{
					{
						Extras: map[string]interface{}{
							"properties": map[string]*jsonschema.Type{
								"_meta": {
									Type: "object",
									Extras: map[string]interface{}{
										"properties": map[string]*jsonschema.Type{
											"op": {
												Enum:        []interface{}{"c", "d", "u"},
												Description: "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
											},
											"source": sourceSchema,
											"before": {
												Ref:         "#" + anchor,
												Description: "Record state immediately before this change was applied.",
											},
										},
									},
									Required: []string{"op", "source"},
								},
							},
						},
						Required: []string{"_meta"},
					},
					{Ref: "#" + anchor},
				},
			},
		}

		var rawSchema, err = schema.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("error marshalling schema JSON: %w", err)
		}

		logrus.WithFields(logrus.Fields{
			"table":     table.Name,
			"namespace": table.Schema,
			"columns":   table.Columns,
			"schema":    string(rawSchema),
		}).Debug("translated table schema")

		var sourceDefinedPrimaryKey [][]string
		for _, colName := range table.PrimaryKey {
			sourceDefinedPrimaryKey = append(sourceDefinedPrimaryKey, []string{colName})
		}

		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    table.Name,
			Namespace:               table.Schema,
			JSONSchema:              rawSchema,
			SupportedSyncModes:      airbyte.AllSyncModes,
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: sourceDefinedPrimaryKey,
		})
	}
	return catalog, err
}
