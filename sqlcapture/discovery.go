package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

// DiscoverCatalog queries the database and generates an Airbyte Catalog
// describing the available tables and their columns.
func DiscoverCatalog(ctx context.Context, db Database) ([]*pc.DiscoverResponse_Binding, error) {
	tables, err := db.DiscoverTables(ctx)
	if err != nil {
		return nil, err
	}

	// Shared schema of the embedded "source" property.
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}).Reflect(db.EmptySourceMetadata())
	sourceSchema.Version = ""

	var catalog []*pc.DiscoverResponse_Binding
	for _, table := range tables {
		logrus.WithFields(logrus.Fields{
			"table":      table.Name,
			"namespace":  table.Schema,
			"primaryKey": table.PrimaryKey,
		}).Debug("discovered table")

		// The anchor by which we'll reference the table schema.
		var anchor = strings.Title(table.Schema) + strings.Title(table.Name)

		// Build `properties` schemas for each table column.
		var properties = make(map[string]*jsonschema.Schema)
		for _, column := range table.Columns {
			var jsonType, err = db.TranslateDBToJSONType(column)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error": err,
					"type":  column.DataType,
				}).Warn("error translating column type to JSON schema")

				// Logging an error from the connector is nice, but can be swallowed by `flowctl-go`.
				// Putting an error in the generated schema is ugly, but makes the failure visible.
				properties[column.Name] = &jsonschema.Schema{
					Description: fmt.Sprintf("ERROR: could not translate column type %q to JSON schema: %v", column.DataType, err),
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
					Required: table.PrimaryKey,
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

		var keyPointers []string
		for _, colName := range table.PrimaryKey {
			keyPointers = append(keyPointers, "/"+colName)
		}

		var res = Resource{
			Namespace: table.Schema,
			Stream:    table.Name,
		}
		resourceSpecJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		catalog = append(catalog, &pc.DiscoverResponse_Binding{
			RecommendedName:    pf.Collection(recommendedStreamName(table.Schema, table.Name)),
			ResourceSpecJson:   resourceSpecJSON,
			DocumentSchemaJson: rawSchema,
			KeyPtrs:            keyPointers,
		})

	}
	return catalog, err
}

func recommendedStreamName(schema, table string) string {
	var streamID = JoinStreamID(schema, table)
	return streamID
}
