package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

// DiscoverCatalog queries the database and generates an Airbyte Catalog
// describing the available tables and their columns.
func DiscoverCatalog(ctx context.Context, db Database) ([]*pc.Response_Discovered_Binding, error) {
	tables, err := db.DiscoverTables(ctx)
	if err != nil {
		return nil, err
	}

	// If there are zero tables (or there's one table but it's the watermarks table) log a warning.
	var _, watermarksPresent = tables[db.WatermarksTable()]
	if len(tables) == 0 || len(tables) == 1 && watermarksPresent {
		logrus.Warn("no tables discovered; note that tables in system schemas will not be discovered and must be added manually if desired")
	}

	// Shared schema of the embedded "source" property.
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}).Reflect(db.EmptySourceMetadata())
	sourceSchema.Version = ""

	var catalog []*pc.Response_Discovered_Binding
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
		}).Trace("translated table schema")

		var keyPointers []string
		for _, colName := range table.PrimaryKey {
			keyPointers = append(keyPointers, primaryKeyToCollectionKey(colName))
		}

		var res = Resource{
			Namespace: table.Schema,
			Stream:    table.Name,
		}
		resourceSpecJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		catalog = append(catalog, &pc.Response_Discovered_Binding{
			RecommendedName:    pf.Collection(recommendedCatalogName(table.Schema, table.Name)),
			ResourceConfigJson: resourceSpecJSON,
			DocumentSchemaJson: rawSchema,
			Key:                keyPointers,
		})

	}
	return catalog, err
}

// Per the flow JSON schema: Collection names are paths of Unicode letters, numbers, '-', '_', or
// '.'. Each path component is separated by a slash '/', and a name may not begin or end in a '/'.

// There is also a requirement for gazette journals that they must be a "clean" path. As a
// simplification to ensure that recommended collection names meet this requirement we will replace
// any occurences of '/' with '_' as well.
var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	return catalogNameSanitizerRe.ReplaceAllString(JoinStreamID(schema, table), "_")
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

// collectionKeyToPrimaryKey is the inverse of primaryKeyToCollectionKey: It converts a Flow
// collection key JSON pointer back to the original database primary key column name by unescaping
// the encoded '~0' and '~1' values back into '~' and '/', respecively.
func collectionKeyToPrimaryKey(ptr string) string {
	ptr = strings.TrimPrefix(ptr, "/")
	// Any encoded '/' must be unescaped first. An escaped database column name containing a literal
	// '~1' results in an escaped JSON pointer like '/~01'. If encoded '~' were escaped first, this
	// would result in a conversion like '~01' -> '~1' -> '/' rather than '~01' -> '~01' -> '~1'.
	ptr = strings.ReplaceAll(ptr, "~1", "/")
	ptr = strings.ReplaceAll(ptr, "~0", "~")
	return ptr
}
