package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
)

// DiscoverCatalog queries the database and generates discovered bindings
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
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: true,
	}).Reflect(db.EmptySourceMetadata())
	sourceSchema.Version = ""

	var catalog []*pc.Response_Discovered_Binding
	for _, table := range tables {
		var logEntry = logrus.WithFields(logrus.Fields{
			"table":      table.Name,
			"namespace":  table.Schema,
			"primaryKey": table.PrimaryKey,
		})
		logEntry.Debug("discovered table")

		// Filter out views and other entities whose type is not `BASE TABLE` from
		// discovery output. This is part of a bugfix in August 2023 and should be
		// removed once the database-specific discovery code in MySQL and SQL Server
		// connectors can safely filter these out at the source.
		if !table.BaseTable {
			logEntry.Info("excluding view or other non-BASE TABLE entity from catalog discovery")
			continue
		}

		// Omit catalog entries for tables with 'OmitBinding = true'. This allows some
		// tables to be filtered out of discovered catalogs while still allowing other
		// connector-internal uses of the data to see the tables.
		if table.OmitBinding {
			logEntry.Debug("excluding table from catalog discovery because OmitBinding is set")
			continue
		}

		// The anchor by which we'll reference the table schema.
		var anchor = strings.Title(table.Schema) + strings.Title(table.Name)

		// Build `properties` schemas for each table column.
		var properties = make(map[string]*jsonschema.Schema)
		for _, column := range table.Columns {
			var jsonType, err = db.TranslateDBToJSONType(column)
			if err != nil {
				// Unhandled types are translated to the catch-all schema {} but with
				// a description clarifying that we don't have a better translation.
				logrus.WithFields(logrus.Fields{
					"error": err,
					"type":  column.DataType,
				}).Debug("error translating column type to JSON schema")
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
					},
					Required: []string{"_meta"},
					If: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"properties": map[string]*jsonschema.Schema{
								"_meta": {
									Extras: map[string]interface{}{
										"properties": map[string]*jsonschema.Schema{
											"op": {
												Extras: map[string]interface{}{
													"const": "d",
												},
											},
										},
									},
								},
							},
						},
					},
					Then: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"reduce": map[string]interface{}{
								"strategy": "merge",
								"delete":   true,
							},
						},
					},
					Else: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"reduce": map[string]interface{}{
								"strategy": "merge",
							},
						},
					},
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

		var suggestedMode = BackfillModeAutomatic
		if len(keyPointers) == 0 {
			keyPointers = db.FallbackCollectionKey()
			suggestedMode = BackfillModeWithoutKey
		}
		var res = Resource{
			Mode:      suggestedMode,
			Namespace: table.Schema,
			Stream:    table.Name,
		}
		resourceSpecJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		catalog = append(catalog, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedCatalogName(table.Schema, table.Name),
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
	var catalogName string
	// Omit 'default schema' names for Postgres and SQL Server. There is
	// no default schema for MySQL databases.
	if schema == "public" || schema == "dbo" {
		catalogName = table
	} else {
		catalogName = schema + "_" + table
	}
	return catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(catalogName), "_")
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

var versionRe = regexp.MustCompile(`(?i)^v?(\d+)\.(\d+)`)

// ParseVersion attempts to parse the major and minor version from a database version string. The
// version string can be optionally prefixed with a "v" which must be followed by one or more digits
// for the major version, a period, and one or more digits for the minor version. Characters
// following the minor version digit(s) are allowed but have no impact on the parsed result.
func ParseVersion(versionStr string) (major, minor int, err error) {
	if matches := versionRe.FindAllStringSubmatch(versionStr, -1); len(matches) != 1 {
		return 0, 0, fmt.Errorf("could not extract major and minor version")
	} else if parts := matches[0]; len(parts) != 3 { // Index 0 is the entire matched string; 1 and 2 are the capture groups
		return 0, 0, fmt.Errorf("could not extract major and minor version")
	} else if major, err = strconv.Atoi(parts[1]); err != nil {
		return 0, 0, err
	} else if minor, err = strconv.Atoi(parts[2]); err != nil {
		return 0, 0, err
	}

	return major, minor, nil
}

// ValidVersion compares a given major and minor version with a required major and minor version.
func ValidVersion(major, minor, reqMajor, reqMinor int) bool {
	if major > reqMajor {
		return true
	} else if major < reqMajor || minor < reqMinor {
		return false
	}

	return true
}
