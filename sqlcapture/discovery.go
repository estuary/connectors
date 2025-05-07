package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// DiscoverCatalog queries the database and generates discovered bindings
// describing the available tables and their columns.
func DiscoverCatalog(ctx context.Context, db Database) ([]*pc.Response_Discovered_Binding, error) {
	tables, err := db.DiscoverTables(ctx)
	if err != nil {
		return nil, err
	}

	var catalog []*pc.Response_Discovered_Binding
	for streamID, table := range tables {
		var logEntry = log.WithFields(log.Fields{
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

		// Don't discover materialized tables, since that is almost never what is intended, and
		// causes problems with synthetic projection names. A column of "flow_published_at" is used
		// as a sentinel for guessing if the table is from a materialization or not. Although not
		// 100% accurate (the user could exclude this field), it will work in the vast majority of
		// cases.
		if slices.Contains(table.ColumnNames, "flow_published_at") {
			logEntry.Info("excluding table from catalog discovery because it contains the column 'flow_published_at' and is likely a materialized table")
			continue
		}

		var documentSchema, keyPointers, err = generateCollectionSchema(db, table, true)
		if err != nil {
			return nil, fmt.Errorf("error generating schema for %q: %w", streamID, err)
		}

		log.WithFields(log.Fields{
			"table":     table.Name,
			"namespace": table.Schema,
			"columns":   table.Columns,
			"schema":    string(documentSchema),
		}).Trace("translated table schema")

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
			DocumentSchemaJson: documentSchema,
			Key:                keyPointers,
		})

	}

	if len(catalog) == 0 {
		log.Warn("no tables discovered; note that tables in system schemas will not be discovered and must be added manually if desired")
	}

	return catalog, err
}

// generateCollectionSchema translates the discovery information of a table into a Flow
// collection schema (plus the discovered collection key, for convenience).
func generateCollectionSchema(db Database, table *DiscoveryInfo, fullWriteSchema bool) (json.RawMessage, []string, error) {
	// Schema of the embedded "source" property.
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: fullWriteSchema,
	}).Reflect(db.EmptySourceMetadata())
	sourceSchema.Version = ""

	if db.HistoryMode() {
		sourceSchema.Extras = map[string]interface{}{
			"reduce": map[string]interface{}{
				"strategy":    "lastWriteWins",
				"associative": false,
			},
		}
	}

	// The discovered collection key is just the discovered primary key of the table (which
	// may be a unique secondary index if the database-specific discovery logic so chooses),
	// except that if any of the columns are supposed to be omitted from the generated schema
	// obviously it's not a suitable key and we should just act like this is a keyless table.
	var collectionKey = table.PrimaryKey
	var collectionKeyHasOmittedColumn = false
	for _, key := range collectionKey {
		if table.Columns[key].OmitColumn {
			collectionKeyHasOmittedColumn = true
			break
		}
	}
	if collectionKeyHasOmittedColumn {
		collectionKey = nil
	}

	var keyPointers []string
	for _, colName := range collectionKey {
		keyPointers = append(keyPointers, primaryKeyToCollectionKey(colName))
	}

	// The anchor by which we'll reference the table schema.
	//lint:ignore SA1019 We don't need the title-casing to handle punctuation properly so strings.Title() is sufficient
	var anchor = strings.Title(table.Schema) + strings.Title(table.Name)

	// Build `properties` schemas for each table column.
	var properties = make(map[string]*jsonschema.Schema)
	for _, column := range table.Columns {
		if column.OmitColumn {
			continue // Skip adding properties corresponding to omitted columns
		}

		var isPrimaryKey = slices.Contains(collectionKey, column.Name)
		var jsonType, err = db.TranslateDBToJSONType(column, isPrimaryKey)
		if err != nil {
			// Unhandled types are translated to the catch-all schema {} but with
			// a description clarifying that we don't have a better translation.
			log.WithFields(log.Fields{
				"error": err,
				"type":  column.DataType,
			}).Debug("error translating column type to JSON schema")
			jsonType = &jsonschema.Schema{
				Description: fmt.Sprintf("using catch-all schema (%v)", err),
			}
		}
		if jsonType.Description != "" {
			jsonType.Description += " "
		}
		var nullabilityDescription = ""
		if !column.IsNullable {
			nullabilityDescription = "non-nullable "
		}

		// If the column type is an array of multiple options, and we're generating the more
		// minimal schema used in SourcedSchema messages, we need to remove "null" as an option.
		if types, ok := jsonType.Extras["type"].([]string); ok && len(types) > 1 && !fullWriteSchema {
			jsonType.Extras["type"] = slices.DeleteFunc(types, func(t string) bool {
				return t == "null"
			})
		}

		jsonType.Description += fmt.Sprintf("(source type: %s%s)", nullabilityDescription, column.DataType)
		properties[column.Name] = jsonType
	}

	var beforeSchema = &jsonschema.Schema{
		Ref:         "#" + anchor,
		Description: "Record state immediately before this change was applied.",
		Extras:      make(map[string]any),
	}
	if fullWriteSchema {
		beforeSchema.Extras["reduce"] = map[string]any{"strategy": "firstWriteWins"}
	} else {
		beforeSchema.Extras["additionalProperties"] = false
	}

	var metaPropertySchema = &jsonschema.Schema{
		Type: "object",
		Extras: map[string]any{
			"properties": map[string]*jsonschema.Schema{
				"op": {
					Enum:        []any{"c", "d", "u"},
					Description: "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
				},
				"source": sourceSchema,
				"before": beforeSchema,
			},
		},
		Required: []string{"op", "source"},
	}
	if fullWriteSchema {
		metaPropertySchema.Extras["reduce"] = map[string]any{"strategy": "merge"}
	} else {
		metaPropertySchema.Extras["additionalProperties"] = false
	}

	var metadataSchema = &jsonschema.Schema{
		Extras: map[string]any{
			"properties": map[string]*jsonschema.Schema{
				"_meta": metaPropertySchema,
			},
		},
		Required: []string{"_meta"},
	}

	// The 'full' schema generated for discovery includes if/then/else conditional reduction
	// behavior which expresses how deletions work. We don't want that in a SourcedSchema update.
	if fullWriteSchema {
		metadataSchema.If = &jsonschema.Schema{
			Extras: map[string]any{
				"properties": map[string]*jsonschema.Schema{
					"_meta": {
						Extras: map[string]any{
							"properties": map[string]*jsonschema.Schema{
								"op": {
									Extras: map[string]any{
										"const": "d",
									},
								},
							},
						},
					},
				},
			},
		}
		metadataSchema.Then = &jsonschema.Schema{
			Extras: map[string]any{
				"reduce": map[string]any{
					"strategy": "merge",
					"delete":   true,
				},
			},
		}
		metadataSchema.Else = &jsonschema.Schema{
			Extras: map[string]any{
				"reduce": map[string]any{
					"strategy": "merge",
				},
			},
		}
	}

	// Schema.Properties is a weird OrderedMap thing, which doesn't allow for inline
	// literal construction. Instead, use the Schema.Extras mechanism with "properties"
	// to generate the properties keyword with an inline map.
	var schema = jsonschema.Schema{
		Definitions: jsonschema.Definitions{
			anchor: &jsonschema.Schema{
				Type: "object",
				Extras: map[string]any{
					"$anchor":    anchor,
					"properties": properties,
				},
				Required: collectionKey,
			},
		},
		Extras: make(map[string]any),
		AllOf:  []*jsonschema.Schema{metadataSchema, {Ref: "#" + anchor}},
	}
	if table.UseSchemaInference {
		schema.Extras["x-infer-schema"] = true
	}
	if !fullWriteSchema {
		schema.Extras["additionalProperties"] = false
	}

	var documentSchema, err = schema.MarshalJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("error marshalling schema JSON: %w", err)
	}
	return documentSchema, keyPointers, nil
}

// Per the flow JSON schema: Collection names are paths of Unicode letters, numbers, '-', '_', or
// '.'. Each path component is separated by a slash '/', and a name may not begin or end in a '/'.

// There is also a requirement for gazette journals that they must be a "clean" path. As a
// simplification to ensure that recommended collection names meet this requirement we will replace
// any occurences of '/' with '_' as well.
var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	var sanitizedSchema = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(schema), "_")
	var sanitizedTable = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(table), "_")

	return sanitizedSchema + "/" + sanitizedTable
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
