package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	bqclient "github.com/estuary/connectors/go/capture/bigquery/client"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

// StreamType identifies one of the three GA4 export logical streams.
type StreamType string

const (
	StreamEvents            StreamType = "events"
	StreamUsers             StreamType = "users"
	StreamPseudonymousUsers StreamType = "pseudonymous_users"
)

// Resource is the per-binding resource configuration. Discovery emits exactly
// one binding per (dataset, stream_type) pair which has matching tables.
type Resource struct {
	Dataset    string     `json:"dataset" jsonschema:"title=Dataset,description=The BigQuery dataset containing the GA4 export tables for this binding." jsonschema_extras:"order=0"`
	StreamType StreamType `json:"stream_type" jsonschema:"title=Stream Type,description=Which GA4 logical stream this binding represents." jsonschema_extras:"enum=events,enum=users,enum=pseudonymous_users,order=1"`
}

// Validate checks that the resource spec possesses all required properties.
func (r *Resource) Validate() error {
	if r.Dataset == "" {
		return fmt.Errorf("missing 'dataset'")
	}
	if _, ok := streams[r.StreamType]; !ok {
		return fmt.Errorf("invalid 'stream_type' %q (must be one of: events, users, pseudonymous_users)", r.StreamType)
	}
	return nil
}

// streamDef declares per-stream-type configuration consumed by both discovery
// and the polling loop.
type streamDef struct {
	Type         StreamType
	TablePrefix  string // e.g. "events_"
	CursorColumn string // column used as within-table cursor
	CursorType   string // "INT64" or "STRING"; used for BQ parameter binding
	KeyColumns   []string
	DataFields   map[string]*jsonschema.Schema
	DataRequired []string
}

var streams = map[StreamType]*streamDef{
	StreamEvents: {
		Type:         StreamEvents,
		TablePrefix:  "events_",
		CursorColumn: "event_timestamp",
		CursorType:   "INT64",
		KeyColumns: []string{
			"/event_timestamp",
			"/event_name",
			"/user_pseudo_id",
			"/event_bundle_sequence_id",
		},
		DataFields: map[string]*jsonschema.Schema{
			"event_timestamp":          {Type: "integer"},
			"event_name":               {Type: "string"},
			"user_pseudo_id":           nullableStringSchema(),
			"event_bundle_sequence_id": nullableIntegerSchema(),
		},
		DataRequired: []string{"event_timestamp", "event_name"},
	},
	StreamUsers: {
		Type:         StreamUsers,
		TablePrefix:  "users_",
		CursorColumn: "user_id",
		CursorType:   "STRING",
		KeyColumns: []string{
			"/_meta/source/table_date",
			"/user_id",
		},
		DataFields: map[string]*jsonschema.Schema{
			"user_id": {Type: "string"},
		},
		DataRequired: []string{"user_id"},
	},
	StreamPseudonymousUsers: {
		Type:         StreamPseudonymousUsers,
		TablePrefix:  "pseudonymous_users_",
		CursorColumn: "pseudo_user_id",
		CursorType:   "STRING",
		KeyColumns: []string{
			"/_meta/source/table_date",
			"/pseudo_user_id",
		},
		DataFields: map[string]*jsonschema.Schema{
			"pseudo_user_id": {Type: "string"},
		},
		DataRequired: []string{"pseudo_user_id"},
	},
}

// streamPrefixes returns the set of all stream-type table prefixes.
func streamPrefixes() []string {
	var out []string
	for _, s := range streams {
		out = append(out, s.TablePrefix)
	}
	sort.Strings(out)
	return out
}

func nullableStringSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Extras: map[string]any{"type": []string{"string", "null"}}}
}

func nullableIntegerSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Extras: map[string]any{"type": []string{"integer", "null"}}}
}

// Discover enumerates GA4 export tables across the configured dataset (or all
// datasets in the project) and emits one binding per (dataset, stream_type)
// pair that has matching tables.
func (d *Driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var bq, err = d.Connect(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	defer bq.Close()

	var datasets []string
	if cfg.Dataset != "" {
		datasets = []string{cfg.Dataset}
	} else {
		datasets, err = listDatasets(ctx, bq)
		if err != nil {
			return nil, fmt.Errorf("listing datasets: %w", err)
		}
	}

	var bindings []*pc.Response_Discovered_Binding
	var discoveryErrors []error
	for _, dataset := range datasets {
		streamsPresent, err := findStreams(ctx, bq, dataset)
		if err != nil {
			if cfg.Dataset != "" {
				return nil, fmt.Errorf("discovering GA4 exports in dataset %q: %w", dataset, err)
			}
			discoveryErrors = append(discoveryErrors, fmt.Errorf("%s: %w", dataset, err))
			log.WithFields(log.Fields{"dataset": dataset, "error": err}).Warn("skipping dataset due to discovery error")
			continue
		}
		// Emit bindings in a stable order
		var orderedTypes = make([]StreamType, 0, len(streamsPresent))
		for st := range streamsPresent {
			orderedTypes = append(orderedTypes, st)
		}
		slices.Sort(orderedTypes)

		for _, streamType := range orderedTypes {
			binding, err := buildDiscoveredBinding(dataset, streamType)
			if err != nil {
				return nil, fmt.Errorf("building binding for %s/%s: %w", dataset, streamType, err)
			}
			bindings = append(bindings, binding)
		}
	}
	if len(bindings) == 0 && len(discoveryErrors) > 0 {
		return nil, fmt.Errorf("no GA4 BigQuery exports discovered, and %d dataset(s) could not be inspected: %w", len(discoveryErrors), errors.Join(discoveryErrors...))
	}
	return &pc.Response_Discovered{Bindings: bindings}, nil
}

// listDatasets enumerates dataset IDs in the configured project.
func listDatasets(ctx context.Context, bq *bigquery.Client) ([]string, error) {
	var out []string
	var iter = bq.Datasets(ctx)
	for {
		ds, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		out = append(out, ds.DatasetID)
	}
	sort.Strings(out)
	return out, nil
}

// findStreams returns the set of stream types whose tables exist in the given
// dataset.
func findStreams(ctx context.Context, bq *bigquery.Client, dataset string) (map[StreamType]bool, error) {
	var prefixes = streamPrefixes()
	var clauses []string
	for _, p := range prefixes {
		clauses = append(clauses, fmt.Sprintf("table_name LIKE '%s%%'", p))
	}
	var query = fmt.Sprintf(
		"SELECT table_name FROM %s.INFORMATION_SCHEMA.TABLES WHERE %s",
		bqclient.QuoteIdentifier(dataset),
		strings.Join(clauses, " OR "),
	)

	rows, err := bq.Query(query).Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying INFORMATION_SCHEMA.TABLES: %w", err)
	}

	var present = make(map[StreamType]bool)
	for {
		var row []bigquery.Value
		if err := rows.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		var tableName, _ = row[0].(string)
		for _, s := range streams {
			if strings.HasPrefix(tableName, s.TablePrefix) {
				// Sanity check: only count if the suffix looks like a date
				var suffix = strings.TrimPrefix(tableName, s.TablePrefix)
				if isDateSuffix(suffix) {
					present[s.Type] = true
				}
				break
			}
		}
	}
	return present, nil
}

var dateSuffixRe = regexp.MustCompile(`^[0-9]{8}$`)

// isDateSuffix returns true if s is exactly 8 digits (a YYYYMMDD value).
func isDateSuffix(s string) bool {
	return dateSuffixRe.MatchString(s)
}

func buildDiscoveredBinding(dataset string, streamType StreamType) (*pc.Response_Discovered_Binding, error) {
	var stream = streams[streamType]
	var resource = &Resource{
		Dataset:    dataset,
		StreamType: streamType,
	}
	resourceJSON, err := json.Marshal(resource)
	if err != nil {
		return nil, fmt.Errorf("serializing resource: %w", err)
	}
	schema, err := generateCollectionSchema(stream)
	if err != nil {
		return nil, fmt.Errorf("generating schema: %w", err)
	}
	return &pc.Response_Discovered_Binding{
		RecommendedName:    recommendedName(dataset, streamType),
		ResourceConfigJson: resourceJSON,
		DocumentSchemaJson: schema,
		Key:                slices.Clone(stream.KeyColumns),
	}, nil
}

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedName(dataset string, streamType StreamType) string {
	var sanitizedDataset = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(dataset), "_")
	var sanitizedStream = catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(string(streamType)), "_")
	return sanitizedDataset + "/" + sanitizedStream
}

// generateCollectionSchema produces the collection's document schema. Only
// the `_meta` envelope and the stream's key columns are explicitly declared;
// `x-infer-schema: true` lets Flow's schema inference handle the deeply
// nested GA4 columns automatically.
func generateCollectionSchema(stream *streamDef) (json.RawMessage, error) {
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeFor[documentMetadata]())
	metadataSchema.AdditionalProperties = nil
	metadataSchema.Definitions = nil
	if metadataSchema.Extras == nil {
		metadataSchema.Extras = make(map[string]any)
	}
	if sourceSchema, ok := metadataSchema.Properties.Get("source"); ok {
		sourceSchema.AdditionalProperties = nil
	}

	var properties = map[string]*jsonschema.Schema{
		"_meta": metadataSchema,
	}
	var required = []string{"_meta"}
	maps.Copy(properties, stream.DataFields)
	required = append(required, stream.DataRequired...)
	sort.Strings(required)

	var schema = &jsonschema.Schema{
		Type:     "object",
		Required: required,
		Extras: map[string]any{
			"properties":     properties,
			"x-infer-schema": true,
		},
	}
	return json.Marshal(schema)
}
