package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// BatchSQLDriver represents a generic "batch SQL" capture behavior, parameterized
// by a config schema, connect function, and value translation logic.
type BatchSQLDriver struct {
	DocumentationURL string
	ConfigSchema     json.RawMessage

	Connect          func(ctx context.Context, configJSON json.RawMessage) (*sql.DB, error)
	TranslateValue   func(val any, databaseTypeName string) (any, error)
	GenerateResource func(resourceName, schemaName, tableName, tableType string) (*Resource, error)
}

// Resource represents the capture configuration of a single resource binding.
type Resource struct {
	Name         string   `json:"name" jsonschema:"title=Name,description=The unique name of this resource."`
	Template     string   `json:"template" jsonschema:"title=Query Template,description=The query template (pkg.go.dev/text/template) to execute to fetch new data,multiline=true"`
	Cursor       []string `json:"cursor" jsonschema:"title=Cursor Columns,description=The names of columns which should be persisted between query executions as a cursor"`
	PollInterval string   `json:"poll,omitempty" jsonschema:"title=Poll Interval,description=How often to execute the fetch query."`
}

// Validate checks that the resource spec possesses all required properties.
func (r Resource) Validate() error {
	var requiredProperties = [][]string{
		{"name", r.Name},
		{"template", r.Template},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if _, err := template.New("query").Parse(r.Template); err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}
	if r.PollInterval != "" {
		if _, err := time.ParseDuration(r.PollInterval); err != nil {
			return fmt.Errorf("invalid poll interval %q: %w", r.PollInterval, err)
		}
	}
	return nil
}

// documentMetadata contains the source metadata located at /_meta
type documentMetadata struct {
	Polled time.Time `json:"polled" jsonschema:"title=Polled Timestamp,description=The time at which the update query which produced this document as executed."`
	Index  int       `json:"index" jsonschema:"title=Result Index,description=The index of this document within the query execution which produced it."`
}

// minimalSchema is the maximally-permissive schema which just specifies the metadata our connector adds.
var minimalSchema = generateMinimalSchema()

// The default collection key just refers to the polling iteration and result index of each document.
var defaultKey = []string{"/_meta/polled", "/_meta/index"}

func generateMinimalSchema() json.RawMessage {
	// Generate schema for the metadata via reflection
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var metadataSchema = reflector.ReflectFromType(reflect.TypeOf(documentMetadata{}))
	metadataSchema.Definitions = nil
	metadataSchema.AdditionalProperties = nil

	// Wrap metadata into an enclosing object schema with a /_meta property
	var schema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{"_meta"},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"properties": map[string]*jsonschema.Schema{
				"_meta": metadataSchema,
			},
			"x-infer-schema": true,
		},
	}

	// Marshal schema to JSON
	bs, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}
	return json.RawMessage(bs)
}

// Spec returns metadata about the capture connector.
func (drv *BatchSQLDriver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	resourceSchema, err := schemagen.GenerateSchema("Batch SQL Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         drv.ConfigSchema,
		ResourceConfigSchemaJson: resourceSchema,
		DocumentationUrl:         drv.DocumentationURL,
	}, nil
}

// Apply does nothing for batch SQL captures.
func (BatchSQLDriver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

const discoverQuery = `SELECT table_schema, table_name, table_type FROM information_schema.tables;`

var excludedSystemSchemas = []string{
	"pg_catalog",
	"information_schema",
	"pg_internal",
	"catalog_history",
	"cron",
}

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for thos tables.
func (drv *BatchSQLDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var db, err = drv.Connect(ctx, req.ConfigJson)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, discoverQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing discovery query: %w", err)
	}
	defer rows.Close()

	var bindings []*pc.Response_Discovered_Binding
	for rows.Next() {
		var tableSchema, tableName, tableType string
		if err := rows.Scan(&tableSchema, &tableName, &tableType); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// Exclude tables in "system schemas" such as information_schema or pg_catalog.
		if slices.Contains(excludedSystemSchemas, tableSchema) {
			continue
		}

		var recommendedName = recommendedCatalogName(tableSchema, tableName)
		var res, err = drv.GenerateResource(recommendedName, tableSchema, tableName, tableType)
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
				"schema": tableSchema,
				"table":  tableName,
				"type":   tableType,
			}).Warn("unable to generate resource spec for entity")
			continue
		}
		resourceConfigJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: minimalSchema,
			Key:                defaultKey,
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

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

// Validate checks that the configuration appears correct and that we can connect
// to the database and execute queries.
func (drv *BatchSQLDriver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	// Perform discovery, which inherently validates that the config is well-formed
	// and that we can connect to the database and execute (some) queries.
	if _, err := drv.Discover(ctx, &pc.Request_Discover{
		ConnectorType: req.ConnectorType,
		ConfigJson:    req.ConfigJson,
	}); err != nil {
		return nil, err
	}

	// Unmarshal and validate resource bindings to make sure they're well-formed too.
	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Name},
		})
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

// Pull is the heart of a capture connector and outputs a neverending stream of documents.
func (drv *BatchSQLDriver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var db, err = drv.Connect(stream.Context(), open.Capture.ConfigJson)
	if err != nil {
		return err
	}
	defer db.Close()

	var resources []Resource
	for _, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
	}

	if open.StateJson != nil {
		// TODO: Consider implementing persistent state, keyed on the resource name property.
	}

	var capture = &capture{
		DB:             db,
		Resources:      resources,
		Output:         stream,
		TranslateValue: drv.TranslateValue,
	}
	return capture.Run(stream.Context())
}

type capture struct {
	DB             *sql.DB
	Resources      []Resource
	Output         *boilerplate.PullOutput
	TranslateValue func(val any, databaseTypeName string) (any, error)
}

func (c *capture) Run(ctx context.Context) error {
	// Notify Flow that we're starting.
	if err := c.Output.Ready(false); err != nil {
		return err
	}

	var eg, workerCtx = errgroup.WithContext(ctx)
	for idx, res := range c.Resources {
		if idx > 0 {
			// Slightly stagger worker thread startup. Five seconds should be long
			// enough for most fast queries to complete their first execution, and
			// the hope is this reduces peak load on both the database and us.
			time.Sleep(5 * time.Second)
		}
		var idx, res = idx, res // Copy for goroutine closure
		eg.Go(func() error { return c.worker(workerCtx, idx, &res) })
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("capture terminated with error: %w", err)
	}
	return nil
}

func (c *capture) worker(ctx context.Context, bindingIndex int, res *Resource) error {
	log.WithFields(log.Fields{
		"name":   res.Name,
		"tmpl":   res.Template,
		"cursor": res.Cursor,
		"poll":   res.PollInterval,
	}).Info("starting worker")

	var queryTemplate, err = template.New("query").Parse(res.Template)
	if err != nil {
		return fmt.Errorf("error parsing template: %w", err)
	}

	var pollInterval = 5 * time.Minute
	if res.PollInterval != "" {
		var dt, err = time.ParseDuration(res.PollInterval)
		if err != nil {
			return fmt.Errorf("invalid poll interval %q: %w", res.PollInterval, err)
		}
		pollInterval = dt
	}

	var cursorValues []any
	for ctx.Err() == nil {
		log.WithField("name", res.Name).Info("polling for updates")
		var nextCursor, err = c.poll(ctx, bindingIndex, queryTemplate, res.Cursor, cursorValues)
		if err != nil {
			return fmt.Errorf("error polling table: %w", err)
		}
		cursorValues = nextCursor
		time.Sleep(pollInterval)
	}
	return ctx.Err()
}

func (c *capture) poll(ctx context.Context, bindingIndex int, tmpl *template.Template, cursorNames []string, cursorValues []any) ([]any, error) {
	var templateArg = map[string]any{
		"IsFirstQuery": len(cursorValues) == 0,
	}

	var queryBuf = new(strings.Builder)
	if err := tmpl.Execute(queryBuf, templateArg); err != nil {
		return nil, fmt.Errorf("error generating query: %w", err)
	}
	var query = queryBuf.String()

	var pollTime = time.Now().UTC()

	log.WithFields(log.Fields{"query": query, "args": cursorValues}).Info("executing query")
	var rows, err = c.DB.QueryContext(ctx, query, cursorValues...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error processing query result: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error processing query result: %w", err)
	}
	for i, columnType := range columnTypes {
		log.WithFields(log.Fields{
			"idx":          i,
			"name":         columnType.DatabaseTypeName(),
			"scanTypeName": columnType.ScanType().Name(),
		}).Debug("column type")
	}

	var resultRow = make(map[string]any)
	var translatedRow = make(map[string]any)
	var columnValues = make([]any, len(columnNames))
	var columnPointers = make([]any, len(columnValues))
	for i := range columnPointers {
		columnPointers[i] = &columnValues[i]
	}

	if len(cursorValues) != len(cursorNames) {
		cursorValues = make([]any, len(cursorNames))
	}
	var count int
	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		for i, name := range columnNames {
			var translatedVal, err = c.TranslateValue(columnValues[i], columnTypes[i].DatabaseTypeName())
			if err != nil {
				return nil, fmt.Errorf("error translating column value: %w", err)
			}
			translatedRow[name] = translatedVal
		}

		translatedRow["_meta"] = &documentMetadata{
			Polled: pollTime,
			Index:  count,
		}

		var bs, err = json.Marshal(translatedRow)
		if err != nil {
			return nil, fmt.Errorf("error serializing document: %w", err)
		} else if err := c.Output.Documents(bindingIndex, bs); err != nil {
			return nil, fmt.Errorf("error emitting document: %w", err)
		} else if err := c.Output.Checkpoint(json.RawMessage(`{}`), true); err != nil {
			return nil, fmt.Errorf("error emitting checkpoint: %w", err)
		}

		for i, name := range columnNames {
			resultRow[name] = columnValues[i]
		}
		for i, name := range cursorNames {
			cursorValues[i] = resultRow[name]
		}

		count++
	}
	return cursorValues, nil
}
