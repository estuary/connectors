package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"text/template"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	_ "github.com/jackc/pgx/v4/stdlib"
)

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	SSLMode string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the PostgreSQL
	// default 5432.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5432"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	var address = c.Address
	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	var params = make(url.Values)
	if c.Advanced.SSLMode != "" {
		params.Set("sslmode", c.Advanced.SSLMode)
	}
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}
	return uri.String()
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

type driver struct{}

func main() {
	boilerplate.RunMain(new(driver))
}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Batch SQL Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-batchsql",
	}, nil
}

func (driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

const discoverQuery = `
SELECT table_schema, table_name, table_type
  FROM information_schema.tables
  WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'catalog_history', 'cron');
`

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	// Validate connection to database
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")
	var db, err = sql.Open("pgx", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
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

		var resourceName = recommendedCatalogName(tableSchema, tableName)
		var res = &Resource{
			Name:     resourceName,
			Template: fmt.Sprintf(`SELECT * FROM "%s"."%s";`, tableSchema, tableName),
		}
		var resourceConfigJSON, err = json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    resourceName,
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

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	// Validate connection to database
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")
	var db, err = sql.Open("pgx", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}
	defer db.Close()

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

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg Config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var resources []Resource
	for _, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
	}

	if open.StateJson != nil {
		// TODO: Consider implementing persistent state, based on the resource name property.
	}

	var capture = &capture{
		Config:    cfg,
		Resources: resources,
		Output:    stream,
	}
	return capture.Run(stream.Context())
}

type capture struct {
	Config    Config
	Resources []Resource
	Output    *boilerplate.PullOutput
}

func (c *capture) Run(ctx context.Context) error {
	log.WithFields(log.Fields{
		"address":  c.Config.Address,
		"user":     c.Config.User,
		"database": c.Config.Database,
	}).Info("connecting to database")
	var db, err = sql.Open("pgx", c.Config.ToURI())
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer db.Close()
	log.Info("connected")

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
		eg.Go(func() error { return c.worker(workerCtx, db, idx, &res) })
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("capture terminated with error: %w", err)
	}
	return nil
}

func (c *capture) worker(ctx context.Context, db *sql.DB, bindingIndex int, res *Resource) error {
	log.WithField("name", res.Name).Info("starting worker")

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
		var nextCursor, err = c.poll(ctx, db, bindingIndex, queryTemplate, res.Cursor, cursorValues)
		if err != nil {
			return fmt.Errorf("error polling table: %w", err)
		}
		cursorValues = nextCursor
		time.Sleep(pollInterval)
	}
	return ctx.Err()
}

func (c *capture) poll(ctx context.Context, db *sql.DB, bindingIndex int, tmpl *template.Template, cursorNames []string, cursorValues []any) ([]any, error) {
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
	var rows, err = db.QueryContext(ctx, query, cursorValues...)
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
			var translatedVal, err = translateRecordField(columnTypes[i].DatabaseTypeName(), columnValues[i])
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
			// TODO(wgd): Consider adding actual checkpoint updating
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
