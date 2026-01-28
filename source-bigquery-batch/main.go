package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

var featureFlagDefaults = map[string]bool{
	// When true, the fallback collection key for keyless source tables will be
	// ["/_meta/row_id"] instead of ["/_meta/polled", "/_meta/index"].
	"keyless_row_id": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,
}

type AuthType string

const (
	CredentialsJSON AuthType = "CredentialsJSON"
	GCPIAM          AuthType = "GCPIAM"
)

type CredentialsJSONConfig struct {
	CredentialsJSON string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=0"`
}

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	CredentialsJSONConfig
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Credentials JSON", CredentialsJSONConfig{}, string(CredentialsJSON)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("GCP IAM", iam.GCPConfig{}, string(GCPIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(CredentialsJSON), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case CredentialsJSON:
		if c.CredentialsJSON == "" {
			return errors.New("missing 'credentials_json'")
		}

		// Sanity check: Are the provided credentials valid JSON? A common error is to upload
		// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
		// directly to bigquery.NewClient.
		if !json.Valid([]byte(c.CredentialsJSON)) {
			return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
		}
		return nil
	case GCPIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	ProjectID       string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset(s)." jsonschema_extras:"order=0"`
	CredentialsJSON string `json:"credentials_json" jsonschema:"-" jsonschema_extras:"secret=true,multiline=true,order=1"`
	Dataset         string `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset to discover tables within." jsonschema_extras:"order=2"`

	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=3"`
	Advanced    advancedConfig     `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	DiscoverViews bool   `json:"discover_views,omitempty" jsonschema:"title=Discover Views,description=When set views will be automatically discovered as resources. If unset only tables will be discovered."`
	PollSchedule  string `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	SourceTag     string `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
	FeatureFlags  string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`

	parsedFeatureFlags map[string]bool // Parsed feature flags setting with defaults applied
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"project_id", c.ProjectID},
		{"dataset", c.Dataset},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials == nil {
		// Sanity check: Are the provided credentials valid JSON? A common error is to upload
		// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
		// directly to bigquery.NewClient.
		if !json.Valid([]byte(c.CredentialsJSON)) {
			return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
		}
	} else if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if c.Advanced.PollSchedule != "" {
		if err := schedule.Validate(c.Advanced.PollSchedule); err != nil {
			return fmt.Errorf("invalid default polling schedule %q: %w", c.Advanced.PollSchedule, err)
		}
	}
	// Strictly speaking this feature-flag parsing isn't validation at all, but it's a convenient
	// method that we can be sure always gets called before the config is used.
	c.Advanced.parsedFeatureFlags = common.ParseFeatureFlags(c.Advanced.FeatureFlags, featureFlagDefaults)
	if c.Advanced.FeatureFlags != "" {
		log.WithField("flags", c.Advanced.parsedFeatureFlags).Info("parsed feature flags")
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}
}

const (
	// Google Cloud DATETIME columns support microsecond precision at most
	datetimeFormatMicros = "2006-01-02T15:04:05.000000"
)

func translateBigQueryValue(val any, fieldSchema *bigquery.FieldSchema) (any, error) {
	// Arrays are represented as their element schema with `Repeated = true` set, so
	// to translate them we need to check `Repeated` before anything else.
	if fieldSchema.Repeated {
		if vals, ok := val.([]bigquery.Value); ok {
			// Construct the non-repeated element schema
			var elementSchema = *fieldSchema
			elementSchema.Repeated = false

			// Translate array elements with the element schema
			var translated = make([]any, len(vals))
			for idx, val := range vals {
				var tval, err = translateBigQueryValue(val, &elementSchema)
				if err != nil {
					return nil, fmt.Errorf("error translating array index %d: %w", idx, err)
				}
				translated[idx] = tval
			}
			return translated, nil
		} else {
			return val, nil
		}
	}

	switch fieldSchema.Type {
	case "RECORD":
		if vals, ok := val.([]bigquery.Value); ok {
			if len(vals) > len(fieldSchema.Schema) {
				return nil, fmt.Errorf("more values than record fields (%d > %d)", len(vals), len(fieldSchema.Schema))
			}
			var translated = make(map[string]any)
			for idx, val := range vals {
				var fieldName = fieldSchema.Schema[idx].Name
				var tval, err = translateBigQueryValue(val, fieldSchema.Schema[idx])
				if err != nil {
					return nil, fmt.Errorf("error translating record field %q: %w", fieldName, err)
				}
				translated[fieldName] = tval
			}
			return translated, nil
		} else {
			return val, nil
		}
	}

	switch val := val.(type) {
	case *big.Rat:
		n, exact := val.FloatPrec()
		if !exact {
			// Add three additional digits of precision to inexact representations.
			// The amount is arbitrary but was chosen so 1/3 becomes "0.333" instead of "0"
			// In theory it should never apply anyway since BigQuery seems to always return
			// rationals with a power-of-ten denominator.
			n += 3
		}
		return val.FloatString(n), nil
	case civil.DateTime:
		return val.In(time.UTC).Format(datetimeFormatMicros), nil
	case string:
		if fieldSchema.Type == "JSON" && json.Valid([]byte(val)) {
			return json.RawMessage([]byte(val)), nil
		}
	case float64:
		if math.IsNaN(val) {
			return "NaN", nil
		}
	}
	return val, nil
}

func (c *Config) CredentialsClientOption() (option.ClientOption, error) {
	if c.Credentials == nil {
		return option.WithCredentialsJSON([]byte(c.CredentialsJSON)), nil
	}

	switch c.Credentials.AuthType {
	case CredentialsJSON:
		return option.WithCredentialsJSON([]byte(c.Credentials.CredentialsJSON)), nil
	case GCPIAM:
		return option.WithTokenSource(oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: c.Credentials.GoogleToken()},
		)), nil
	}
	return nil, fmt.Errorf("unknown 'auth_type'")
}

func connectBigQuery(ctx context.Context, cfg *Config) (*bigquery.Client, error) {
	log.WithFields(log.Fields{
		"project_id": cfg.ProjectID,
	}).Info("connecting to database")

	credOption, err := cfg.CredentialsClientOption()
	if err != nil {
		return nil, err
	}
	var clientOpts = []option.ClientOption{
		credOption,
		option.WithUserAgent("EstuaryFlow (GPN:Estuary;)"),
	}
	client, err := bigquery.NewClient(ctx, cfg.ProjectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}

	if err := executeQuery(ctx, client, "SELECT true;"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return client, nil
}

func executeQuery(ctx context.Context, client *bigquery.Client, query string) error {
	if job, err := client.Query(query).Run(ctx); err != nil {
		return err
	} else if js, err := job.Wait(ctx); err != nil {
		return err
	} else if js.Err() != nil {
		return js.Err()
	}
	return nil
}

func selectQueryTemplate(res *Resource) (string, error) {
	if res.Template != "" {
		return res.Template, nil
	}
	return tableQueryTemplate, nil
}

const tableQueryTemplate = `{{if not .CursorFields -}}
  SELECT * FROM {{quoteTableName .SchemaName .TableName}};
{{- else -}}
  SELECT * FROM {{quoteTableName .SchemaName .TableName}}
  {{- if not .IsFirstQuery -}}
	{{- range $i, $k := $.CursorFields -}}
	  {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
		{{- if lt $j $i -}}
		  {{$n}} = @p{{$j}} AND {{end -}}
	  {{- end -}}
	  {{$k}} > @p{{$i}}
	{{- end -}})
  {{- end}}
  ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{$k}}{{end -}};
{{- end}}`

var templateFuncs = template.FuncMap{
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}

func generateBigQueryResource(cfg *Config, resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	if strings.EqualFold(tableType, "BASE TABLE") {
		return &Resource{
			Name:       resourceName,
			SchemaName: schemaName,
			TableName:  tableName,
		}, nil
	}
	if strings.EqualFold(tableType, "VIEW") && cfg.Advanced.DiscoverViews {
		return &Resource{
			Name:       resourceName,
			SchemaName: schemaName,
			TableName:  tableName,
		}, nil
	}
	return nil, fmt.Errorf("unsupported entity type %q", tableType)
}

var bigqueryDriver = &BatchSQLDriver{
	DocumentationURL:    "https://go.estuary.dev/source-bigquery-batch",
	ConfigSchema:        generateConfigSchema(),
	Connect:             connectBigQuery,
	GenerateResource:    generateBigQueryResource,
	SelectQueryTemplate: selectQueryTemplate,
	ExcludedSystemSchemas: []string{
		"information_schema",
	},
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("Batch BigQuery", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

func main() {
	boilerplate.RunMain(bigqueryDriver)
}
