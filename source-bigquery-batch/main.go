package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	ProjectID       string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset(s)." jsonschema_extras:"order=0"`
	CredentialsJSON string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`
	Dataset         string `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset to discover tables within." jsonschema_extras:"order=2"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	PollSchedule string `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"project_id", c.ProjectID},
		{"credentials_json", c.CredentialsJSON},
		{"dataset", c.Dataset},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	// Sanity check: Are the provided credentials valid JSON? A common error is to upload
	// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
	// directly to bigquery.NewClient.
	if !json.Valid([]byte(c.CredentialsJSON)) {
		return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
	}
	if c.Advanced.PollSchedule != "" {
		if err := schedule.Validate(c.Advanced.PollSchedule); err != nil {
			return fmt.Errorf("invalid default polling schedule %q: %w", c.Advanced.PollSchedule, err)
		}
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}
}

func translateBigQueryValue(val any, fieldType bigquery.FieldType) (any, error) {
	switch val := val.(type) {
	case string:
		if fieldType == "JSON" && json.Valid([]byte(val)) {
			return json.RawMessage([]byte(val)), nil
		}
	case float64:
		if math.IsNaN(val) {
			return "NaN", nil
		}
	}
	return val, nil
}

func connectBigQuery(ctx context.Context, cfg *Config) (*bigquery.Client, error) {
	log.WithFields(log.Fields{
		"project_id": cfg.ProjectID,
	}).Info("connecting to database")

	var clientOpts = []option.ClientOption{
		option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)),
	}
	var client, err = bigquery.NewClient(ctx, cfg.ProjectID, clientOpts...)
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

const tableQueryTemplateTemplate = `{{/* Default query template which adapts to cursor field selection */}}
{{- if not .CursorFields -}}
  SELECT * FROM %[1]s;
{{- else -}}
  SELECT * FROM %[1]s
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

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func generateBigQueryResource(resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	var queryTemplate string
	if strings.EqualFold(tableType, "BASE TABLE") {
		queryTemplate = fmt.Sprintf(tableQueryTemplateTemplate, quoteTableName(schemaName, tableName))
	} else {
		return nil, fmt.Errorf("discovery will not autogenerate resource configs for entities of type %q, but you may add them manually", tableType)
	}

	return &Resource{
		Name:     resourceName,
		Template: queryTemplate,
	}, nil
}

var bigqueryDriver = &BatchSQLDriver{
	DocumentationURL: "https://go.estuary.dev/source-bigquery-batch",
	ConfigSchema:     generateConfigSchema(),
	Connect:          connectBigQuery,
	GenerateResource: generateBigQueryResource,
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
