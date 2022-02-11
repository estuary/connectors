package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

func main() {
	boilerplate.RunMain(newBigQueryDriver())
}

// Config represents the endpoint configuration for BigQuery.
type config struct {
	BillingProjectID string `json:"billingProjectId,omitempty"`
	ProjectID        string `json:"projectId"`
	Dataset          string `json:"dataset"`
	Region           string `json:"region,omitempty"`
	Bucket           string `json:"bucket"`
	BucketPath       string `json:"bucketPath"`
	CredentialsFile  string `json:"credentialsFile,omitempty"`
	CredentialsJSON  []byte `json:"credentialsJson,omitempty"`
}

func (c *config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("expected project_id")
	}
	if c.Dataset == "" {
		return fmt.Errorf("expected dataset")
	}
	if c.Region == "" {
		return fmt.Errorf("expected region")
	}
	if c.Bucket == "" {
		return fmt.Errorf("expected bucket")
	}
	return nil
}

// DatasetPath returns the sqlDriver.ResourcePath including the dataset.
func (c *config) DatasetPath(path ...string) sqlDriver.ResourcePath {
	return append([]string{c.ProjectID, c.Dataset}, path...)
}

type tableConfig struct {
	base *config

	Table string `json:"table"`
	Delta bool   `json:"deltaUpdates,omitempty"`
}

func (c *tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

// Path returns the sqlDriver.ResourcePath for a table.
func (c tableConfig) Path() sqlDriver.ResourcePath {
	return c.base.DatasetPath(c.Table)
}

// DeltaUpdates returns if BigQuery is in DeltaUpdates mode or not.
func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

// newBigQueryDriver creates a new Driver for BigQuery.
func newBigQueryDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		DocumentationURL: "https://docs.estuary.dev/#FIXME",
		EndpointSpecType: &config{},
		ResourceSpecType: &tableConfig{},
		NewResource: func(endpoint sqlDriver.Endpoint) sqlDriver.Resource {
			return &tableConfig{base: endpoint.(*Endpoint).config}
		},
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (sqlDriver.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing BigQuery configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"project_id": parsed.ProjectID,
				"dataset":    parsed.Dataset,
				"region":     parsed.Region,
				"bucket":     parsed.Bucket,
				"bucketPath": parsed.BucketPath,
			}).Info("opening bigquery")

			var clientOpts []option.ClientOption

			// Pick one of the credentials options. It's plausible you could use machine credentials in which case neither option is present.
			if parsed.CredentialsFile != "" {
				clientOpts = append(clientOpts, option.WithCredentialsFile(parsed.CredentialsFile))
			} else if len(parsed.CredentialsJSON) != 0 {
				clientOpts = append(clientOpts, option.WithCredentialsJSON(parsed.CredentialsJSON))
			}

			// Allow overriding the main 'projectId' with 'billingProjectId' for client operation billing.
			var billingProjectID = parsed.BillingProjectID
			if billingProjectID == "" {
				billingProjectID = parsed.ProjectID
			}
			bigQueryClient, err := bigquery.NewClient(ctx, billingProjectID, clientOpts...)
			if err != nil {
				return nil, fmt.Errorf("creating bigquery client: %w", err)
			}

			cloudStorageClient, err := storage.NewClient(ctx, clientOpts...)
			if err != nil {
				return nil, fmt.Errorf("creating cloud storage client: %w", err)
			}

			return &Endpoint{
				config:             parsed,
				bigQueryClient:     bigQueryClient,
				cloudStorageClient: cloudStorageClient,
				generator:          SQLGenerator(),
				flowTables:         sqlDriver.DefaultFlowTables(parsed.ProjectID + "." + parsed.Dataset + "."), // Prefix with project ID and dataset
			}, nil
		},
		NewTransactor: func(
			ctx context.Context,
			ep sqlDriver.Endpoint,
			spec *pf.MaterializationSpec,
			sdFence sqlDriver.Fence,
			resources []sqlDriver.Resource,
		) (_ pm.Transactor, err error) {
			var t = &transactor{
				ep:       ep.(*Endpoint),
				fence:    sdFence.(*fence),
				bindings: make([]*binding, len(spec.Bindings)),
			}

			// Create the bindings for this transactor
			for bindingPos, spec := range spec.Bindings {
				var target = sqlDriver.ResourcePath(spec.ResourcePath).Join()
				t.bindings[bindingPos], err = newBinding(t.ep.generator, bindingPos, target, spec)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", target, err)
				}
			}
			return t, nil
		},
	}
}

// Bigquery only allows underscore, letters, numbers, and sometimes hyphens for identifiers. Convert everything else to underscore.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-\._0-9a-zA-Z]`)

func identifierSanitizer(text string) string {
	return identifierSanitizerRegexp.ReplaceAllString(text, "_")
}

// SQLGenerator returns a SQLGenerator for the BigQuery SQL dialect.
func SQLGenerator() sqlDriver.Generator {
	var jsonMapper = sqlDriver.ConstColumnType{
		SQLType: "STRING",
		ValueConverter: func(i interface{}) (interface{}, error) {
			switch ii := i.(type) {
			case []byte:
				return string(ii), nil
			case json.RawMessage:
				return string(ii), nil
			case nil:
				return json.RawMessage(nil), nil
			default:
				return nil, fmt.Errorf("invalid type %#v for variant", i)
			}
		},
	}

	var typeMappings = sqlDriver.ColumnTypeMapper{
		sqlDriver.ARRAY:   jsonMapper,
		sqlDriver.BINARY:  sqlDriver.RawConstColumnType("BYTES"),
		sqlDriver.BOOLEAN: sqlDriver.RawConstColumnType("BOOL"),
		sqlDriver.INTEGER: sqlDriver.RawConstColumnType("INT64"),
		sqlDriver.NUMBER:  sqlDriver.RawConstColumnType("BIGNUMERIC"),
		sqlDriver.OBJECT:  jsonMapper,
		sqlDriver.STRING: sqlDriver.StringTypeMapping{
			Default: sqlDriver.RawConstColumnType("STRING"),
			ByFormat: map[string]sqlDriver.TypeMapper{
				"date":      sqlDriver.RawConstColumnType("DATE"),
				"date-time": sqlDriver.RawConstColumnType("TIMESTAMP"),
			},
		},
	}

	var nullable sqlDriver.TypeMapper = sqlDriver.NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner:       typeMappings,
	}

	return sqlDriver.Generator{
		Placeholder:        sqlDriver.QuestionMarkPlaceholder,
		CommentRenderer:    sqlDriver.LineCommentRenderer(),
		IdentifierRenderer: sqlDriver.NewRenderer(identifierSanitizer, sqlDriver.BackticksWrapper(), nil),
		ValueRenderer:      sqlDriver.NewRenderer(sqlDriver.DefaultQuoteSanitizer, sqlDriver.SingleQuotesWrapper(), nil),
		TypeMappings:       nullable,
	}
}
