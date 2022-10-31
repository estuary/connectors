package main

import (
	"context"
	"encoding/base64"
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
	ProjectID        string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset." jsonschema_extras:"order=0"`
	CredentialsJSON  string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`
	Region           string `json:"region" jsonschema:"title=Region,description=Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region." jsonschema_extras:"order=2"`
	Dataset          string `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset that will be used to store the materialization output." jsonschema_extras:"order=3"`
	Bucket           string `json:"bucket" jsonschema:"title=Bucket,description=Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery." jsonschema_extras:"order=4"`
	BucketPath       string `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects to Google Cloud Storage's bucket." jsonschema_extras:"order=5"`
	BillingProjectID string `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Billing Project ID connected to the BigQuery dataset. Defaults to Project ID if not specified." jsonschema_extras:"order=6"`
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

	Table string `json:"table" jsonschema:"title=Table,description=Table in the BigQuery dataset to store materialized result in."`
	Delta bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false."`
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

// decodeCredentials allows support for legacy credentials that were base64 encoded. Previously, the
// connector required base64 encoding of JSON service account credentials. In the future, this
// fallback can be removed when base64 encoded credentials are no longer supported and only
// unencoded JSON is acceptable.
func decodeCredentials(credentialString string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(credentialString)
	if err == nil {
		// If the provided credentials string was a valid base64 encoding, assume that it was base64
		// encoded JSON and return the result of successfully decoding that.
		return decoded
	}

	// Otherwise, assume that the credentials string was not base64 encoded.
	return []byte(credentialString)

}

// newBigQueryDriver creates a new Driver for BigQuery.
func newBigQueryDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-bigquery",
		EndpointSpecType: config{},
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
				"project_id":  parsed.ProjectID,
				"dataset":     parsed.Dataset,
				"region":      parsed.Region,
				"bucket":      parsed.Bucket,
				"bucket_path": parsed.BucketPath,
			}).Info("opening bigquery")

			var clientOpts []option.ClientOption

			clientOpts = append(clientOpts, option.WithCredentialsJSON(decodeCredentials(parsed.CredentialsJSON)))

			// Allow overriding the main 'project_id' with 'billing_project_id' for client operation billing.
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
