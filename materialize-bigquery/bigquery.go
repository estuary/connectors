package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

var featureFlagDefaults = map[string]bool{
	// When set, object and array field types will be materialized as JSON
	// columns, instead of the historical behavior of strings.
	"objects_and_arrays_as_json":       true,
	"datetime_keys_as_string":          true,
	"retain_existing_data_on_backfill": false,
	"skip_cleanup":                     false,
	"native_binary_column_type":        true,
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

type config struct {
	ProjectID        string             `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset." jsonschema_extras:"order=0"`
	CredentialsJSON  string             `json:"credentials_json" jsonschema:"-" jsonschema_extras:"secret=true,multiline=true,order=1"`
	Region           string             `json:"region" jsonschema:"title=Region,description=Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region." jsonschema_extras:"order=2"`
	Dataset          string             `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=3"`
	Bucket           string             `json:"bucket" jsonschema:"title=Bucket,description=Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery." jsonschema_extras:"order=4"`
	BucketPath       string             `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects to Google Cloud Storage's bucket." jsonschema_extras:"order=5"`
	BillingProjectID string             `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Billing Project ID connected to the BigQuery dataset. Defaults to Project ID if not specified." jsonschema_extras:"order=6"`
	HardDelete       bool               `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=7"`
	Credentials      *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=8"`
	Schedule         m.ScheduleConfig   `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger    dbt.JobConfig      `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	DisableFieldTruncation bool   `json:"disableFieldTruncation,omitempty" jsonschema:"title=Disable Field Truncation,description=Disables truncation of materialized fields. May result in errors for documents with extremely large values or complex nested structures."`
	NoFlowDocument         bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags           string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
	Endpoint               string `json:"endpoint,omitempty" jsonschema:"title=BigQuery Endpoint,description=The BigQuery API endpoint to connect to. Use if you're materializing to a compatible API that isn't provided by Google. The client uses this as the full API base path — for example the goccy/bigquery-emulator requires http://host:9050/bigquery/v2/ rather than http://host:9050."`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"project_id", c.ProjectID},
		{"dataset", c.Dataset},
		{"region", c.Region},
		{"bucket", c.Bucket},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Credentials are required unless a custom endpoint is configured, in which case they are
	// optional: an emulator behind the endpoint needs none, while a proxy in front of the real
	// service may still require them. Credentials accompanying an endpoint must arrive via the
	// structured 'credentials' configuration: the deprecated top-level 'credentials_json' exists
	// only so pre-existing configs keep working, and rejecting the combination here keeps such
	// credentials from being silently ignored by CredentialsClientOption.
	if c.Credentials != nil {
		if err := c.Credentials.Validate(); err != nil {
			return err
		}
	} else if c.Advanced.Endpoint == "" {
		// Sanity check: Are the provided credentials valid JSON? A common error is to upload
		// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
		// directly to bigquery.NewClient.
		if !json.Valid([]byte(c.CredentialsJSON)) {
			return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
		}
	} else if c.CredentialsJSON != "" {
		return fmt.Errorf("credentials for a custom endpoint must be provided via 'credentials', not the deprecated 'credentials_json' field")
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	if err := blob.ValidateBucketPath(c.effectiveBucketPath()); err != nil {
		return fmt.Errorf("bucket_path %w", err)
	}

	return nil
}

func (c config) effectiveBucketPath() string {
	// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
	// chars in the URI and so that the object key does not start with a /.
	return strings.TrimPrefix(c.BucketPath, "/")
}

func (c config) CredentialsClientOption() (option.ClientOption, error) {
	if c.Credentials == nil {
		// A custom endpoint without structured credentials means an unauthenticated server, like
		// an emulator; a proxy in front of the real authenticated service takes its credentials
		// through 'credentials'. Validate guarantees the deprecated top-level 'credentials_json'
		// is empty whenever an endpoint is configured, so nothing is ignored here.
		if c.Advanced.Endpoint != "" {
			return option.WithoutAuthentication(), nil
		}
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

// clientOptions returns the client options shared by every API client the
// connector constructs, including the server-detection probe, so the probe
// observes the server through exactly the same credentials and endpoint as
// regular operation.
func (c config) clientOptions() ([]option.ClientOption, error) {
	credOption, err := c.CredentialsClientOption()
	if err != nil {
		return nil, err
	}
	var clientOpts = []option.ClientOption{
		credOption,
		option.WithUserAgent("EstuaryFlow (GPN:Estuary;)"),
	}
	if c.Advanced.Endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(c.Advanced.Endpoint))
	}
	return clientOpts, nil
}

// effectiveBillingProjectID allows overriding the main 'project_id' with
// 'billing_project_id' for client operation billing.
func (c config) effectiveBillingProjectID() string {
	if c.BillingProjectID != "" {
		return c.BillingProjectID
	}
	return c.ProjectID
}

func (c config) client(ctx context.Context, ep *sql.Endpoint[config], isEmulatorGoccy bool) (*client, error) {
	clientOpts, err := c.clientOptions()
	if err != nil {
		return nil, err
	}

	bigqueryClient, err := bigquery.NewClient(ctx, c.effectiveBillingProjectID(), clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	// Use the much faster storage read API for reading query results if the
	// authorization provides sufficient access, typically via the "BigQuery
	// Read Session User" role. Result iterators will automatically fall back to
	// the standard but much slower job/table read API if the storage API can't
	// be accessed. The goccy emulator does not implement the Storage Read API,
	// so it is skipped only for the detected emulator — a SaaS server behind a
	// custom endpoint still gets it.
	if !isEmulatorGoccy {
		if err := bigqueryClient.EnableStorageReadClient(ctx, clientOpts...); err != nil {
			return nil, fmt.Errorf("enabling storage read client: %w", err)
		}
	}

	return &client{
		bigqueryClient:      bigqueryClient,
		cfg:                 c,
		ep:                  ep,
		isEmulatorGoccy:     isEmulatorGoccy,
		emulatorTempTableMu: &sync.Mutex{},
	}, nil
}

func (c config) DefaultNamespace() string {
	return c.Dataset
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type tableConfig struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Table in the BigQuery dataset to store materialized result in." jsonschema_extras:"x-collection-name=true"`
	Dataset   string `json:"dataset,omitempty" jsonschema:"title=Alternative Dataset,description=Alternative dataset for this table (optional). Must be located in the region set in the endpoint configuration." jsonschema_extras:"x-schema-name=true"`
	Delta     bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false." jsonschema_extras:"x-delta-updates=true"`
	projectID string
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Dataset == "" {
		c.Dataset = cfg.Dataset
	}
	c.projectID = cfg.ProjectID

	return c
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	return []string{c.projectID, c.Dataset, c.Table}, c.Delta, nil
}

func Driver() *sql.Driver[config, tableConfig] {
	return newBigQueryDriver()
}

func newBigQueryDriver() *sql.Driver[config, tableConfig] {
	// PreReqs receives only the config, but must skip the GCS bucket check for
	// a detected goccy emulator (see preReqs). CheckPrerequisites is invoked on
	// the materializer built from NewEndpoint's result, so NewEndpoint — and
	// with it the detection probe — always runs first; its result is recorded
	// here for PreReqs to consult rather than re-probing the server.
	var detectedEmulatorGoccy bool
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-bigquery",
		StartTunnel:      func(ctx context.Context, cfg config) error { return nil },
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"project_id":  cfg.ProjectID,
				"dataset":     cfg.Dataset,
				"region":      cfg.Region,
				"bucket":      cfg.Bucket,
				"bucket_path": cfg.effectiveBucketPath(),
			}).Info("creating bigquery endpoint")

			// Classify the server once per session; everything emulator-gated
			// downstream keys off this single result (REQ-002). Spec never
			// reaches NewEndpoint, so this probe never runs without a
			// genuine need for a live server.
			isEmulatorGoccy, err := detectEmulatorGoccy(ctx, cfg)
			if err != nil {
				return nil, err
			}
			detectedEmulatorGoccy = isEmulatorGoccy

			dialect := bqDialect(featureFlags, isEmulatorGoccy)
			templates := renderTemplates(dialect, isEmulatorGoccy)

			// BigQuery's default SerPolicy has historically had limits of 1500
			// for truncation, rather than the more common 1000.
			serPolicy := &pf.SerPolicy{
				StrTruncateAfter:       1 << 16, // 64 KiB
				NestedObjTruncateAfter: 1500,
				ArrayTruncateAfter:     1500,
			}
			if cfg.Advanced.DisableFieldTruncation {
				serPolicy = boilerplate.SerPolicyDisabled
			}

			return &sql.Endpoint[config]{
				Config:          cfg,
				Dialect:         dialect,
				SerPolicy:       serPolicy,
				MetaCheckpoints: nil,
				NewClient: func(ctx context.Context, _ string, ep *sql.Endpoint[config]) (sql.Client, error) {
					return ep.Config.client(ctx, ep, isEmulatorGoccy)
				},
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       prepareNewTransactor(templates, isEmulatorGoccy),
				ConcurrentApply:     true,
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
				Options: m.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &m.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(cfg.ProjectID + cfg.Dataset),
					},
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: func(ctx context.Context, cfg config) *cerrors.PrereqErr {
			return preReqs(ctx, cfg, detectedEmulatorGoccy)
		},
	}
}

func main() {
	boilerplate.RunMain(newBigQueryDriver())
}
