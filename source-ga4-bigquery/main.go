package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	bqclient "github.com/estuary/connectors/go/capture/bigquery/client"
	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// featureFlagDefaults declares the connector's feature flags and their default
// values. Currently empty; reserved for future use.
var featureFlagDefaults = map[string]bool{}

// Config tells the connector how to connect to and interact with BigQuery.
type Config struct {
	ProjectID   string                `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset(s) containing GA4 exports." jsonschema_extras:"order=0"`
	Dataset     string                `json:"dataset,omitempty" jsonschema:"title=Dataset,description=Optional. The specific BigQuery dataset containing GA4 exports. If unset all datasets in the project will be discovered for matching tables; in that case the credentials must have permission to list datasets in the project." jsonschema_extras:"order=1"`
	Credentials *bqclient.Credentials `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`
	Advanced    advancedConfig        `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	PollSchedule        string `json:"poll,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to execute the polling cycle. Accepts a Go duration string like '24h' or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to 'daily at 12:00Z'." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	WindowDays          int    `json:"window_days,omitempty" jsonschema:"title=Window Days,description=Number of recent daily tables treated as the live window. Each cycle final-captures the oldest table in this window (catching late events) and primary-captures the newest. Defaults to 4 which aligns with GA4's 72-hour late-event window."`
	CaptureIntermediate bool   `json:"capture_intermediate,omitempty" jsonschema:"title=Capture Intermediate Days,description=If enabled the connector queries every table in the live window on each poll rather than just the newest and oldest. Trades higher BigQuery scan cost for fresher intermediate-day data. Defaults to false."`
	MinDate             string `json:"min_date,omitempty" jsonschema:"title=Minimum Date,description=Optional YYYY-MM-DD cutoff. Tables for dates strictly before this are skipped. Used to bound the cost of an initial backfill." jsonschema_extras:"pattern=^[0-9]{4}-[0-9]{2}-[0-9]{2}$"`
	SourceTag           string `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
	BillingProjectID    string `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Project that BigQuery jobs are billed to. Defaults to Project ID if not specified."`
	Endpoint            string `json:"endpoint,omitempty" jsonschema:"title=BigQuery Endpoint,description=The BigQuery endpoint URI to connect to. Use if you're capturing from a compatible API that isn't provided by Google."`
	FeatureFlags        string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`

	parsedFeatureFlags map[string]bool
	parsedMinDate      string // YYYYMMDD form of MinDate (dashes stripped); empty if MinDate unset.
}

const (
	documentationURL    = "https://go.estuary.dev/source-ga4-bigquery"
	defaultPollSchedule = "daily at 12:00Z"
	defaultWindowDays   = 4
)

var minDateRe = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("missing 'project_id'")
	}
	if c.Advanced.Endpoint == "" {
		if c.Credentials == nil {
			return fmt.Errorf("missing 'credentials'")
		}
		if err := c.Credentials.Validate(); err != nil {
			return err
		}
	}
	if c.Advanced.PollSchedule != "" {
		if err := schedule.Validate(c.Advanced.PollSchedule); err != nil {
			return fmt.Errorf("invalid polling schedule %q: %w", c.Advanced.PollSchedule, err)
		}
	}
	if c.Advanced.WindowDays < 0 {
		return fmt.Errorf("'window_days' must be non-negative")
	}
	if c.Advanced.MinDate != "" {
		if !minDateRe.MatchString(c.Advanced.MinDate) {
			return fmt.Errorf("invalid 'min_date' %q: must be YYYY-MM-DD", c.Advanced.MinDate)
		}
		c.Advanced.parsedMinDate = strings.ReplaceAll(c.Advanced.MinDate, "-", "")
	}
	c.Advanced.parsedFeatureFlags = common.ParseFeatureFlags(c.Advanced.FeatureFlags, featureFlagDefaults)
	if c.Advanced.FeatureFlags != "" {
		log.WithField("flags", c.Advanced.parsedFeatureFlags).Info("parsed feature flags")
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = defaultPollSchedule
	}
	if c.Advanced.WindowDays == 0 {
		c.Advanced.WindowDays = defaultWindowDays
	}
}

func connectBigQuery(ctx context.Context, cfg *Config) (*bigquery.Client, error) {
	clientOpts, err := cfg.bigQueryClientOptions()
	if err != nil {
		return nil, err
	}
	var billingProjectID = cfg.Advanced.BillingProjectID
	if billingProjectID == "" {
		billingProjectID = cfg.ProjectID
	}
	return bqclient.Connect(ctx, billingProjectID, clientOpts...)
}

func (c *Config) credentialsClientOption() (option.ClientOption, error) {
	if c.Credentials == nil {
		return nil, fmt.Errorf("missing credentials")
	}
	return c.Credentials.ClientOption()
}

func (c *Config) bigQueryClientOptions() ([]option.ClientOption, error) {
	if c.Advanced.Endpoint != "" {
		return []option.ClientOption{
			option.WithEndpoint(c.Advanced.Endpoint),
			option.WithoutAuthentication(),
		}, nil
	}

	credOption, err := c.credentialsClientOption()
	if err != nil {
		return nil, err
	}
	return []option.ClientOption{credOption}, nil
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("GA4 BigQuery", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

var ga4Driver = &Driver{
	DocumentationURL: documentationURL,
	ConfigSchema:     generateConfigSchema(),
	Connect:          connectBigQuery,
}

func main() {
	boilerplate.RunMain(ga4Driver)
}
