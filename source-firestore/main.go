package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
)

type backfillMode string

const (
	backfillModeSync  = backfillMode("sync")
	backfillModeNone  = backfillMode("none")
	backfillModeAsync = backfillMode("async")
)

type resource struct {
	Path          string       `json:"path" jsonschema:"title=Path to Collection,description=Supports parent/*/nested to capture all nested collections of parent's children"`
	BackfillMode  backfillMode `json:"backfillMode" jsonschema:"title=Backfill Mode,description=Configures the handling of data already in the collection. Refer to go.estuary.dev/source-firestore for details or just stick with 'async'. Has no effect if changed after a binding is added.,enum=async,enum=none,enum=sync"`
	InitTimestamp string       `json:"initTimestamp,omitempty" jsonschema:"Initial Replication Timestamp,description=Optionally overrides the initial replication timestamp (which is either Zero or Now depending on the backfill mode). Has no effect if changed after a binding is added."`
}

func (r resource) Validate() error {
	if r.Path == "" {
		return fmt.Errorf("resource path unspecified")
	}
	if r.BackfillMode != backfillModeSync && r.BackfillMode != backfillModeAsync && r.BackfillMode != backfillModeNone {
		return fmt.Errorf("invalid backfill mode %q for %q", r.BackfillMode, r.Path)
	}
	if r.InitTimestamp != "" {
		if _, err := time.Parse(time.RFC3339Nano, r.InitTimestamp); err != nil {
			return fmt.Errorf("invalid initTimestamp value %q: %w", r.InitTimestamp, err)
		}
	}
	return nil
}

type config struct {
	// Service account JSON key to use as Application Default Credentials
	CredentialsJSON string `json:"googleCredentials" jsonschema:"title=Credentials,description=Google Cloud Service Account JSON credentials." jsonschema_extras:"secret=true,multiline=true"`

	// Optional name of the database to capture from
	DatabasePath string `json:"database,omitempty" jsonschema:"title=Database,description=Optional name of the database to capture from. Leave blank to autodetect. Typically \"projects/$PROJECTID/databases/(default)\"."`
}

var databasePathRe = regexp.MustCompile(`^projects/[^/]+/databases/[^/]+$`)

func (c *config) Validate() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("googleCredentials is required")
	}
	if c.DatabasePath != "" && !databasePathRe.MatchString(c.DatabasePath) {
		return fmt.Errorf("invalid database path %q", c.DatabasePath)
	}
	return nil
}

type driver struct{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Google Firestore", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Firestore Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-firestore",
	}, nil
}

func (driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{
		ActionDescription: "",
	}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}
