package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/go/dbt"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/microsoft/go-mssqldb/azuread"
)

var featureFlagDefaults = map[string]bool{}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

type config struct {
	ClientID           string                     `json:"clientID" jsonschema:"title=Client ID,description=Client ID for the service principal used to connect to the Azure Fabric Warehouse." jsonschema_extras:"order=0"`
	ClientSecret       string                     `json:"clientSecret" jsonschema:"title=Client Secret,description=Client Secret for the service principal used to connect to the Azure Fabric Warehouse." jsonschema_extras:"order=1,secret=true"`
	Warehouse          string                     `json:"warehouse" jsonschema:"title=Warehouse,description=Name of the Azure Fabric Warehouse to connect to." jsonschema_extras:"order=2"`
	Schema             string                     `json:"schema" jsonschema:"title=Schema,description=Schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=3"`
	ConnectionString   string                     `json:"connectionString" jsonschema:"title=Connection String,description=SQL connection string for the Azure Fabric Warehouse." jsonschema_extras:"order=4"`
	StorageAccountName string                     `json:"storageAccountName" jsonschema:"title=Storage Account Name,description=Name of the storage account that temporary files will be written to." jsonschema_extras:"order=5"`
	StorageAccountKey  string                     `json:"storageAccountKey" jsonschema:"title=Storage Account Key,description=Storage account key for the storage account that temporary files will be written to." jsonschema_extras:"order=6,secret=true"`
	ContainerName      string                     `json:"containerName" jsonschema:"title=Storage Account Container Name,description=Name of the container in the storage account where temporary files will be written." jsonschema_extras:"order=7"`
	Directory          string                     `json:"directory,omitempty" jsonschema:"title=Directory,description=Optional prefix that will be used for temporary files." jsonschema_extras:"order=8"`
	HardDelete         bool                       `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=9"`
	Schedule           boilerplate.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger      dbt.JobConfig              `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"clientID", c.ClientID},
		{"clientSecret", c.ClientSecret},
		{"warehouse", c.Warehouse},
		{"schema", c.Schema},
		{"connectionString", c.ConnectionString},
		{"storageAccountName", c.StorageAccountName},
		{"storageAccountKey", c.StorageAccountKey},
		{"containerName", c.ContainerName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Directory != "" {
		if strings.HasPrefix(c.Directory, "/") {
			return fmt.Errorf("directory %q cannot start with /", c.Directory)
		}
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	} else if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *config) db() (*stdsql.DB, error) {
	db, err := stdsql.Open(
		azuread.DriverName,
		fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%d;database=%s;fedauth=ActiveDirectoryServicePrincipal",
			c.ConnectionString, c.ClientID, c.ClientSecret, 1433, c.Warehouse,
		))
	if err != nil {
		return nil, err
	}

	return db, nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,description=Should updates to this table be done via delta updates." jsonschema_extras:"x-delta-updates=true"`

	warehouse string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the endpoint schema. This will be over-written by a present `schema` property
		// within `raw`.
		Schema:    ep.Config.(*config).Schema,
		warehouse: ep.Config.(*config).Warehouse,
	}
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.warehouse, c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-azure-fabric-warehouse",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		StartTunnel:      func(ctx context.Context, conf any) error { return nil },
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			var featureFlags = common.ParseFeatureFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.Warehouse, cfg.Schema}),
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     true,
				FeatureFlags:        featureFlags,
			}, nil
		},
		PreReqs: preReqs,
	}
}
