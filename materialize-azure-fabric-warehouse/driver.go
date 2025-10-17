package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/microsoft/go-mssqldb/azuread"
)

var featureFlagDefaults = map[string]bool{
	"datetime_keys_as_string": true,
	"retain_existing_data_on_backfill": false,
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

type config struct {
	ClientID           string           `json:"clientID" jsonschema:"title=Client ID,description=Client ID for the service principal used to connect to the Azure Fabric Warehouse." jsonschema_extras:"order=0"`
	ClientSecret       string           `json:"clientSecret" jsonschema:"title=Client Secret,description=Client Secret for the service principal used to connect to the Azure Fabric Warehouse." jsonschema_extras:"order=1,secret=true"`
	Warehouse          string           `json:"warehouse" jsonschema:"title=Warehouse,description=Name of the Azure Fabric Warehouse to connect to." jsonschema_extras:"order=2"`
	Schema             string           `json:"schema" jsonschema:"title=Schema,description=Schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=3"`
	ConnectionString   string           `json:"connectionString" jsonschema:"title=Connection String,description=SQL connection string for the Azure Fabric Warehouse." jsonschema_extras:"order=4"`
	StorageAccountName string           `json:"storageAccountName" jsonschema:"title=Storage Account Name,description=Name of the storage account that temporary files will be written to." jsonschema_extras:"order=5"`
	StorageAccountKey  string           `json:"storageAccountKey" jsonschema:"title=Storage Account Key,description=Storage account key for the storage account that temporary files will be written to." jsonschema_extras:"order=6,secret=true"`
	ContainerName      string           `json:"containerName" jsonschema:"title=Storage Account Container Name,description=Name of the container in the storage account where temporary files will be written." jsonschema_extras:"order=7"`
	Directory          string           `json:"directory,omitempty" jsonschema:"title=Directory,description=Optional prefix that will be used for temporary files." jsonschema_extras:"order=8"`
	HardDelete         bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=9"`
	Schedule           m.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger      dbt.JobConfig    `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

func (c config) Validate() error {
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

func (c config) db() (*stdsql.DB, error) {
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

func (c config) DefaultNamespace() string {
	return c.Schema
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,description=Should updates to this table be done via delta updates." jsonschema_extras:"x-delta-updates=true"`

	warehouse string
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	return nil
}

func (r tableConfig) WithDefaults(cfg config) tableConfig {
	if r.Schema == "" {
		r.Schema = cfg.Schema
	}
	r.warehouse = cfg.Warehouse

	return r
}

func (r tableConfig) Parameters() ([]string, bool, error) {
	return []string{r.warehouse, r.Schema, r.Table}, r.Delta, nil
}

// isCaseInsensitiveDatabase checks if the database uses a case-insensitive collation
// by querying DATABASEPROPERTYEX and looking for _CI_ in the collation name.
// SQL Server collations use _CI_ for case-insensitive and _CS_ for case-sensitive.
func isCaseInsensitiveDatabase(ctx context.Context, db *stdsql.DB, warehouse string) (bool, error) {
	// Create a temporary dialect just to get the Literal function for the query
	tmpDialect := createDialect(featureFlagDefaults, false)
	var collation string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT DATABASEPROPERTYEX(%s, 'Collation')",
		tmpDialect.Literal(warehouse),
	)).Scan(&collation); err != nil {
		return false, fmt.Errorf("querying database collation: %w", err)
	}

	return strings.Contains(collation, "_CI_"), nil
}

func newDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-azure-fabric-warehouse",
		StartTunnel:      func(ctx context.Context, cfg config) error { return nil },
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			// Query database collation to determine if resources are case-insensitive
			db, err := cfg.db()
			if err != nil {
				return nil, fmt.Errorf("opening database connection: %w", err)
			}
			defer db.Close()

			caseInsensitiveResources, err := isCaseInsensitiveDatabase(ctx, db, cfg.Warehouse)
			if err != nil {
				return nil, err
			}

			dialect := createDialect(featureFlags, caseInsensitiveResources)
			templates := renderTemplates(dialect)
			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.Warehouse, cfg.Schema}),
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       newTransactor,
				ConcurrentApply:     true,
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
				Options: m.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &m.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(cfg.ConnectionString),
					},
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: preReqs,
	}
}
