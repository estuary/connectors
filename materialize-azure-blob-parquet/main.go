package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/estuary/connectors/filesink"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/materialize"
)

var featureFlagDefaults = map[string]bool{
	// Starting on 18-Aug-2025 newly created parquet materializations will use
	// STRING logical type for UUID fields, rather than a UUID logical type.
	"uuid_logical_type": false,
	"retain_existing_data_on_backfill": false,
}

type config struct {
	filesink.AzureBlobConfig
	ParquetConfig filesink.ParquetConfig `json:"parquetConfig,omitempty" jsonschema:"title=Parquet Configuration,description=Configuration specific to materializing parquet files."`
	Advanced      advancedConfig         `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
	if err := c.AzureBlobConfig.Validate(); err != nil {
		return err
	} else if err := c.ParquetConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) CommonConfig() filesink.CommonConfig {
	return filesink.CommonConfig{
		Prefix:                c.Prefix,
		Extension:             ".parquet",
		UploadInterval:        c.UploadInterval,
		FileSizeLimit:         c.FileSizeLimit,
		CaseInsensitiveFields: false,
	}
}

var driver = filesink.FileDriver{
	NewConfig: func(raw json.RawMessage) (filesink.Config, error) {
		var cfg config
		if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg, nil
	},
	NewStore: func(ctx context.Context, c filesink.Config, featureFlags map[string]bool) (filesink.Store, error) {
		return filesink.NewAzureBlob(ctx, c.(config).AzureBlobConfig)
	},
	NewWriter: func(c filesink.Config, featureFlags map[string]bool, b *pf.MaterializationSpec_Binding, w io.WriteCloser) (filesink.StreamWriter, error) {
		return filesink.NewParquetWriter(c.(config).ParquetConfig, b, w, featureFlags["uuid_logical_type"])
	},
	NewConstraints: func(p *pf.Projection) *materialize.Response_Validated_Constraint {
		return filesink.StdConstraints(p)
	},
	DocumentationURL: func() string {
		return "https://go.estuary.dev/materialize-azure-blob-parquet"
	},
	ConfigSchema: func() ([]byte, error) {
		endpointSchema, err := schemagen.GenerateSchema("EndpointConfig", config{}).MarshalJSON()
		if err != nil {
			return nil, err
		}

		return endpointSchema, nil
	},
}

func main() {
	boilerplate.RunMain(driver)
}
