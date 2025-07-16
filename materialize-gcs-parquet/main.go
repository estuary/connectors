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

type config struct {
	filesink.GCSStoreConfig
	ParquetConfig filesink.ParquetConfig `json:"parquetConfig,omitempty" jsonschema:"title=Parquet Configuration,description=Configuration specific to materializing parquet files."`
}

func (c config) Validate() error {
	if err := c.GCSStoreConfig.Validate(); err != nil {
		return err
	} else if err := c.ParquetConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) CommonConfig() filesink.CommonConfig {
	return filesink.CommonConfig{
		Prefix:         c.Prefix,
		Extension:      ".parquet",
		UploadInterval: c.UploadInterval,
		FileSizeLimit:  c.FileSizeLimit,
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
	NewStore: func(ctx context.Context, c filesink.Config) (filesink.Store, error) {
		return filesink.NewGCSStore(ctx, c.(config).GCSStoreConfig)
	},
	NewEncoder: func(c filesink.Config, b *pf.MaterializationSpec_Binding, w io.WriteCloser) (filesink.StreamEncoder, error) {
		return filesink.NewParquetStreamEncoder(c.(config).ParquetConfig, b, w)
	},
	NewConstraints: func(p *pf.Projection) *materialize.Response_Validated_Constraint {
		return filesink.StdConstraints(p)
	},
	DocumentationURL: func() string {
		return "https://go.estuary.dev/materialize-gcs-parquet"
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
