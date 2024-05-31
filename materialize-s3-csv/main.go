package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/materialize"
)

type config struct {
	filesink.S3StoreConfig
	CsvConfig filesink.CsvConfig `json:"csvConfig,omitempty" jsonschema:"title=CSV Configuration,description=Configuration specific to materializing CSV files."`
}

func (c config) Validate() error {
	if err := c.S3StoreConfig.Validate(); err != nil {
		return err
	} else if err := c.CsvConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) CommonConfig() filesink.CommonConfig {
	interval, err := m.ParseDelay(c.UploadInterval)
	if err != nil {
		// Validated to parse in (config).Validate, so this should never fail.
		panic(fmt.Errorf("failed to parse UploadInterval: %w", err))
	}

	return filesink.CommonConfig{
		Prefix:         c.Prefix,
		Extension:      ".csv.gz",
		UploadInterval: interval,
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
		return filesink.NewS3Store(ctx, c.(config).S3StoreConfig)
	},
	NewEncoder: func(c filesink.Config, b *pf.MaterializationSpec_Binding, w io.WriteCloser) filesink.StreamEncoder {
		return filesink.NewCsvStreamEncoder(c.(config).CsvConfig, b, w)
	},
	NewConstraints: func(p *pf.Projection) *materialize.Response_Validated_Constraint {
		return filesink.StdConstraints(p)
	},
	DocumentationURL: func() string {
		return "https://go.estuary.dev/materialize-s3-csv"
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
