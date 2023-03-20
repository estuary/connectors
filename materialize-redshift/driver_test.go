package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TESTDB") != "yes" {
		t.Skipf("skipping %q: ${TESTDB} != \"yes\"", t.Name())
		return config{}
	}

	out := config{}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"REDSHIFT_ADDRESS", &out.Address},
		{"REDSHIFT_USER", &out.User},
		{"REDSHIFT_PASSWORD", &out.Password},
		{"REDSHIFT_DATABASE", &out.Database},
		{"REDSHIFT_BUCKET", &out.Bucket},
		{"AWS_ACCESS_KEY_ID", &out.AWSAccessKeyID},
		{"AWS_SECRET_ACCESS_KEY", &out.AWSSecretAccessKey},
		{"AWS_REGION", &out.Region},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}

	return out
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the TESTDB environment variable. It will take several minutes for this test to
	// complete (you should run it with a sufficient -timeout value).
	cfg := mustGetCfg(t)

	client := client{uri: cfg.toURI()}

	ctx := context.Background()

	sql.RunFenceTestCases(t,
		sql.FenceSnapshotPath,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		rsDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var err = client.withDB(func(db *stdsql.DB) error {
				var fenceUpdate strings.Builder
				if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				var _, err = db.Exec(fenceUpdate.String())
				return err
			})
			return err
		},
		func(table sql.Table) (out string, err error) {
			err = client.withDB(func(db *stdsql.DB) error {
				out, err = sql.StdDumpTable(ctx, db, table)
				return err
			})
			return
		},
	)
}

func TestPrereqs(t *testing.T) {
	// These tests assume that the configuration obtained from environment variables forms a valid
	// config that could be used to materialize into Redshift. Various parameters of the
	// configuration are then manipulated to test assertions for incorrect configs.

	cfg := mustGetCfg(t)

	nonExistentBucket := uuid.NewString()

	tests := []struct {
		name string
		cfg  func(config) config
		want []error
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.User = "wrong" + cfg.User
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) config {
				cfg.Address = "wrong." + cfg.Address
				return cfg
			},
			want: []error{fmt.Errorf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
		{
			name: "bucket doesn't exist",
			cfg: func(cfg config) config {
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
		{
			name: "unauthorized to bucket: access key id",
			cfg: func(cfg config) config {
				cfg.AWSAccessKeyID = "wrong" + cfg.AWSAccessKeyID
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to bucket %q", cfg.Bucket)},
		},
		{
			name: "unauthorized to bucket: secret access key",
			cfg: func(cfg config) config {
				cfg.AWSSecretAccessKey = "wrong" + cfg.AWSSecretAccessKey
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to bucket %q", cfg.Bucket)},
		},
		{
			name: "database problem and bucket problem",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{
				fmt.Errorf("database %q does not exist", "wrong"+cfg.Database),
				fmt.Errorf("bucket %q does not exist", nonExistentBucket),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawBytes, err := json.Marshal(tt.cfg(cfg))
			require.NoError(t, err)
			require.Equal(t, tt.want, prereqs(context.Background(), rawBytes).Unwrap())
		})
	}
}

func TestSpecification(t *testing.T) {
	var resp, err = newRedshiftDriver().
		Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_AIRBYTE_SOURCE})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
