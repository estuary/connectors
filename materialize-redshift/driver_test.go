package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if testing.Short() {
		t.Skip("skipping test in short mode")
		return config{}
	}

	jsonBytes, err := exec.Command("sops", "--decrypt", "--output-type", "json", "testdata/config.local.yaml").Output()
	require.NoError(t, err)

	var out config
	require.NoError(t, json.Unmarshal(jsonBytes, &out))
	require.NoError(t, out.Validate())

	return out
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
			Delta: delta,
		}
	}

	// A staged transaction is named in the connector state by its manifest
	// key, which is a fresh UUID per transaction.
	sanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`).
				ReplaceAllString(s, "<uuid>")
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, sanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

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
			want: []error{fmt.Errorf("not authorized to write to \"%s%s\"", cfg.Bucket, cfg.BucketPath)},
		},
		{
			name: "unauthorized to bucket: secret access key",
			cfg: func(cfg config) config {
				cfg.AWSSecretAccessKey = "wrong" + cfg.AWSSecretAccessKey
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to \"%s%s\"", cfg.Bucket, cfg.BucketPath)},
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

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(ctx, tt.cfg(cfg)).Unwrap())
		})
	}
}
