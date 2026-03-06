//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Address: "localhost:9000",
		Credentials: credentialConfig{
			AuthType: PASSWORD_AUTH_TYPE,
			User:     "flow",
			Password: "flow",
		},
		Database: "flow",
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func clickHouseGetSchema(ctx context.Context, db *stdsql.DB, database string, table string) (string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position",
		database, table,
	))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name string
		Type string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	slices.SortFunc(cols, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range cols {
		if err := enc.Encode(c); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db := cfg.openDB()
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newClickHouseDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := clickHouseGetSchema(ctx, db, cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db := cfg.openDB()
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newClickHouseDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := clickHouseGetSchema(ctx, db, cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			dialect := clickHouseDialect(cfg.Database)
			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = dialect.Identifier(col)
			}
			keys = append(keys, dialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, dialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			keys = append(keys, "`_version`")
			values = append(values, "1")
			keys = append(keys, "`_is_deleted`")
			values = append(values, "0")
			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", dialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err := db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table)))
		},
	)
}

func TestPrereqs(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) config
		want []string
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.Credentials.User = "wrong" + cfg.Credentials.User
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Credentials.Password = "wrong" + cfg.Credentials.Password
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) config {
				cfg.Address = "localhost:19000"
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual = preReqs(ctx, tt.cfg(cfg)).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}
