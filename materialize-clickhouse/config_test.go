package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestClickHouseConfig(t *testing.T) {
	validConfig := config{
		Address: "clickhouse.example.com:9000",
		Credentials: credentialConfig{
			AuthType:         UserPass,
			usernamePassword: usernamePassword{Username: "default", Password: "password"},
		},
		Database: "test_db",
	}
	require.NoError(t, validConfig.Validate())

	noAddress := validConfig
	noAddress.Address = ""
	require.Error(t, noAddress.Validate())

	noUser := validConfig
	noUser.Credentials.Username = ""
	require.Error(t, noUser.Validate())

	noPass := validConfig
	noPass.Credentials.Password = ""
	require.Error(t, noPass.Validate())

	noDatabase := validConfig
	noDatabase.Database = ""
	require.Error(t, noDatabase.Validate())
}

func TestDefaultNamespace(t *testing.T) {
	var cfg config
	require.Equal(t, "", cfg.DefaultNamespace())
}

func TestCredentialValidation(t *testing.T) {
	var invalid = credentialConfig{AuthType: "bogus"}
	require.ErrorContains(t, invalid.Validate(), "invalid credentials auth type")
}

func TestTableConfigValidation(t *testing.T) {
	var empty tableConfig
	require.ErrorContains(t, empty.Validate(), "missing table")

	var valid = tableConfig{Table: "my_table"}
	require.NoError(t, valid.Validate())
}

func TestResolvedAddress(t *testing.T) {
	// With port — unchanged.
	var cfg = config{Address: "host:1234"}
	require.Equal(t, "host:1234", cfg.resolvedAddress())

	// Without port — appends :9000.
	cfg.Address = "host"
	require.Equal(t, "host:9000", cfg.resolvedAddress())
}

func TestUnmarshalState(t *testing.T) {
	var tr transactor

	// nil state is a no-op.
	require.NoError(t, tr.UnmarshalState(nil))
	require.Equal(t, uint64(0), tr.version)

	// "null" state is a no-op.
	require.NoError(t, tr.UnmarshalState(json.RawMessage("null")))
	require.Equal(t, uint64(0), tr.version)

	// Valid JSON sets version.
	require.NoError(t, tr.UnmarshalState(json.RawMessage(`{"version":42}`)))
	require.Equal(t, uint64(42), tr.version)

	// Invalid JSON returns an error.
	require.Error(t, tr.UnmarshalState(json.RawMessage("not-json")))
}

func TestAcknowledge(t *testing.T) {
	var tr transactor
	state, err := tr.Acknowledge(t.Context())
	require.NoError(t, err)
	require.Nil(t, state)
}

func TestConnectorStateRoundTrip(t *testing.T) {
	var cs = connectorState{Version: 42}
	data, err := json.Marshal(cs)
	require.NoError(t, err)

	var cs2 connectorState
	require.NoError(t, json.Unmarshal(data, &cs2))
	require.Equal(t, cs, cs2)
}

func TestTombstoneValue(t *testing.T) {
	tests := []struct {
		name      string
		col       sql.Column
		wantNil   bool
		wantValue any
	}{
		{
			name:    "nullable column returns nil",
			col:     sql.Column{MustExist: false, MappedType: sql.MappedType{DDL: "String"}},
			wantNil: true,
		},
		{
			name:      "non-nullable String returns empty string",
			col:       sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "String"}},
			wantValue: "",
		},
		{
			name:      "non-nullable Bool returns false",
			col:       sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "Bool"}},
			wantValue: false,
		},
		{
			name:      "non-nullable Int64 returns 0",
			col:       sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "Int64"}},
			wantValue: int64(0),
		},
		{
			name:      "non-nullable Float64 returns 0",
			col:       sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "Float64"}},
			wantValue: float64(0),
		},
		{
			name:    "non-nullable Date32 returns clamped minimum date",
			col:     sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "Date32"}},
			wantNil: false, // checked separately below
		},
		{
			name:    "non-nullable DateTime64 returns clamped minimum datetime",
			col:     sql.Column{MustExist: true, MappedType: sql.MappedType{DDL: "DateTime64(6, 'UTC')"}},
			wantNil: false, // checked separately below
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got = tombstoneValue(tt.col)
			if tt.wantNil {
				require.Nil(t, got)
				return
			}

			// Date types: verify they're time.Time values within the valid range.
			if strings.HasPrefix(tt.col.DDL, "Date") {
				tv, ok := got.(time.Time)
				require.True(t, ok, "expected time.Time, got %T", got)
				require.False(t, tv.IsZero(), "expected non-zero time")
				require.GreaterOrEqual(t, tv.Year(), 1900)
				return
			}

			require.Equal(t, tt.wantValue, got)
		})
	}
}

func TestSpecification(t *testing.T) {
	resp, err := newClickHouseDriver().
		Spec(t.Context(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
