package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	var resp, err = NewDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestIsSerializationFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", errors.New("boom"), false},
		{"1023", &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "1023", Detail: "Serializable isolation violation on table - 1, transactions forming the cycle are: 2, 3 (pid:4)"}, true},
		{"wrapped 1023", fmt.Errorf("writing applied tokens: %w", &pgconn.PgError{Code: "XX000", Message: "1023"}), true},
		{"deadlock", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}, true},
		{"other internal", &pgconn.PgError{Code: "XX000", Message: "1018"}, false},
		{"column exists", &pgconn.PgError{Code: "42701"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSerializationFailure(tc.err))
		})
	}
}

func TestRetryOnSerializationFailure(t *testing.T) {
	serializationRetryDelay = 0
	var violation = &pgconn.PgError{Code: "XX000", Message: "1023"}

	var calls int
	require.NoError(t, retryOnSerializationFailure(context.Background(), func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("writing applied tokens: %w", violation)
		}
		return nil
	}))
	require.Equal(t, 3, calls)

	calls = 0
	require.ErrorIs(t, retryOnSerializationFailure(context.Background(), func() error {
		calls++
		return violation
	}), violation)
	require.Equal(t, maxTxnAttempts, calls)

	calls = 0
	var other = errors.New("not transient")
	require.ErrorIs(t, retryOnSerializationFailure(context.Background(), func() error {
		calls++
		return other
	}), other)
	require.Equal(t, 1, calls)
}
