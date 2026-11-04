package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/retry"
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

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"plain", errors.New("boom"), retry.Fatal},
		{"1023", &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "1023", Detail: "Serializable isolation violation on table - 1, transactions forming the cycle are: 2, 3 (pid:4)"}, retry.Retryable},
		{"wrapped 1023", fmt.Errorf("writing applied tokens: %w", &pgconn.PgError{Code: "XX000", Message: "1023"}), retry.Retryable},
		{"deadlock", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}, retry.Retryable},
		{"other internal", &pgconn.PgError{Code: "XX000", Message: "1018"}, retry.Fatal},
		{"column exists", &pgconn.PgError{Code: "42701"}, retry.Fatal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classify(tc.err))
		})
	}
}

func TestCommitRetry(t *testing.T) {
	var policy = commitRetry
	policy.InitialBackoff = 0
	var violation = &pgconn.PgError{Code: "XX000", Message: "1023"}

	var calls int
	require.NoError(t, policy.Do(t.Context(), "op", func(context.Context) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("writing applied tokens: %w", violation)
		}
		return nil
	}))
	require.Equal(t, 3, calls)

	calls = 0
	require.ErrorIs(t, policy.Do(t.Context(), "op", func(context.Context) error {
		calls++
		return violation
	}), violation)
	require.Equal(t, policy.MaxAttempts, calls)

	calls = 0
	var other = errors.New("not transient")
	require.ErrorIs(t, policy.Do(t.Context(), "op", func(context.Context) error {
		calls++
		return other
	}), other)
	require.Equal(t, 1, calls)
}
