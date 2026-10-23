package connector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleQueryTimeout(t *testing.T) {
	const timeout = 50 * time.Millisecond

	ctx, touch, cancel := ctxWithIdleQueryTimeout(context.Background(), timeout)
	for range 6 {
		time.Sleep(timeout / 2)
		touch()
	}
	require.NoError(t, ctx.Err(), "a touched context outlives its timeout")

	time.Sleep(2 * timeout)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorContains(t, context.Cause(ctx), "no query results for 50ms")
	cancel()
}
