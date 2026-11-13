package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBefore(t *testing.T) {
	var before = newToken()
	time.Sleep(2 * time.Millisecond)
	var now = time.Now()
	var cutoff = tokenBefore(now)

	require.Less(t, before, cutoff)
	// Made in the same millisecond as the cutoff, or later.
	require.Less(t, cutoff, newToken())
}
