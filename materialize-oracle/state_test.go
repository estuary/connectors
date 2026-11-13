package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBefore(t *testing.T) {
	var tag = taskTag("acmeCo/materialize-oracle")
	var before = newToken(tag)
	time.Sleep(2 * time.Millisecond)
	var cutoff = tokenBefore(tag, time.Now())

	require.Less(t, tag+"-", before)
	require.Less(t, before, cutoff)
	// Made in the same millisecond as the cutoff, or later.
	require.Less(t, cutoff, newToken(tag))

	// Another task's tokens sort outside of the range.
	var other = newToken(taskTag("acmeCo/other"))
	require.False(t, other > tag+"-" && other < cutoff, other)
}
