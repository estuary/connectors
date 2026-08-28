package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleBackoff(t *testing.T) {
	// The default maximum of one second means no extra wait, before or after the grace period.
	require.Equal(t, time.Duration(0), idleBackoff(1, 0, defaultIdleBackoffMax))
	require.Equal(t, time.Duration(0), idleBackoff(idleGraceEmpties+100, 0, defaultIdleBackoffMax))

	// With a raised maximum there is still no extra wait during the grace period.
	require.Equal(t, time.Duration(0), idleBackoff(1, 0, time.Minute))
	require.Equal(t, time.Duration(0), idleBackoff(idleGraceEmpties, 0, time.Minute))

	// Then doubling from the minimum up to the cap, where it stays.
	var backoff time.Duration
	var got []time.Duration
	for n := idleGraceEmpties + 1; n <= idleGraceEmpties+7; n++ {
		backoff = idleBackoff(n, backoff, time.Minute)
		got = append(got, backoff)
	}
	require.Equal(t, []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		time.Minute,
		time.Minute,
	}, got)

	// A configured maximum clamps the ramp.
	backoff = 0
	got = nil
	for n := idleGraceEmpties + 1; n <= idleGraceEmpties+4; n++ {
		backoff = idleBackoff(n, backoff, 5*time.Second)
		got = append(got, backoff)
	}
	require.Equal(t, []time.Duration{
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	}, got)
}
