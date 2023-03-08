package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pc.SpecRequest{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestDiscover(t *testing.T) {
	// Discover for this capture does not actually make any external API or database calls, so we
	// can always test it as part of the normal unit tests.
	t.Setenv("TEST_DATABASE", "yes")

	startDate, err := time.Parse(time.RFC3339Nano, "2019-01-02T15:04:05.999999999Z")
	require.NoError(t, err)
	endDate, err := time.Parse(time.RFC3339Nano, "2019-02-03T15:04:05.999999999Z")
	require.NoError(t, err)

	capture := captureSpec(t, []string{"trades"}, []string{"AAPL"}, startDate, endDate, nil)
	capture.VerifyDiscover(context.Background(), t, nil...)
}
