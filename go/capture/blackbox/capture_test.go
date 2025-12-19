package blackbox

import (
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func TestSpec(t *testing.T) {
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Spec("Get Connector Spec")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDiscoverNoFilter(t *testing.T) {
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.DiscoverFull("All Bindings")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDiscoverFilterEvents(t *testing.T) {
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(`events`)
	tc.DiscoverFull("Events Only")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDiscoverFilterUnmatched(t *testing.T) {
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(`unmatched`)
	tc.DiscoverFull("Unmatched Filter")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestRun(t *testing.T) {
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(`events`)
	tc.DocumentSanitizers = []JSONSanitizer{{
		Matcher:     regexp.MustCompile(`"ts":"[^"]+"`),
		Replacement: `"ts":"<TIMESTAMP>"`,
	}}
	tc.Discover("Events Only")
	tc.Run("Capture Events", 1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestInvalidConfig(t *testing.T) {
	t.Skip("TODO: needs sanitization for timestamps and temp file paths in stderr output")
	tc, err := NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log

	// Modify the catalog to set an invalid rate
	tc.Capture.Catalog, err = sjson.SetBytes(tc.Capture.Catalog, `captures.acmeCo/test/source-hello-world.endpoint.local.config.rate`, -1)
	require.NoError(t, err)

	tc.Discover("With Invalid Config")
	tc.Run("With Invalid Config", 1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
