package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/stretchr/testify/require"
)

// Sanitizers redact wall-clock timestamps that the connector stamps onto
// state checkpoints and document _meta. These would otherwise drift on every
// run and make the snapshot useless.
var documentSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"polled":"\d{4}-\d{2}-\d{2}T[^"]+"`), Replacement: `"polled":"REDACTED"`},
}

var checkpointSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"last_poll_started":"\d{4}-\d{2}-\d{2}T[^"]+"`), Replacement: `"last_poll_started":"REDACTED"`},
}

// newTestCapture returns a TranscriptCapture wired to the public GA4 sample
// dataset. Skips the test if TEST_DATABASE != "yes". The sample is provided
// by Google, so no DB control connection is needed for setup or teardown.
//
// SHUTDOWN_AFTER_POLLING is set so the connector exits cleanly once it has no
// immediate work pending; given the daily polling cadence, no test scenario
// benefits from waiting for another scheduler loop.
func newTestCapture(t testing.TB) *blackbox.TranscriptCapture {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: TEST_DATABASE != \"yes\"", t.Name())
	}

	os.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	t.Cleanup(func() { os.Unsetenv("SHUTDOWN_AFTER_POLLING") })

	credPath := strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credBytes, err := os.ReadFile(credPath)
	require.NoError(t, err)

	tc, err := blackbox.NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = func(args ...any) { t.Log(args...) }
	tc.DocumentSanitizers = documentSanitizers
	tc.CheckpointSanitizers = checkpointSanitizers

	require.NoError(t, tc.Capture.EditConfig("project_id", publicSampleProjectID))
	require.NoError(t, tc.Capture.EditConfig("dataset", publicSampleDataset))
	require.NoError(t, tc.Capture.EditConfig("credentials.auth_type", "CredentialsJSON"))
	require.NoError(t, tc.Capture.EditConfig("credentials.credentials_json", string(credBytes)))
	if *testProjectID != "" {
		require.NoError(t, tc.Capture.EditConfig("advanced.billing_project_id", *testProjectID))
	}
	return tc
}

// TestDiscoverPublicSample exercises Discover against the public GA4 sample
// dataset and snapshots the full discovered binding (resource config, key,
// and collection schema). The public sample only has events_YYYYMMDD tables,
// so we expect exactly one binding.
func TestDiscoverPublicSample(t *testing.T) {
	tc := newTestCapture(t)
	tc.Capture.DiscoveryFilter = regexp.MustCompile("/events$")
	tc.DiscoverFull("Discover Public Sample")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCapturePublicSample captures five days of events from the public GA4
// sample and snapshots various aspects of the result. Note that this fetches
// approximately 1GB of data from BigQuery.
func TestCapturePublicSample(t *testing.T) {
	tc := newTestCapture(t)
	require.NoError(t, tc.Capture.EditConfig("advanced.min_date", "2021-01-27"))
	tc.Capture.DiscoveryFilter = regexp.MustCompile("/events$")

	tc.Discover("Discover Public Sample")
	tc.Run("Capture All Events in Window", -1)
	cupaloy.SnapshotT(t, summarizeCapture(tc.Transcript.String()))
}

// summarizeTranscript returns a capture transcript digest suitable for
// snapshotting when the full transcript is too massive. It includes the
// SHA-256 hash of the full transcript, plus head and tail slices.
func summarizeCapture(transcript string) string {
	const headTailLines = 300
	var b strings.Builder

	var checksum = sha256.Sum256([]byte(transcript))
	fmt.Fprintf(&b, "=== Transcript Metadata ===\nChecksum: %s\nTotal Size: %d\n\n", hex.EncodeToString(checksum[:]), len(transcript))

	lines := strings.Split(strings.TrimRight(transcript, "\n"), "\n")
	if len(lines) <= 2*headTailLines {
		b.WriteString(strings.Join(lines, "\n"))
		b.WriteString("\n")
		return b.String()
	}

	b.WriteString(strings.Join(lines[:headTailLines], "\n"))
	fmt.Fprintf(&b, "\n\n=== ... %d transcript lines elided ... ===\n\n", len(lines)-2*headTailLines)
	b.WriteString(strings.Join(lines[len(lines)-headTailLines:], "\n"))
	b.WriteString("\n")
	return b.String()
}
