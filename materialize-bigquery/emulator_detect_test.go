package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Response bodies recorded during the REQ-001 detection spike: SaaS from a
// jobs.query `SELECT 1` against project estuary-theatre, goccy from the same
// request against ghcr.io/goccy/bigquery-emulator. The discriminating signal
// is the `kind` discovery field, present only in Google's response.
const recordedSaaSQueryResponse = `{
	"kind": "bigquery#queryResponse",
	"schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
	"jobReference": {"projectId": "estuary-theatre", "jobId": "job_UEtM2s6qhtQXbeeYmuOfZLuepFOb", "location": "US"},
	"totalRows": "1",
	"rows": [{"f": [{"v": "1"}]}],
	"totalBytesProcessed": "0",
	"jobComplete": true,
	"cacheHit": false,
	"queryId": "job_UEtM2s6qhtQXbeeYmuOfZLuepFOb"
}`

const recordedGoccyQueryResponse = `{
	"jobReference": {"projectId": "test-project", "jobId": "ptPFmJtelKMJHBjXbAqhcYcLzcAf"},
	"schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
	"rows": [{"f": [{"v": "1"}]}],
	"totalRows": "1",
	"jobComplete": true
}`

// TestClassifyServer unit-tests classification and retry/fail-fast handling
// against recorded responses served from a local httptest server (REQ-002).
func TestClassifyServer(t *testing.T) {
	var ctx = context.Background()

	var serve = func(t *testing.T, handler http.HandlerFunc) *httptest.Server {
		t.Helper()
		var server = httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return server
	}

	t.Run("recorded-goccy-classifies-emulator", func(t *testing.T) {
		var server = serve(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(recordedGoccyQueryResponse))
		})

		// Exercise the full production entrypoint, including how it derives
		// client options and the probed project from the config.
		var cfg = config{
			ProjectID: "test-project",
			Advanced:  advancedConfig{Endpoint: server.URL + "/"},
		}
		isEmulator, err := detectEmulatorGoccy(ctx, cfg)
		require.NoError(t, err)
		require.True(t, isEmulator, "goccy-shaped response must classify as emulator")
	})

	t.Run("recorded-saas-classifies-saas", func(t *testing.T) {
		var server = serve(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(recordedSaaSQueryResponse))
		})

		var cfg = config{
			ProjectID: "estuary-theatre",
			Advanced:  advancedConfig{Endpoint: server.URL + "/"},
		}
		isEmulator, err := detectEmulatorGoccy(ctx, cfg)
		require.NoError(t, err)
		require.False(t, isEmulator, "SaaS-shaped response must classify as SaaS")
	})

	t.Run("inconclusive-retries-then-errors", func(t *testing.T) {
		var attempts atomic.Int32
		var server = serve(t, func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			http.Error(w, `{"error": {"code": 503, "message": "unavailable"}}`, http.StatusServiceUnavailable)
		})

		var retrySchedule = []time.Duration{time.Millisecond, time.Millisecond}
		_, err := classifyServerWithRetry(ctx, "test-project", server.URL, retrySchedule,
			option.WithEndpoint(server.URL+"/"),
			option.WithoutAuthentication(),
		)
		require.Error(t, err)
		require.Equal(t, int32(len(retrySchedule)+1), attempts.Load(), "every scheduled retry must be attempted")
		require.Contains(t, err.Error(), server.URL, "error must name the probed endpoint")

		// The underlying API error must remain unwrappable through the
		// fmt.Errorf %w chain.
		var gErr *googleapi.Error
		require.ErrorAs(t, err, &gErr)
		require.Equal(t, http.StatusServiceUnavailable, gErr.Code)
	})
}

const goccyProbeEndpoint = "http://localhost:9050/bigquery/v2/"

// TestDetectionProbe demonstrates the REQ-001 spike probe against all three
// live server arrangements the probe must distinguish. Subtests skip when
// their server isn't reachable so the default suite stays runnable everywhere;
// deterministic always-on coverage lives in TestClassifyServer.
func TestDetectionProbe(t *testing.T) {
	var ctx = context.Background()

	t.Run("goccy-emulator", func(t *testing.T) {
		requireGoccyReachable(t)

		isSaaS, err := classifyServerWithRetry(ctx, "test-project", goccyProbeEndpoint, nil,
			option.WithEndpoint(goccyProbeEndpoint),
			option.WithoutAuthentication(),
		)
		require.NoError(t, err)
		require.False(t, isSaaS, "goccy emulator must classify as emulator")
	})

	t.Run("saas", func(t *testing.T) {
		var projectID, creds = saasProbeCredentials(t)

		isSaaS, err := classifyServerWithRetry(ctx, projectID, googleBigQueryEndpoint, nil,
			option.WithCredentialsJSON(creds),
		)
		require.NoError(t, err)
		require.True(t, isSaaS, "real SaaS BigQuery must classify as SaaS")
	})

	// The advanced.endpoint field exists so users can point the connector at a
	// proxy in front of real BigQuery. Endpoint-set therefore must not imply
	// emulator: this subtest runs the probe through a local reverse proxy with
	// credentials present and expects a SaaS classification.
	t.Run("saas-behind-proxy", func(t *testing.T) {
		var projectID, creds = saasProbeCredentials(t)

		var target, err = url.Parse("https://bigquery.googleapis.com")
		require.NoError(t, err)
		var proxy = httptest.NewServer(&httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(target)
				pr.Out.Host = target.Host
			},
		})
		defer proxy.Close()

		isSaaS, err := classifyServerWithRetry(ctx, projectID, proxy.URL, nil,
			option.WithEndpoint(proxy.URL+"/bigquery/v2/"),
			option.WithCredentialsJSON(creds),
		)
		require.NoError(t, err)
		require.True(t, isSaaS, "SaaS behind a proxy must still classify as SaaS")
	})
}

// requireGoccyReachable skips unless a goccy emulator is listening locally
// (e.g. docker run -p 9050:9050 ghcr.io/goccy/bigquery-emulator
// --project=test-project --dataset=testing).
func requireGoccyReachable(t *testing.T) {
	t.Helper()

	var u, err = url.Parse(goccyProbeEndpoint)
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", u.Host, time.Second)
	if err != nil {
		t.Skipf("goccy emulator not reachable at %s: %v", u.Host, err)
	}
	require.NoError(t, conn.Close())

	// Reachable but not actually goccy (or not ready) would make the probe
	// assertion meaningless, so confirm the server responds to the REST API.
	resp, err := http.Get(strings.TrimSuffix(goccyProbeEndpoint, "/") + "/projects/test-project/datasets")
	if err != nil {
		t.Skipf("goccy emulator not responding at %s: %v", goccyProbeEndpoint, err)
	}
	require.NoError(t, resp.Body.Close())
}

// saasProbeCredentials decrypts the integration-test endpoint config,
// mirroring how the TestIntegration harness provides it. Skips in -short mode
// alongside the rest of the integration suite.
func saasProbeCredentials(t *testing.T) (string, []byte) {
	t.Helper()

	if testing.Short() {
		t.Skip()
	}

	projectID, err := exec.Command("sops", "--decrypt", "--extract", `["project_id"]`, "testdata/config.yaml").Output()
	require.NoError(t, err, "decrypting testdata/config.yaml (project_id)")
	creds, err := exec.Command("sops", "--decrypt", "--extract", `["credentials_json_sops"]`, "testdata/config.yaml").Output()
	require.NoError(t, err, "decrypting testdata/config.yaml (credentials_json_sops)")

	return strings.TrimSpace(string(projectID)), creds
}
