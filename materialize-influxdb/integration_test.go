package connector

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	testutil.SkipRuntimeV1(t)

	t.Cleanup(func() { runFixtureScript(t, "cleanup.sh") })
	runFixtureScript(t, "setup.sh")

	raw, err := os.ReadFile("testdata/config.local.yaml")
	require.NoError(t, err)
	var values map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &values))
	raw, err = json.Marshal(values)
	require.NoError(t, err)
	cfg, err := resolveEndpointConfig(raw)
	require.NoError(t, err)

	t.Run("validate", func(t *testing.T) {
		_, err := Driver().Validate(t.Context(), &pm.Request_Validate{ConfigJson: raw})
		require.NoError(t, err)
		for _, tc := range []struct {
			name   string
			change func(*config)
			err    string
		}{
			{"unauthorized", func(c *config) { c.Credentials.AccessToken = "invalid-test-token" }, "authentication failed (HTTP 401)"},
			{"missing_bucket", func(c *config) { c.Bucket = "missing-bucket" }, "bucket must already exist"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				invalid := cfg
				tc.change(&invalid)
				raw, err := json.Marshal(invalid)
				require.NoError(t, err)
				_, err = Driver().Validate(t.Context(), &pm.Request_Validate{ConfigJson: raw})
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	for _, phase := range []struct{ name, fixture string }{
		{"first_load", "fixture_a.jsonl"},
		{"updates_and_deletes", "fixture_b.jsonl"},
	} {
		if !t.Run(phase.name, func(t *testing.T) {
			testutil.RunFlowctl(t, "raw", "preview-next",
				"--source", "testdata/materialize.flow.yaml",
				"--name", "tests/materialize-influxdb",
				"--fixture", filepath.Join("testdata", phase.fixture),
				"--shards", "1",
				"--network", "flow-test",
				"--timeout", "5m",
			)

			points, err := queryPoints(t.Context(), cfg)
			require.NoError(t, err)
			require.Len(t, points, 8)
			// Group by actual timestamps before normalizing publication times, so
			// an update that overwrites an older order still fails the snapshot.
			for i := range points {
				if points[i].Measurement == "orders" {
					points[i].Time = "publication-time"
				}
				metaJSON, ok := points[i].Fields["_meta"].(string)
				require.True(t, ok, "the selected metadata object must be a JSON string")
				var meta map[string]any
				require.NoError(t, json.Unmarshal([]byte(metaJSON), &meta))
				publishedUUID, ok := meta["uuid"].(string)
				require.True(t, ok, "Flow must populate the document UUID")
				_, err := uuid.Parse(publishedUUID)
				require.NoError(t, err)
				meta["uuid"] = "fixture-uuid"
				normalized, err := json.Marshal(meta)
				require.NoError(t, err)
				points[i].Fields["_meta"] = string(normalized)
			}
			snapshotPoints(t, points)
		}) {
			return
		}
	}
}

func runFixtureScript(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "bash", name).CombinedOutput()
	require.NoErrorf(t, err, "%s: %s", name, out)
}

type pointSnapshot struct {
	Measurement string            `json:"measurement"`
	Time        string            `json:"time"`
	Tags        map[string]string `json:"tags"`
	Fields      map[string]any    `json:"fields"`
	Types       map[string]string `json:"types"`
}

func queryPoints(ctx context.Context, cfg config) ([]pointSnapshot, error) {
	body, err := json.Marshal(map[string]any{
		"query": fmt.Sprintf("from(bucket: %q) |> range(start: 0, stop: 2262-04-11T23:47:16.854775806Z)", cfg.Bucket),
		"type":  "flux",
		"dialect": map[string]any{
			"annotations": []string{"datatype"},
			"header":      true,
		},
	})
	if err != nil {
		return nil, err
	}
	c, err := cfg.client(nil)
	if err != nil {
		return nil, err
	}
	defer c.httpClient.CloseIdleConnections()
	req, err := c.newRequest(ctx, http.MethodPost, "query", "application/json", bytes.NewReader(body), url.Values{"org": {cfg.Org}})
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/csv")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("querying InfluxDB: HTTP %d: %s", resp.StatusCode, body)
	}
	return parseQueryPoints(resp.Body)
}

func parseQueryPoints(input io.Reader) ([]pointSnapshot, error) {
	r := csv.NewReader(input)
	r.FieldsPerRecord = -1
	var header, types []string
	points := make(map[string]*pointSnapshot)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if row[0] == "#datatype" {
			types, header = row, nil
			continue
		} else if strings.HasPrefix(row[0], "#") {
			continue
		} else if len(row) > 1 && row[1] == "error" {
			return nil, fmt.Errorf("InfluxDB returned a query error")
		} else if len(row) > 1 && row[1] == "result" {
			header = row
			continue
		}
		if len(header) != len(row) || len(types) != len(row) {
			return nil, fmt.Errorf("query row does not match its CSV header and datatypes")
		}
		values := make(map[string]string)
		datatype := make(map[string]string)
		tags := make(map[string]string)
		for i, name := range header {
			values[name], datatype[name] = row[i], types[i]
			switch name {
			case "", "result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement":
			default:
				if row[i] != "" {
					tags[name] = row[i]
				}
			}
		}
		if values["_measurement"] == "" || values["_field"] == "" {
			return nil, fmt.Errorf("query row has no measurement or field")
		}
		timestamp, err := time.Parse(time.RFC3339Nano, values["_time"])
		if err != nil {
			return nil, err
		}
		stamp := timestamp.UTC().Format(time.RFC3339Nano)
		key, err := json.Marshal([]any{values["_measurement"], stamp, tags})
		if err != nil {
			return nil, err
		}
		point := points[string(key)]
		if point == nil {
			point = &pointSnapshot{
				Measurement: values["_measurement"], Time: stamp, Tags: tags,
				Fields: make(map[string]any), Types: make(map[string]string),
			}
			points[string(key)] = point
		}
		field := values["_field"]
		if _, ok := point.Fields[field]; ok {
			return nil, fmt.Errorf("duplicate field %q for point %s", field, key)
		}
		var value any
		switch datatype["_value"] {
		case "long":
			value, err = strconv.ParseInt(values["_value"], 10, 64)
		case "unsignedLong":
			value, err = strconv.ParseUint(values["_value"], 10, 64)
		case "double":
			value, err = strconv.ParseFloat(values["_value"], 64)
		case "boolean":
			value, err = strconv.ParseBool(values["_value"])
		case "string":
			value = values["_value"]
		default:
			return nil, fmt.Errorf("unknown query datatype %q", datatype["_value"])
		}
		if err != nil {
			return nil, fmt.Errorf("decoding field %q: %w", field, err)
		}
		point.Fields[field], point.Types[field] = value, datatype["_value"]
	}
	out := make([]pointSnapshot, 0, len(points))
	for _, point := range points {
		out = append(out, *point)
	}
	return out, nil
}

func snapshotPoints(t *testing.T, points []pointSnapshot) {
	t.Helper()
	sort.Slice(points, func(i, j int) bool {
		a, err := json.Marshal(points[i])
		require.NoError(t, err)
		b, err := json.Marshal(points[j])
		require.NoError(t, err)
		return string(a) < string(b)
	})
	formatted, err := json.MarshalIndent(points, "", "  ")
	require.NoError(t, err)
	path := filepath.Join(".snapshots", strings.ReplaceAll(t.Name(), "/", "-"))
	if previous, err := os.ReadFile(path); err == nil && string(previous) != string(formatted)+"\n" {
		require.NoError(t, os.WriteFile(path+".actual", append(formatted, '\n'), 0o644))
	}
	cupaloy.SnapshotT(t, string(formatted))
}
