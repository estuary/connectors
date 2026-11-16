package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func TestIntegration(t *testing.T) {
	if testing.Short() || (os.Getenv("TEST_DATABASE") != "yes" && os.Getenv("CONNECTOR") != "materialize-redis") {
		t.Skip("set TEST_DATABASE=yes or run CONNECTOR=materialize-redis VERSION=local ../tests/materialize/run.sh")
	}

	t.Cleanup(func() {
		out, err := exec.Command("bash", "../tests/materialize-redis/cleanup.sh").CombinedOutput()
		require.NoError(t, err, string(out))
	})
	out, err := exec.Command("bash", "../tests/materialize-redis/setup.sh").CombinedOutput()
	require.NoError(t, err, string(out))

	ctx := context.Background()
	client, err := (config{Address: "localhost:6380"}).client()
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	require.NoError(t, client.Ping(ctx).Err())

	bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", "testdata/materialize.flow.yaml")
	for _, hardDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("hardDelete=%t", hardDelete), func(t *testing.T) {
			require.NoError(t, client.FlushDB(ctx).Err())
			catalog, err := sjson.SetBytes(bundled, "materializations.acmeCo/tests/materialize-redis.endpoint.local.config.hardDelete", hardDelete)
			require.NoError(t, err)
			source := filepath.Join(t.TempDir(), "test.flow.json")
			require.NoError(t, os.WriteFile(source, catalog, 0o600))

			for _, phase := range []string{"initial", "incremental", "updated"} {
				if !t.Run(phase, func(t *testing.T) {
					var args []string
					if boilerplate.RuntimeV1() {
						// A single session must leave data even without a recovery drain.
						args = []string{"preview", "--sessions=-1"}
					} else {
						args = []string{"raw", "preview-next", "--shards", "1"}
					}
					args = append(args,
						"--name", "acmeCo/tests/materialize-redis",
						"--source", source,
						"--fixture", "testdata/fixture."+phase+".json",
						"--timeout", "2m",
						"--network", "flow-test",
					)
					boilerplate.RunFlowctl(t, args...)
					snapshotRedis(t, client)
				}) {
					return
				}
			}
		})
	}
}

func snapshotRedis(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()
	var keys []string
	iter := client.Scan(ctx, 0, "*", 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	require.NoError(t, iter.Err())
	slices.Sort(keys)
	require.NotEmpty(t, keys)

	var rows []string
	for _, key := range keys {
		raw, err := client.Get(ctx, key).Result()
		require.NoError(t, err)
		var compact bytes.Buffer
		require.NoError(t, json.Compact(&compact, []byte(raw)))
		require.Equal(t, compact.String(), raw, "stored JSON must be compact")

		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &doc))
		meta, ok := doc["_meta"].(map[string]any)
		require.True(t, ok, "missing document metadata for %s", key)
		id, ok := meta["uuid"].(string)
		require.True(t, ok, "missing document UUID for %s", key)
		_, err = uuid.Parse(id)
		require.NoError(t, err)
		meta["uuid"] = "UUID"

		// Check expiration before normalizing its countdown for the snapshot.
		ttl, err := client.TTL(ctx, key).Result()
		require.NoError(t, err)
		expiration := "none"
		if strings.HasPrefix(key, "events:") {
			require.GreaterOrEqual(t, ttl, 24*time.Hour-5*time.Minute)
			require.LessOrEqual(t, ttl, 24*time.Hour)
			expiration = "expires"
		} else {
			require.Equal(t, time.Duration(-1), ttl, "unexpected expiration for %s", key)
		}

		row, err := json.Marshal(struct {
			Key   string         `json:"key"`
			TTL   string         `json:"ttl"`
			Value map[string]any `json:"value"`
		}{key, expiration, doc})
		require.NoError(t, err)
		rows = append(rows, string(row))
	}

	got := strings.Join(rows, "\n")
	path := filepath.Join(".snapshots", strings.ReplaceAll(t.Name(), "/", "-"))
	if previous, err := os.ReadFile(path); err == nil && string(previous) != got+"\n" {
		require.NoError(t, os.WriteFile(path+".actual", []byte(got+"\n"), 0o644))
	}
	cupaloy.SnapshotT(t, got)
}
