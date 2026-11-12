package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/go/retry"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"gopkg.in/yaml.v3"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(index string, delta bool) resource {
		return resource{Index: index, DeltaUpdates: delta}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	// Elasticsearch tests run as users with only the privileges the
	// connector documentation tells users to grant.
	ctx := t.Context()
	adminCfg, err := loadConfig("testdata/config.local.yaml")
	require.NoError(t, err)
	admin, err := adminCfg.toClient()
	require.NoError(t, err)
	required := createUser(t, ctx, admin, "required privileges", requiredPrivilegesRole, false)
	optional := createUser(t, ctx, admin, "optional privileges", optionalPrivilegesRole, true)

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact, ConfigOverrides: required.configOverrides()})
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn,
			boilerplate.RuntimeConfig{ConfigOverrides: required.configOverrides()})
	})

	t.Run("truncate", func(t *testing.T) {
		testTruncate(t, "testdata/truncate.flow.yaml", makeResourceFn, []testUser{required, optional})
	})

	t.Run("cancel truncations", func(t *testing.T) {
		testCancelTruncations(t, "testdata/truncate.flow.yaml", []testUser{required, optional})
	})

	t.Run("delete by query cancelled", func(t *testing.T) {
		testDeleteByQueryCancelled(t, "testdata/truncate.flow.yaml")
	})

	// OpenSearch is an API-compatible fork of Elasticsearch. The same connector
	// supports both, auto-detecting OpenSearch and adapting a few field mapping
	// types (notably 'flat_object' in place of 'flattened'). These run against
	// the opensearch service in the compose file and have their own snapshots,
	// which differ from the Elasticsearch ones in the affected mapping types.
	t.Run("opensearch", func(t *testing.T) {
		// OpenSearch runs with its security plugin disabled, so its tests
		// run as the test config's user, which can do anything.
		unrestricted := testUser{name: "no security", hasOptional: true}

		t.Run("materialize", func(t *testing.T) {
			boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.opensearch.flow.yaml", makeResourceFn, nil,
				boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
		})

		t.Run("apply", func(t *testing.T) {
			boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.opensearch.flow.yaml", makeResourceFn, boilerplate.RuntimeConfig{})
		})

		t.Run("truncate", func(t *testing.T) {
			testTruncate(t, "testdata/truncate.opensearch.flow.yaml", makeResourceFn, []testUser{unrestricted})
		})

		t.Run("cancel truncations", func(t *testing.T) {
			testCancelTruncations(t, "testdata/truncate.opensearch.flow.yaml", []testUser{unrestricted})
		})

		t.Run("delete by query cancelled", func(t *testing.T) {
			testDeleteByQueryCancelled(t, "testdata/truncate.opensearch.flow.yaml")
		})
	})

	// Elasticsearch does not support migrations since index mappings are
	// immutable, so the migration test is not applicable.
}

// requiredPrivilegesRole grants the privileges the connector documentation
// tells users to grant.
const requiredPrivilegesRole = `{
	"cluster": ["monitor"],
	"indices": [{
		"names": ["*"],
		"privileges": ["read", "write", "view_index_metadata", "create_index", "delete_index"]
	}]
}`

// optionalPrivilegesRole grants the required privileges and the optional ones
// the connector documentation describes: the maintenance index privilege and
// the cancel_task cluster privilege.
const optionalPrivilegesRole = `{
	"cluster": ["monitor", "cancel_task"],
	"indices": [{
		"names": ["*"],
		"privileges": ["read", "write", "view_index_metadata", "create_index", "delete_index", "maintenance"]
	}]
}`

// loadConfig reads the endpoint config in the YAML file at name.
func loadConfig(name string) (config, error) {
	raw, err := os.ReadFile(name)
	if err != nil {
		return config{}, err
	}

	// config has only JSON tags, so the YAML is converted to JSON first.
	var doc map[string]any
	err = yaml.Unmarshal(raw, &doc)
	if err != nil {
		return config{}, fmt.Errorf("parsing %s: %w", name, err)
	}
	asJSON, err := json.Marshal(doc)
	if err != nil {
		return config{}, err
	}

	var cfg config
	err = json.Unmarshal(asJSON, &cfg)
	if err != nil {
		return config{}, fmt.Errorf("decoding %s: %w", name, err)
	}
	return cfg, nil
}

// testUser is a user the connector authenticates as in a test. A user with no
// username is the test config's own user.
type testUser struct {
	// name names the subtests run as the user.
	name     string
	username string
	password string
	// hasOptional is whether the user has the optional privileges.
	hasOptional bool
}

// configOverrides sets the user's credentials in an endpoint config, by sjson
// path.
func (u testUser) configOverrides() map[string]any {
	if u.username == "" {
		return nil
	}
	return map[string]any{
		"credentials.username": u.username,
		"credentials.password": u.password,
	}
}

// withCredentials returns cfg with the user's credentials.
func (u testUser) withCredentials(cfg config) config {
	if u.username != "" {
		cfg.Credentials.Username = u.username
		cfg.Credentials.Password = u.password
	}
	return cfg
}

// createUser uses admin to create a user granted role, which is deleted when
// the test ends.
func createUser(t *testing.T, ctx context.Context, admin *client, name string, role string, hasOptional bool) testUser {
	t.Helper()
	var username = "flow_" + strings.ReplaceAll(name, " ", "_")
	const password = "flow_test_password"

	res, err := admin.es.Security.PutRole(username, strings.NewReader(role), admin.es.Security.PutRole.WithContext(ctx))
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	res.Body.Close()

	res, err = admin.es.Security.PutUser(username, strings.NewReader(`{"password":"`+password+`","roles":["`+username+`"]}`), admin.es.Security.PutUser.WithContext(ctx))
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	res.Body.Close()

	t.Cleanup(func() {
		if res, err := admin.es.Security.DeleteUser(username); err == nil {
			res.Body.Close()
		}
		if res, err := admin.es.Security.DeleteRole(username); err == nil {
			res.Body.Close()
		}
	})

	return testUser{name: name, username: username, password: password, hasOptional: hasOptional}
}

// withBundleCredentials returns the bundle with the user's credentials in each
// materialization's config.
func (u testUser) withBundleCredentials(t *testing.T, bundled []byte) []byte {
	t.Helper()

	for taskName := range gjson.GetBytes(bundled, "materializations").Map() {
		cfgPath := fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)
		rawCfg := []byte(gjson.GetBytes(bundled, cfgPath).Raw)

		var err error
		for path, value := range u.configOverrides() {
			rawCfg, err = sjson.SetBytes(rawCfg, path, value)
			require.NoError(t, err)
		}
		bundled, err = sjson.SetRawBytes(bundled, cfgPath, rawCfg)
		require.NoError(t, err)
	}

	return bundled
}

// testTruncate drives the truncate-after-backfill fixture through flowYAML
// once as each user. The fixture stores ids 1-3, then re-stores only id 1
// during a backfill. With the optional privileges, the standard-updates index
// loses the documents published before the backfill, except id 99, which has
// no publication time. With only the required privileges, which lack
// maintenance, nothing is deleted. The delta-updates index always keeps
// everything.
func testTruncate(t *testing.T, flowYAML string, makeResourceFn func(string, bool) resource, users []testUser) {
	t.Helper()
	if boilerplate.RuntimeV1() {
		t.Skip("backfill signals require runtime-next")
	}
	boilerplate.RunTestAllTasks(t, flowYAML, func(t *testing.T, bundled []byte, taskName string, cfg config) {
		var ctx = context.Background()
		admin, err := cfg.toClient()
		require.NoError(t, err)

		for _, user := range users {
			t.Run(user.name, func(t *testing.T) {
				var want = map[string][]int64{
					"truncate_standard": {1, 2, 3, 99},
					"truncate_delta":    {1, 1, 2, 3, 99},
				}
				if user.hasOptional {
					want["truncate_standard"] = []int64{1, 99}
				}
				var task = boilerplate.PrepareTestTask(t, NewMaterializer, user.withBundleCredentials(t, bundled), taskName, user.withCredentials(cfg), makeResourceFn)

				// Each index starts with a document lacking a publication
				// time. The index is created first with the test config's
				// replica count, since auto-creation would use the cluster
				// default of one replica, which a single node can't
				// allocate, and the connector's stores wait for every copy
				// of a shard.
				for _, name := range []string{"truncate_standard", "truncate_delta"} {
					var index = task.Resource(t, name)
					require.NoError(t, admin.createIndex(ctx, index, nil, cfg.Advanced.Replicas, map[string]property{}))

					res, err := admin.es.Index(
						index,
						strings.NewReader(`{"id":99}`),
						admin.es.Index.WithContext(ctx),
						admin.es.Index.WithRefresh("true"),
					)
					require.NoError(t, err)
					require.False(t, res.IsError(), res.String())
					res.Body.Close()
				}

				boilerplate.RunFlowctl(t, "raw", "preview-next",
					"--name", task.Name,
					"--source", task.SourcePath,
					"--fixture", "testdata/truncate.fixture.json",
					"--shards", "1",
					"--timeout", "5m",
				)

				for name, want := range want {
					var index = task.Resource(t, name)

					// The connector's stores don't refresh the index.
					refreshRes, err := admin.es.Indices.Refresh(
						admin.es.Indices.Refresh.WithContext(ctx),
						admin.es.Indices.Refresh.WithIndex(index),
					)
					require.NoError(t, err)
					require.False(t, refreshRes.IsError(), refreshRes.String())
					refreshRes.Body.Close()

					res, err := admin.es.Search(
						admin.es.Search.WithContext(ctx),
						admin.es.Search.WithIndex(index),
						admin.es.Search.WithSort("id:asc"),
						admin.es.Search.WithSize(100),
					)
					require.NoError(t, err)
					require.False(t, res.IsError(), res.String())

					var body struct {
						Hits struct {
							Hits []struct {
								Source struct {
									ID int64 `json:"id"`
								} `json:"_source"`
							} `json:"hits"`
						} `json:"hits"`
					}
					require.NoError(t, json.NewDecoder(res.Body).Decode(&body))
					res.Body.Close()

					var ids []int64
					for _, h := range body.Hits.Hits {
						ids = append(ids, h.Source.ID)
					}
					require.Equal(t, want, ids, name)
				}
			})
		}
	})
}

// startThrottledDelete indexes docs documents into index under group, and
// starts a delete by query of that group tagged with opaqueID, throttled to one
// document per second so that it keeps running, and returns the task's ID.
// Deletes of different groups don't conflict over documents.
func startThrottledDelete(t *testing.T, ctx context.Context, c *client, index string, group int, docs int, opaqueID string) string {
	t.Helper()

	var body strings.Builder
	for i := range docs {
		fmt.Fprintf(&body, "{\"index\":{}}\n{\"n\":%d,\"group\":%d}\n", i, group)
	}
	res, err := c.es.Bulk(strings.NewReader(body.String()),
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithIndex(index),
		c.es.Bulk.WithRefresh("true"),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	res.Body.Close()

	res, err = c.es.DeleteByQuery(
		[]string{index},
		strings.NewReader(fmt.Sprintf(`{"query":{"term":{"group":%d}}}`, group)),
		c.es.DeleteByQuery.WithContext(ctx),
		c.es.DeleteByQuery.WithWaitForCompletion(false),
		c.es.DeleteByQuery.WithRequestsPerSecond(1),
		c.es.DeleteByQuery.WithScrollSize(1),
		c.es.DeleteByQuery.WithOpaqueID(opaqueID),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	var started struct {
		Task string `json:"task"`
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&started))
	res.Body.Close()
	require.NotEmpty(t, started.Task)
	return started.Task
}

// testCancelTruncations starts throttled deletes of an index tagged as this
// materialization's truncation, as another materialization's truncation, and
// as a customer's own delete, and checks as each user that cancelTruncations
// cancels only the first. Without the optional cancel_task privilege, it warns
// and leaves the delete running.
func testCancelTruncations(t *testing.T, flowYAML string, users []testUser) {
	t.Helper()
	boilerplate.RunTestAllTasks(t, flowYAML, func(t *testing.T, bundled []byte, taskName string, cfg config) {
		var ctx = context.Background()
		admin, err := cfg.toClient()
		require.NoError(t, err)

		const index = "truncate_cancel"
		var oursPrefix = truncationTagPrefix(taskName, index)
		var theirsPrefix = truncationTagPrefix(taskName+"-other", index)
		var ours = oursPrefix + "earlier-run"
		var theirs = theirsPrefix + "earlier-run"
		const customer = "customer-delete"

		running := func(t *testing.T) map[string]string {
			t.Helper()
			got, err := admin.listDeleteByQueryTasks(ctx, []string{oursPrefix, theirsPrefix, customer})
			require.NoError(t, err)
			return got
		}

		for _, user := range users {
			t.Run(user.name, func(t *testing.T) {
				admin.deleteIndex(ctx, index)
				require.NoError(t, admin.createIndex(ctx, index, nil, cfg.Advanced.Replicas, map[string]property{}))
				t.Cleanup(func() {
					if res, err := admin.es.Tasks.Cancel(
						admin.es.Tasks.Cancel.WithActions(deleteByQueryAction),
						admin.es.Tasks.Cancel.WithWaitForCompletion(true),
					); err == nil {
						res.Body.Close()
					}
					admin.deleteIndex(context.Background(), index)
				})

				oursTask := startThrottledDelete(t, ctx, admin, index, 1, 20, ours)
				theirsTask := startThrottledDelete(t, ctx, admin, index, 2, 20, theirs)
				customerTask := startThrottledDelete(t, ctx, admin, index, 3, 20, customer)
				require.Equal(t, map[string]string{oursTask: ours, theirsTask: theirs, customerTask: customer}, running(t))

				c, err := user.withCredentials(cfg).toClient()
				require.NoError(t, err)

				// With no binding events, warnings are logged.
				hook := logtest.NewGlobal()
				defer hook.Reset()
				tr := &transactor{client: c, materializationName: taskName, bindings: []binding{{index: index}}}
				require.NoError(t, tr.cancelTruncations(ctx))

				if user.hasOptional {
					require.Empty(t, truncationWarnings(hook))
					require.Equal(t, map[string]string{theirsTask: theirs, customerTask: customer}, running(t))
				} else {
					warnings := truncationWarnings(hook)
					require.Len(t, warnings, 1)
					require.Contains(t, warnings[0], "cancel_task")
					require.Contains(t, running(t), oursTask)
				}
			})
		}
	})
}

// truncationWarnings returns the messages of warnings logged about
// truncations.
func truncationWarnings(hook *logtest.Hook) []string {
	var out []string
	for _, e := range hook.AllEntries() {
		if e.Level == log.WarnLevel && strings.Contains(e.Message, "truncation of index") {
			out = append(out, e.Message)
		}
	}
	return out
}

// testDeleteByQueryCancelled cancels a running background delete by query, and
// checks that waiting on it fails rather than reporting a completed delete.
func testDeleteByQueryCancelled(t *testing.T, flowYAML string) {
	t.Helper()
	boilerplate.RunTestAllTasks(t, flowYAML, func(t *testing.T, bundled []byte, taskName string, cfg config) {
		var ctx = context.Background()
		c, err := cfg.toClient()
		require.NoError(t, err)

		const index = "truncate_cancelled"
		const docs = 10

		c.deleteIndex(ctx, index)
		require.NoError(t, c.createIndex(ctx, index, nil, cfg.Advanced.Replicas, map[string]property{}))
		t.Cleanup(func() { c.deleteIndex(context.Background(), index) })

		taskID := startThrottledDelete(t, ctx, c, index, 1, docs, "")

		res, err := c.es.Tasks.Cancel(
			c.es.Tasks.Cancel.WithContext(ctx),
			c.es.Tasks.Cancel.WithTaskID(taskID),
			c.es.Tasks.Cancel.WithWaitForCompletion(true),
		)
		require.NoError(t, err)
		require.False(t, res.IsError(), res.String())
		res.Body.Close()

		// Elasticsearch stores the outcome of a cancelled task, either as a
		// response reporting the cancellation or as an error, depending on
		// what the task was doing when it was cancelled. OpenSearch doesn't
		// store it, so the task is not found. Any of these fails without a
		// retry.
		result, err := c.checkDeleteByQuery(ctx, taskID)
		if err != nil {
			require.Equal(t, retry.Fatal, classifyTaskPoll(err), err.Error())
		} else {
			require.ErrorContains(t, result.err(), "was cancelled")
			require.Less(t, result.Deleted, int64(docs))
		}
	})
}
