package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/esapi"
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

	// Tests run as users with only the privileges the connector needs.
	ctx := t.Context()
	required, optional, err := createTestUsers(ctx, "testdata/config.local.yaml")
	require.NoError(t, err)

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact, ConfigOverrides: required.configOverrides()})
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn,
			boilerplate.RuntimeConfig{ConfigOverrides: required.configOverrides()})
	})

	t.Run("truncate", func(t *testing.T) {
		testTruncate(t, "testdata/truncate.flow.yaml", makeResourceFn, []*testUser{required, optional})
	})

	t.Run("cancel truncations", func(t *testing.T) {
		testCancelTruncations(t, "testdata/truncate.flow.yaml", []*testUser{required, optional})
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
		required, optional, err := createTestUsers(ctx, "testdata/config.opensearch.local.yaml")
		require.NoError(t, err)

		t.Run("materialize", func(t *testing.T) {
			boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.opensearch.flow.yaml", makeResourceFn, nil,
				boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact, ConfigOverrides: required.configOverrides()})
		})

		t.Run("apply", func(t *testing.T) {
			boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.opensearch.flow.yaml", makeResourceFn,
				boilerplate.RuntimeConfig{ConfigOverrides: required.configOverrides()})
		})

		t.Run("truncate", func(t *testing.T) {
			testTruncate(t, "testdata/truncate.opensearch.flow.yaml", makeResourceFn, []*testUser{required, optional})
		})

		t.Run("cancel truncations", func(t *testing.T) {
			testCancelTruncations(t, "testdata/truncate.opensearch.flow.yaml", []*testUser{required, optional})
		})

		t.Run("delete by query cancelled", func(t *testing.T) {
			testDeleteByQueryCancelled(t, "testdata/truncate.opensearch.flow.yaml")
		})
	})

	// Elasticsearch does not support migrations since index mappings are
	// immutable, so the migration test is not applicable.
}

// role is a role's definition for each distribution's security API.
type role struct {
	elasticsearch string
	openSearch    string
}

// requiredPrivileges grants the privileges the connector can't run without.
var requiredPrivileges = role{
	elasticsearch: `{
		"cluster": ["monitor"],
		"indices": [{
			"names": ["*"],
			"privileges": ["read", "write", "view_index_metadata", "create_index", "delete_index"]
		}]
	}`,
	openSearch: `{
		"cluster_permissions": ["cluster_monitor"],
		"index_permissions": [{
			"index_patterns": ["*"],
			"allowed_actions": ["read", "write", "create_index", "indices:admin/get", "indices:admin/delete"]
		}]
	}`,
}

// optionalPrivileges grants the required privileges and the optional ones,
// which allow deleting the documents published before a backfill and
// cancelling such deletes from an earlier run.
var optionalPrivileges = role{
	elasticsearch: `{
		"cluster": ["monitor", "cancel_task"],
		"indices": [{
			"names": ["*"],
			"privileges": ["read", "write", "view_index_metadata", "create_index", "delete_index", "maintenance"]
		}]
	}`,
	openSearch: `{
		"cluster_permissions": ["cluster_monitor", "cluster:admin/tasks/cancel"],
		"index_permissions": [{
			"index_patterns": ["*"],
			"allowed_actions": ["read", "write", "create_index", "indices:admin/get", "indices:admin/delete", "indices:admin/refresh*"]
		}]
	}`,
}

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

// createTestUsers uses the admin credentials of the endpoint config at
// cfgPath to create a user with the required privileges and one with the
// optional privileges too. Users that already exist are updated to have
// exactly those privileges.
func createTestUsers(ctx context.Context, cfgPath string) (required, optional *testUser, err error) {
	adminCfg, err := loadConfig(cfgPath)
	if err != nil {
		return required, optional, err
	}
	admin, err := adminCfg.toClient()
	if err != nil {
		return required, optional, err
	}
	info, err := admin.info(ctx)
	if err != nil {
		return required, optional, err
	}
	var openSearch = info.isOpenSearch()

	required, err = createUser(ctx, admin, openSearch, "required privileges", requiredPrivileges, false)
	if err != nil {
		return required, optional, err
	}
	optional, err = createUser(ctx, admin, openSearch, "optional privileges", optionalPrivileges, true)
	if err != nil {
		return required, optional, err
	}
	return required, optional, nil
}

// testUser is a user the connector authenticates as in a test.
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
	return map[string]any{
		"credentials.username": u.username,
		"credentials.password": u.password,
	}
}

// withCredentials returns cfg with the user's credentials.
func (u testUser) withCredentials(cfg config) config {
	cfg.Credentials.Username = u.username
	cfg.Credentials.Password = u.password
	return cfg
}

// createUser uses admin to create or update a user granted r, whose username
// is derived from name, and returns it.
func createUser(ctx context.Context, admin *client, openSearch bool, name string, r role, hasOptional bool) (*testUser, error) {
	var u = &testUser{
		name:        name,
		username:    "flow_" + strings.ReplaceAll(name, " ", "_"),
		password:    "Flow-Test-Password-1",
		hasOptional: hasOptional,
	}

	var err error
	if openSearch {
		err = createOpenSearchUser(ctx, admin, u.username, u.password, r.openSearch)
	} else {
		err = createElasticsearchUser(ctx, admin, u.username, u.password, r.elasticsearch)
	}
	if err != nil {
		return nil, fmt.Errorf("creating user %q: %w", u.username, err)
	}
	return u, nil
}

// createElasticsearchUser uses admin to create or update an Elasticsearch user
// and a role of the same name.
func createElasticsearchUser(ctx context.Context, admin *client, username, password, role string) error {
	res, err := admin.es.Security.PutRole(username, strings.NewReader(role), admin.es.Security.PutRole.WithContext(ctx))
	err = checkResponse(res, err, "creating role")
	if err != nil {
		return err
	}

	user, err := json.Marshal(struct {
		Password string   `json:"password"`
		Roles    []string `json:"roles"`
	}{Password: password, Roles: []string{username}})
	if err != nil {
		return err
	}
	res, err = admin.es.Security.PutUser(username, bytes.NewReader(user), admin.es.Security.PutUser.WithContext(ctx))
	return checkResponse(res, err, "creating user")
}

// checkResponse closes the body of res, and returns err or an error describing
// res if it failed.
func checkResponse(res *esapi.Response, err error, action string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("%s: %s", action, res.String())
	}
	return nil
}

// createOpenSearchUser uses admin to create or update an OpenSearch user and a
// role of the same name mapped to it. OpenSearch's security plugin has its own
// API, which the client doesn't cover.
func createOpenSearchUser(ctx context.Context, admin *client, username, password, role string) error {
	user, err := json.Marshal(struct {
		Password string `json:"password"`
	}{Password: password})
	if err != nil {
		return err
	}
	mapping, err := json.Marshal(struct {
		Users []string `json:"users"`
	}{Users: []string{username}})
	if err != nil {
		return err
	}

	for _, req := range []struct {
		path string
		body []byte
	}{
		{"roles/", []byte(role)},
		{"internalusers/", user},
		{"rolesmapping/", mapping},
	} {
		err := openSearchSecurityRequest(ctx, admin, http.MethodPut, req.path+username, req.body)
		if err != nil {
			return err
		}
	}
	return nil
}

// openSearchSecurityRequest sends a request to the API of OpenSearch's
// security plugin at path, which is relative to _plugins/_security/api/.
func openSearchSecurityRequest(ctx context.Context, admin *client, method, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, method, "/_plugins/_security/api/"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := admin.es.Perform(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("%s %s: reading response: %w", method, path, err)
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, res.Status, respBody)
	}
	return nil
}

// withBundleCredentials returns the bundle with the user's credentials in each
// materialization's config.
func (u testUser) withBundleCredentials(bundled []byte) ([]byte, error) {
	for taskName := range gjson.GetBytes(bundled, "materializations").Map() {
		cfgPath := fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)
		rawCfg := []byte(gjson.GetBytes(bundled, cfgPath).Raw)

		var err error
		for path, value := range u.configOverrides() {
			rawCfg, err = sjson.SetBytes(rawCfg, path, value)
			if err != nil {
				return nil, err
			}
		}
		bundled, err = sjson.SetRawBytes(bundled, cfgPath, rawCfg)
		if err != nil {
			return nil, err
		}
	}

	return bundled, nil
}

// testTruncate drives the truncate-after-backfill fixture through flowYAML
// once as each user. The fixture stores ids 1-3, then re-stores only id 1
// during a backfill. With the optional privileges, the standard-updates index
// loses the documents published before the backfill, except id 99, which has
// no publication time. With only the required privileges, which don't allow
// refreshing the index, nothing is deleted. The delta-updates index always
// keeps everything.
func testTruncate(t *testing.T, flowYAML string, makeResourceFn func(string, bool) resource, users []*testUser) {
	t.Helper()
	if boilerplate.RuntimeV1() {
		t.Skip("backfill signals require runtime-next")
	}
	boilerplate.RunTestAllTasks(t, flowYAML, func(t *testing.T, bundled []byte, taskName string, cfg config) {
		admin, err := cfg.toClient()
		require.NoError(t, err)

		for _, user := range users {
			t.Run(user.name, func(t *testing.T) {
				var ctx = t.Context()
				var want = map[string][]int64{
					"truncate_standard": {1, 2, 3, 99},
					"truncate_delta":    {1, 1, 2, 3, 99},
				}
				if user.hasOptional {
					want["truncate_standard"] = []int64{1, 99}
				}
				userBundled, err := user.withBundleCredentials(bundled)
				require.NoError(t, err)
				var task = boilerplate.PrepareTestTask(t, NewMaterializer, userBundled, taskName, user.withCredentials(cfg), makeResourceFn)

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
func startThrottledDelete(ctx context.Context, c *client, index string, group int, docs int, opaqueID string) (string, error) {
	type doc struct {
		N     int `json:"n"`
		Group int `json:"group"`
	}
	var body bytes.Buffer
	var enc = json.NewEncoder(&body)
	for i := range docs {
		err := enc.Encode(map[string]any{"index": struct{}{}})
		if err != nil {
			return "", err
		}
		err = enc.Encode(doc{N: i, Group: group})
		if err != nil {
			return "", err
		}
	}
	res, err := c.es.Bulk(&body,
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithIndex(index),
		c.es.Bulk.WithRefresh("true"),
	)
	err = checkResponse(res, err, "indexing documents")
	if err != nil {
		return "", err
	}

	query, err := json.Marshal(map[string]any{
		"query": map[string]any{"term": map[string]any{"group": group}},
	})
	if err != nil {
		return "", err
	}
	res, err = c.es.DeleteByQuery(
		[]string{index},
		bytes.NewReader(query),
		c.es.DeleteByQuery.WithContext(ctx),
		c.es.DeleteByQuery.WithWaitForCompletion(false),
		c.es.DeleteByQuery.WithRequestsPerSecond(1),
		c.es.DeleteByQuery.WithScrollSize(1),
		c.es.DeleteByQuery.WithOpaqueID(opaqueID),
	)
	if err != nil {
		return "", fmt.Errorf("starting delete by query: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return "", fmt.Errorf("starting delete by query: %s", res.String())
	}

	var started struct {
		Task string `json:"task"`
	}
	err = json.NewDecoder(res.Body).Decode(&started)
	if err != nil {
		return "", fmt.Errorf("decoding delete by query response: %w", err)
	}
	if started.Task == "" {
		return "", errors.New("delete by query response has no task")
	}
	return started.Task, nil
}

// testCancelTruncations starts throttled deletes of an index tagged as this
// materialization's truncation, as another materialization's truncation, and
// as a customer's own delete, and checks as each user that cancelTruncations
// cancels only the first. Without the optional privilege to cancel tasks, it
// warns and leaves the delete running.
func testCancelTruncations(t *testing.T, flowYAML string, users []*testUser) {
	t.Helper()
	boilerplate.RunTestAllTasks(t, flowYAML, func(t *testing.T, bundled []byte, taskName string, cfg config) {
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
			got, err := admin.listDeleteByQueryTasks(t.Context(), []string{oursPrefix, theirsPrefix, customer})
			require.NoError(t, err)
			return got
		}

		for _, user := range users {
			t.Run(user.name, func(t *testing.T) {
				var ctx = t.Context()
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

				oursTask, err := startThrottledDelete(ctx, admin, index, 1, 20, ours)
				require.NoError(t, err)
				theirsTask, err := startThrottledDelete(ctx, admin, index, 2, 20, theirs)
				require.NoError(t, err)
				customerTask, err := startThrottledDelete(ctx, admin, index, 3, 20, customer)
				require.NoError(t, err)
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
		var ctx = t.Context()
		c, err := cfg.toClient()
		require.NoError(t, err)

		const index = "truncate_cancelled"
		const docs = 10

		c.deleteIndex(ctx, index)
		require.NoError(t, c.createIndex(ctx, index, nil, cfg.Advanced.Replicas, map[string]property{}))
		t.Cleanup(func() { c.deleteIndex(context.Background(), index) })

		taskID, err := startThrottledDelete(ctx, c, index, 1, docs, "")
		require.NoError(t, err)

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
