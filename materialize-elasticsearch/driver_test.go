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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
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

	// Elasticsearch tests run as a user with only the privileges the
	// connector documentation tells users to grant.
	ctx := t.Context()
	adminCfg, err := loadConfig("testdata/config.local.yaml")
	require.NoError(t, err)
	admin, err := adminCfg.toClient()
	require.NoError(t, err)
	required := createUser(t, ctx, admin, "required privileges", requiredPrivilegesRole)

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact, ConfigOverrides: required.configOverrides()})
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn,
			boilerplate.RuntimeConfig{ConfigOverrides: required.configOverrides()})
	})

	// OpenSearch is an API-compatible fork of Elasticsearch. The same connector
	// supports both, auto-detecting OpenSearch and adapting a few field mapping
	// types (notably 'flat_object' in place of 'flattened'). These run against
	// the opensearch service in the compose file and have their own snapshots,
	// which differ from the Elasticsearch ones in the affected mapping types.
	// OpenSearch runs with its security plugin disabled, so its tests run as
	// the test config's user, which can do anything.
	t.Run("opensearch", func(t *testing.T) {
		t.Run("materialize", func(t *testing.T) {
			boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.opensearch.flow.yaml", makeResourceFn, nil,
				boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
		})

		t.Run("apply", func(t *testing.T) {
			boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.opensearch.flow.yaml", makeResourceFn, boilerplate.RuntimeConfig{})
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

// testUser is a user the connector authenticates as in a test.
type testUser struct {
	username string
	password string
}

// configOverrides sets the user's credentials in an endpoint config, by sjson
// path.
func (u testUser) configOverrides() map[string]any {
	return map[string]any{
		"credentials.username": u.username,
		"credentials.password": u.password,
	}
}

// createUser uses admin to create a user granted role, which is deleted when
// the test ends.
func createUser(t *testing.T, ctx context.Context, admin *client, name string, role string) testUser {
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

	return testUser{username: username, password: password}
}
