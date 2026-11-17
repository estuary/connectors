package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	m "github.com/estuary/connectors/go/materialize"
	mbp "github.com/estuary/connectors/materialize-boilerplate"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"gopkg.in/yaml.v3"
)

// requiredPolicyActions are the permissions the connector documentation tells
// users to grant.  Notably this doesn't include dynamodb:Scan, which was added
// as an optional permission later on in the life of the connector.
var requiredPolicyActions = []string{
	"dynamodb:BatchGetItem",
	"dynamodb:BatchWriteItem",
	"dynamodb:CreateTable",
	"dynamodb:DeleteTable",
	"dynamodb:DescribeTable",
}

// loadConfig reads the endpoint config in the YAML file at name.
func loadConfig(name string) (config, error) {
	raw, err := os.ReadFile(name)
	if err != nil {
		return config{}, err
	}

	// config has only JSON tags, so the YAML is converted to JSON first.
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return config{}, fmt.Errorf("parsing %s: %w", name, err)
	}
	asJSON, err := json.Marshal(doc)
	if err != nil {
		return config{}, err
	}

	var cfg config
	if err := json.Unmarshal(asJSON, &cfg); err != nil {
		return config{}, fmt.Errorf("decoding %s: %w", name, err)
	}
	return cfg, nil
}

// accessKeys holds the access keys the connector authenticates with.
type accessKeys struct {
	accessKeyID     string
	secretAccessKey string
}

// configOverrides sets the keys in an endpoint config, by sjson path.
func (k accessKeys) configOverrides() map[string]any {
	return map[string]any{
		"awsAccessKeyId":     k.accessKeyID,
		"awsSecretAccessKey": k.secretAccessKey,
	}
}

// createRestrictedUser uses the admin keys of adminCfg to create an IAM user
// allowed only requiredPolicyActions, and returns its access keys.
func createRestrictedUser(ctx context.Context, adminCfg config) (accessKeys, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(adminCfg.Region),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(adminCfg.AWSAccessKeyID, adminCfg.AWSSecretAccessKey, "")),
	)
	if err != nil {
		return accessKeys{}, fmt.Errorf("loading AWS config: %w", err)
	}
	svc := iam.NewFromConfig(awsCfg, func(o *iam.Options) {
		o.BaseEndpoint = aws.String(adminCfg.Advanced.Endpoint)
	})

	policy, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   requiredPolicyActions,
			"Resource": "*",
		}},
	})
	if err != nil {
		return accessKeys{}, err
	}

	userName := fmt.Sprintf("materialize-dynamodb-%d", time.Now().UnixNano())
	if _, err := svc.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(userName)}); err != nil {
		return accessKeys{}, fmt.Errorf("creating user %s: %w", userName, err)
	}
	if _, err := svc.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:       aws.String(userName),
		PolicyName:     aws.String("documented-permissions"),
		PolicyDocument: aws.String(string(policy)),
	}); err != nil {
		return accessKeys{}, fmt.Errorf("putting policy of user %s: %w", userName, err)
	}
	key, err := svc.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(userName)})
	if err != nil {
		return accessKeys{}, fmt.Errorf("creating access key of user %s: %w", userName, err)
	}

	return accessKeys{
		accessKeyID:     aws.ToString(key.AccessKey.AccessKeyId),
		secretAccessKey: aws.ToString(key.AccessKey.SecretAccessKey),
	}, nil
}

// withKeys bundles the flow.yaml at sourcePath and returns the bundle with
// keys filled into the config of each materialization.
func withKeys(t *testing.T, sourcePath string, keys accessKeys) []byte {
	t.Helper()

	bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", sourcePath)

	for taskName := range gjson.GetBytes(bundled, "materializations").Map() {
		cfgPath := fmt.Sprintf("materializations.%s.endpoint.local.config", taskName)
		rawCfg := []byte(gjson.GetBytes(bundled, cfgPath).Raw)

		var err error
		for path, value := range keys.configOverrides() {
			rawCfg, err = sjson.SetBytes(rawCfg, path, value)
			require.NoError(t, err)
		}
		bundled, err = sjson.SetRawBytes(bundled, cfgPath, rawCfg)
		require.NoError(t, err)
	}

	return bundled
}

// adminSnapshotter reads tables back with admin keys, since the restricted
// user lacks dynamodb:Scan. Every other method runs with the keys of the
// task's config.
type adminSnapshotter struct {
	mbp.Materializer[config, fieldConfig, resource, mappedType]
	admin mbp.Materializer[config, fieldConfig, resource, mappedType]
}

func (a *adminSnapshotter) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	return a.admin.SnapshotTestResource(ctx, path)
}

// newTestMaterializer returns NewMaterializer for the test harness.  This
// allows the tests to run with the connectors credentials, while snapshotting
// with additional permissions.
func newTestMaterializer(adminCfg config) mbp.NewMaterializerFn[config, fieldConfig, resource, mappedType] {
	return func(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (mbp.Materializer[config, fieldConfig, resource, mappedType], error) {
		mat, err := NewMaterializer(ctx, materializationName, cfg, featureFlags)
		if err != nil {
			return nil, err
		}
		admin, err := NewMaterializer(ctx, materializationName, adminCfg, featureFlags)
		if err != nil {
			return nil, err
		}

		return &adminSnapshotter{Materializer: mat, admin: admin}, nil
	}
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	ctx := t.Context()
	adminCfg, err := loadConfig("testdata/config.local.yaml")
	require.NoError(t, err)
	keys, err := createRestrictedUser(ctx, adminCfg)
	require.NoError(t, err)
	newMaterializer := newTestMaterializer(adminCfg)

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, newMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal, ConfigOverrides: keys.configOverrides()})
	})

	t.Run("truncate", func(t *testing.T) {
		boilerplate.SkipRuntimeV1(t)

		client, err := adminCfg.client(ctx)
		require.NoError(t, err)
		adminKeys := accessKeys{accessKeyID: adminCfg.AWSAccessKeyID, secretAccessKey: adminCfg.AWSSecretAccessKey}

		// The fixture stores ids 1-3, then re-stores only id 1 during a
		// backfill. Truncation needs dynamodb:Scan, which
		// requiredPolicyActions lacks, so with those permissions the
		// connector skips it. Otherwise only truncate_standard has
		// flow_published_at selected, so only it loses the ids the backfill
		// didn't re-send.
		for _, tc := range []struct {
			name string
			keys accessKeys
			want map[string][]int64
		}{
			{
				name: "documented permissions",
				keys: keys,
				want: map[string][]int64{
					"truncate_standard":        {1, 2, 3},
					"truncate_no_published_at": {1, 2, 3},
				},
			},
			{
				name: "all permissions",
				keys: adminKeys,
				want: map[string][]int64{
					"truncate_standard":        {1},
					"truncate_no_published_at": {1, 2, 3},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cfg := adminCfg
				cfg.AWSAccessKeyID = tc.keys.accessKeyID
				cfg.AWSSecretAccessKey = tc.keys.secretAccessKey
				task := boilerplate.PrepareTestTask(t, newMaterializer, withKeys(t, "testdata/truncate.flow.yaml", tc.keys),
					"acmeCo/tests/materialize-dynamodb-truncate", cfg, makeResourceFn)

				boilerplate.RunFlowctl(t, "raw", "preview-next",
					"--name", task.Name,
					"--source", task.SourcePath,
					"--fixture", "testdata/truncate.fixture.json",
					"--shards", "1",
					"--timeout", "5m",
					"--network", "flow-test",
				)

				for table, want := range tc.want {
					ids, err := scanIDs(ctx, client, task.Resource(t, table))
					require.NoError(t, err)
					require.Equal(t, want, ids, table)
				}
			})
		}
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterializer, "testdata/apply.flow.yaml", makeResourceFn,
			boilerplate.RuntimeConfig{ConfigOverrides: keys.configOverrides()})
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, newMaterializer, "testdata/migrate.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{ConfigOverrides: keys.configOverrides()})
	})
}

// scanIDs returns the sorted ids stored in table.
func scanIDs(ctx context.Context, client *client, table string) ([]int64, error) {
	out, err := client.db.Scan(ctx, &dynamodb.ScanInput{
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("scanning table %q: %w", table, err)
	}

	var ids []int64
	for _, item := range out.Items {
		n, ok := item["id"].(*types.AttributeValueMemberN)
		if !ok {
			return nil, fmt.Errorf("item missing numeric id: %v", item)
		}
		v, err := strconv.ParseInt(n.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing id %q: %w", n.Value, err)
		}
		ids = append(ids, v)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids, nil
}
