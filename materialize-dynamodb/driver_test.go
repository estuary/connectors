package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	m "github.com/estuary/connectors/go/materialize"
	mbp "github.com/estuary/connectors/materialize-boilerplate"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// requiredPolicyActions are the permissions the connector documentation tells
// users to grant.
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

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterializer, "testdata/apply.flow.yaml", makeResourceFn,
			boilerplate.RuntimeConfig{ConfigOverrides: keys.configOverrides()})
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, newMaterializer, "testdata/migrate.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{ConfigOverrides: keys.configOverrides()})
	})
}
