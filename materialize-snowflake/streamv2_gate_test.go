package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"sync"
	"testing"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// testJWTPrivateKey is a throwaway key pair in the PKCS#8 PEM form credentials
// carry, generated once for the whole package because the v2 write path is
// refused outright without JWT credentials, and those only validate against a
// key which really parses.
var testJWTPrivateKey = sync.OnceValue(func() string {
	var key, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
})

func testStreamingConfig(t *testing.T, featureFlags string) config {
	t.Helper()

	return testStreamingConfigAuth(t, featureFlags, snowflake_auth.JWT)
}

// testStreamingConfigAuth builds a streaming endpoint configuration authenticating
// the given way, so that the v2 path's requirement of a key pair can be exercised
// from both sides.
func testStreamingConfigAuth(t *testing.T, featureFlags string, authType string) config {
	t.Helper()

	var credentials = &snowflake_auth.CredentialConfig{
		AuthType: authType,
		User:     "will",
	}
	switch authType {
	case snowflake_auth.JWT:
		credentials.PrivateKey = testJWTPrivateKey()
	default:
		credentials.Password = "some+complex/password"
	}

	return config{
		Host:        "orgname-accountname.snowflakecomputing.com",
		Database:    "mydb",
		Schema:      "myschema",
		Credentials: credentials,
		Advanced:    advancedConfig{FeatureFlags: featureFlags},
	}
}

func TestValidateStreamingFlags(t *testing.T) {
	for _, tt := range []struct {
		name         string
		featureFlags string
		wantErr      bool
	}{
		{
			name:         "no flags",
			featureFlags: "",
		},
		{
			name:         "v1 streaming alone",
			featureFlags: "snowpipe_streaming",
		},
		{
			// "snowpipe_streaming" is enabled by default, so opting into v2
			// without naming v1 is the ordinary way to select the v2 write path
			// and must remain valid.
			name:         "v2 streaming alone",
			featureFlags: "snowpipe_streaming_v2",
		},
		{
			name:         "v2 streaming with v1 explicitly disabled",
			featureFlags: "no_snowpipe_streaming,snowpipe_streaming_v2",
		},
		{
			name:         "v1 streaming with v2 explicitly disabled",
			featureFlags: "snowpipe_streaming,no_snowpipe_streaming_v2",
		},
		{
			name:         "both explicitly disabled",
			featureFlags: "no_snowpipe_streaming,no_snowpipe_streaming_v2",
		},
		{
			name:         "unrelated flags",
			featureFlags: "allow_existing_tables_for_new_bindings,snowpipe_streaming_v2",
		},
		{
			name:         "both explicitly enabled",
			featureFlags: "snowpipe_streaming,snowpipe_streaming_v2",
			wantErr:      true,
		},
		{
			name:         "both explicitly enabled in the opposite order",
			featureFlags: "snowpipe_streaming_v2,snowpipe_streaming",
			wantErr:      true,
		},
		{
			name:         "both explicitly enabled among unrelated flags",
			featureFlags: "snowpipe_streaming,allow_existing_tables_for_new_bindings,snowpipe_streaming_v2",
			wantErr:      true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var err = testStreamingConfig(t, tt.featureFlags).Validate()

			if !tt.wantErr {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, "snowpipe_streaming")
			require.ErrorContains(t, err, "snowpipe_streaming_v2")
			// The conflicting-flags refusal must be distinguishable from the
			// runtime-mismatch refusal, which is the only other reason this
			// write path is refused.
			require.NotContains(t, err.Error(), boilerplate.RuntimeV2FlagName)
		})
	}
}

// TestValidateStreamingV2Auth covers the v2 write path's requirement of key-pair
// credentials. Refusing the combination is what keeps the flag from being silently
// ignored: streamsV2 would decline the path for want of a key pair while the
// runtime gate went on holding the task to the v2 runtime for having named the
// flag, leaving it on the staged path the operator asked it to leave.
func TestValidateStreamingV2Auth(t *testing.T) {
	t.Run("the v2 write path with key-pair credentials is allowed", func(t *testing.T) {
		require.NoError(t, testStreamingConfigAuth(t, "snowpipe_streaming_v2", snowflake_auth.JWT).Validate())
	})

	t.Run("the v2 write path without key-pair credentials is refused", func(t *testing.T) {
		var err = testStreamingConfigAuth(t, "snowpipe_streaming_v2", snowflake_auth.UserPass).Validate()
		require.ErrorContains(t, err, flagSnowpipeStreamingV2)
		// The operator's remedies are the authentication method and the flag, so
		// the message must name both.
		require.ErrorContains(t, err, "key-pair")
	})

	t.Run("user-password credentials are allowed without the v2 write path", func(t *testing.T) {
		for _, flags := range []string{"", "snowpipe_streaming", "no_snowpipe_streaming_v2"} {
			require.NoError(t, testStreamingConfigAuth(t, flags, snowflake_auth.UserPass).Validate())
		}
	})

	t.Run("the gate refuses the same configuration", func(t *testing.T) {
		configJson, err := json.Marshal(testStreamingConfigAuth(t, "snowpipe_streaming_v2", snowflake_auth.UserPass))
		require.NoError(t, err)

		// Reported ahead of the runtime mismatch, as the conflicting-flags refusal
		// is: the operator is told about the configuration they wrote rather than
		// the runtime it implies.
		var spec = testStreamingSpec(t, "snowpipe_streaming_v2", false)
		spec.ConfigJson = configJson
		err = requireStreamingV2Runtime(spec)
		require.ErrorContains(t, err, "key-pair")
		require.NotContains(t, err.Error(), boilerplate.RuntimeV2FlagName)
	})

	t.Run("a configuration carrying no credentials at all is refused rather than panicking", func(t *testing.T) {
		var spec = testStreamingSpec(t, "snowpipe_streaming_v2", true)
		spec.ConfigJson = json.RawMessage(`{"host":"h.snowflakecomputing.com","advanced":{"feature_flags":"snowpipe_streaming_v2"}}`)
		require.ErrorContains(t, requireStreamingV2Runtime(spec), "key-pair")
	})
}

func testStreamingSpec(t *testing.T, featureFlags string, runtimeV2 bool) *pf.MaterializationSpec {
	t.Helper()

	configJson, err := json.Marshal(testStreamingConfig(t, featureFlags))
	require.NoError(t, err)

	var shardTemplate = new(pc.ShardSpec)
	if runtimeV2 {
		shardTemplate.LabelSet = pb.MustLabelSet(labels.FlagPrefix+boilerplate.RuntimeV2FlagName, "true")
	}

	return &pf.MaterializationSpec{
		ConfigJson:    configJson,
		ShardTemplate: shardTemplate,
	}
}

func TestRequireStreamingV2Runtime(t *testing.T) {
	t.Run("v2 write path on the v2 runtime is allowed", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "snowpipe_streaming_v2", true)))
	})

	t.Run("v2 write path without the v2 runtime is refused", func(t *testing.T) {
		var err = requireStreamingV2Runtime(testStreamingSpec(t, "snowpipe_streaming_v2", false))
		require.ErrorContains(t, err, "snowpipe_streaming_v2")
		// The operator's remedy is the shard flag, so the message must name it.
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})

	t.Run("v2 write path disabled is allowed off the v2 runtime", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "", false)))
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "no_snowpipe_streaming_v2", false)))
	})

	t.Run("v2 write path disabled is allowed on the v2 runtime", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "", true)))
	})

	t.Run("a nil shard template is refused", func(t *testing.T) {
		var spec = testStreamingSpec(t, "snowpipe_streaming_v2", false)
		spec.ShardTemplate = nil
		require.ErrorContains(t, requireStreamingV2Runtime(spec), boilerplate.RuntimeV2FlagName)
	})

	t.Run("conflicting flags are reported ahead of the runtime mismatch", func(t *testing.T) {
		var err = requireStreamingV2Runtime(testStreamingSpec(t, "snowpipe_streaming,snowpipe_streaming_v2", false))
		require.ErrorContains(t, err, "cannot both be enabled")
		require.NotContains(t, err.Error(), boilerplate.RuntimeV2FlagName)
	})

	t.Run("an unparseable endpoint config is surfaced", func(t *testing.T) {
		var spec = testStreamingSpec(t, "", false)
		spec.ConfigJson = json.RawMessage(`{"host":`)
		require.ErrorContains(t, requireStreamingV2Runtime(spec), "parsing endpoint config")
	})

	t.Run("a missing spec is an error rather than a panic", func(t *testing.T) {
		require.ErrorContains(t, requireStreamingV2Runtime(nil), "no materialization spec")
	})
}

// TestMissingRuntimeV2Warning covers what a publish can say about the runtime
// flag. Publication invokes Validate, which carries no shard template of the spec
// being published, so the last published one is the only evidence there is: it
// recognises a task being moved onto this write path, and cannot tell a publish
// which forgets the runtime flag from one which adds it in the same change.
func TestMissingRuntimeV2Warning(t *testing.T) {
	var configJson = func(t *testing.T, featureFlags string) json.RawMessage {
		t.Helper()
		var out, err = json.Marshal(testStreamingConfig(t, featureFlags))
		require.NoError(t, err)
		return out
	}

	t.Run("moving a task which has not run the v2 runtime onto this path warns", func(t *testing.T) {
		var warning = missingRuntimeV2Warning(
			configJson(t, "snowpipe_streaming_v2"),
			testStreamingSpec(t, "", false),
		)
		// The remedy is the shard flag, so the warning must name it, along with
		// the feature flag whose presence provoked it.
		require.Contains(t, warning, boilerplate.RuntimeV2FlagName)
		require.Contains(t, warning, flagSnowpipeStreamingV2)
	})

	t.Run("a task already on the v2 runtime is silent", func(t *testing.T) {
		require.Empty(t, missingRuntimeV2Warning(
			configJson(t, "snowpipe_streaming_v2"),
			testStreamingSpec(t, "snowpipe_streaming_v2", true),
		))
	})

	t.Run("a publish which does not ask for this write path is silent", func(t *testing.T) {
		require.Empty(t, missingRuntimeV2Warning(configJson(t, ""), testStreamingSpec(t, "", false)))
		require.Empty(t, missingRuntimeV2Warning(
			configJson(t, "no_snowpipe_streaming_v2"),
			testStreamingSpec(t, "", false),
		))
	})

	t.Run("a task with nothing published before it is silent", func(t *testing.T) {
		// A task being created has no last spec to read, and its startup refusal
		// is the clean one: nothing has run under the v2 runtime, so no recovery
		// log guard fires ahead of the connector's own message.
		require.Empty(t, missingRuntimeV2Warning(configJson(t, "snowpipe_streaming_v2"), nil))
	})

	t.Run("an unparseable endpoint config is silent", func(t *testing.T) {
		// Validate reports that failure for itself, with a better message than a
		// warning could carry.
		require.Empty(t, missingRuntimeV2Warning(json.RawMessage(`{"host":`), testStreamingSpec(t, "", false)))
	})
}

// TestGatedDriverRefusals pins the RPCs the gate is wired into. Both refuse
// before the wrapped driver runs, so neither reaches Snowflake.
func TestGatedDriverRefusals(t *testing.T) {
	var ctx = context.Background()
	var driver = NewGatedDriver()
	var spec = testStreamingSpec(t, "snowpipe_streaming_v2", false)

	t.Run("publishing is refused", func(t *testing.T) {
		_, err := driver.Apply(ctx, &pm.Request_Apply{Materialization: spec})
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})

	t.Run("starting is refused", func(t *testing.T) {
		_, _, _, err := driver.NewTransactor(ctx, pm.Request_Open{Materialization: spec}, nil)
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})
}
