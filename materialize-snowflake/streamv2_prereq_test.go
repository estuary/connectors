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
// selected only with JWT credentials, and those only validate against a key
// which really parses.
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

// TestValidateStreamingFlags pins that no combination of the two streaming flags is
// rejected by configuration validation: naming both selects the v2 write path,
// since that path implies the v1 flag.
func TestValidateStreamingFlags(t *testing.T) {
	for _, tt := range []struct {
		name         string
		featureFlags string
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
		},
		{
			name:         "both explicitly enabled in the opposite order",
			featureFlags: "snowpipe_streaming_v2,snowpipe_streaming",
		},
		{
			name:         "both explicitly enabled among unrelated flags",
			featureFlags: "snowpipe_streaming,allow_existing_tables_for_new_bindings,snowpipe_streaming_v2",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, testStreamingConfig(t, tt.featureFlags).Validate())
		})
	}
}

// TestValidateStreamingV2Auth pins that credentials never fail configuration
// validation on account of the v2 flag: the v2 write path is selected only with
// key-pair credentials, and a configuration naming the flag with any other
// credentials falls through to the write path those credentials support.
func TestValidateStreamingV2Auth(t *testing.T) {
	t.Run("the v2 write path with key-pair credentials is allowed", func(t *testing.T) {
		var cfg = testStreamingConfigAuth(t, "snowpipe_streaming_v2", snowflake_auth.JWT)
		require.NoError(t, cfg.Validate())
		flags, err := boilerplate.ResolveFlags(cfg, &pf.MaterializationSpec{})
		require.NoError(t, err)
		require.True(t, cfg.isStreamsV2(true, flags))
	})

	t.Run("the v2 flag without key-pair credentials is allowed and does not select the path", func(t *testing.T) {
		var cfg = testStreamingConfigAuth(t, "snowpipe_streaming_v2", snowflake_auth.UserPass)
		require.NoError(t, cfg.Validate())
		flags, err := boilerplate.ResolveFlags(cfg, &pf.MaterializationSpec{})
		require.NoError(t, err)
		require.False(t, cfg.isStreamsV2(true, flags))
	})

	t.Run("user-password credentials are allowed without the v2 write path", func(t *testing.T) {
		for _, flags := range []string{"", "snowpipe_streaming", "no_snowpipe_streaming_v2"} {
			require.NoError(t, testStreamingConfigAuth(t, flags, snowflake_auth.UserPass).Validate())
		}
	})

	t.Run("a configuration carrying no credentials at all does not panic", func(t *testing.T) {
		var spec = testStreamingSpec(t, "snowpipe_streaming_v2", true)
		spec.ConfigJson = json.RawMessage(`{"host":"h.snowflakecomputing.com","advanced":{"feature_flags":"snowpipe_streaming_v2"}}`)
		require.NoError(t, requireStreamingV2Runtime(spec, nil))
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
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "snowpipe_streaming_v2", true), nil))
	})

	t.Run("v2 write path without the v2 runtime is rejected", func(t *testing.T) {
		var err = requireStreamingV2Runtime(testStreamingSpec(t, "snowpipe_streaming_v2", false), nil)
		require.ErrorContains(t, err, "snowpipe_streaming_v2")
		// The operator's remedy is the shard flag, so the message must name it.
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})

	t.Run("v2 write path disabled is allowed off the v2 runtime", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "", false), nil))
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "no_snowpipe_streaming_v2", false), nil))
	})

	t.Run("v2 write path disabled is allowed on the v2 runtime", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(testStreamingSpec(t, "", true), nil))
	})

	t.Run("a nil shard template is rejected", func(t *testing.T) {
		var spec = testStreamingSpec(t, "snowpipe_streaming_v2", false)
		spec.ShardTemplate = nil
		require.ErrorContains(t, requireStreamingV2Runtime(spec, nil), boilerplate.RuntimeV2FlagName)
	})

	t.Run("an unparseable endpoint config is surfaced", func(t *testing.T) {
		var spec = testStreamingSpec(t, "", false)
		spec.ConfigJson = json.RawMessage(`{"host":`)
		require.ErrorContains(t, requireStreamingV2Runtime(spec, nil), "parsing endpoint config")
	})

	t.Run("a missing spec is an error rather than a panic", func(t *testing.T) {
		require.ErrorContains(t, requireStreamingV2Runtime(nil, nil), "no materialization spec")
	})
}

func TestRuntimePrereqDriverRejections(t *testing.T) {
	var ctx = context.Background()
	var driver = NewRuntimePrereqDriver()
	var spec = testStreamingSpec(t, "snowpipe_streaming_v2", false)

	t.Run("publishing is rejected", func(t *testing.T) {
		_, err := driver.Apply(ctx, &pm.Request_Apply{Materialization: spec})
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})

	t.Run("starting is rejected", func(t *testing.T) {
		_, _, _, err := driver.NewTransactor(ctx, pm.Request_Open{Materialization: spec}, nil)
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
	})
}

func TestRequireStreamingV2RuntimeWithState(t *testing.T) {
	const stateKey, channel = "sk.v1", "chan-1"

	var specOf = func(t *testing.T, runtimeV2 bool) *pf.MaterializationSpec {
		var spec = testStreamingSpec(t, "snowpipe_streaming", runtimeV2)
		spec.Bindings = []*pf.MaterializationSpec_Binding{{ResourcePath: []string{"TBL"}, StateKey: stateKey}}
		return spec
	}

	var stateWith = func(t *testing.T, sv2Checkpoint streamV2Checkpoint) json.RawMessage {
		t.Helper()
		var out, err = json.Marshal(checkpoint{stateKey: &checkpointItem{StreamV2: sv2Checkpoint}})
		require.NoError(t, err)
		return out
	}

	t.Run("no state carried is allowed", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(specOf(t, false), nil))
	})

	t.Run("the v2 runtime is already running is allowed", func(t *testing.T) {
		var sv2Checkpoint = streamV2Checkpoint{fullKeyRange: {ChannelName: channel, Routed: 3}}
		require.NoError(t, requireStreamingV2Runtime(specOf(t, true), stateWith(t, sv2Checkpoint)))
	})

	t.Run("nil items name nothing to drop", func(t *testing.T) {
		var sv2Checkpoint = streamV2Checkpoint{fullKeyRange: nil}
		require.NoError(t, requireStreamingV2Runtime(specOf(t, false), stateWith(t, sv2Checkpoint)))
	})

	t.Run("an item off the v2 runtime is rejected", func(t *testing.T) {
		var sv2Checkpoint = streamV2Checkpoint{fullKeyRange: {ChannelName: channel, Routed: 3}}
		var err = requireStreamingV2Runtime(specOf(t, false), stateWith(t, sv2Checkpoint))
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
		require.ErrorContains(t, err, channel)
	})

	t.Run("an unreadable state document is tolerated", func(t *testing.T) {
		require.NoError(t, requireStreamingV2Runtime(specOf(t, false), json.RawMessage(`not json`)))
	})

	// The runtime prerequisite check is what puts this ahead of the wrapped driver,
	// so the rejection reaches the operator without Snowflake being touched at all.
	t.Run("publishing an item off the v2 runtime is rejected", func(t *testing.T) {
		var state = stateWith(t, streamV2Checkpoint{fullKeyRange: {ChannelName: channel, Routed: 3}})
		_, err := NewRuntimePrereqDriver().Apply(context.Background(), &pm.Request_Apply{Materialization: specOf(t, false), StateJson: state})
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
		require.ErrorContains(t, err, channel)
	})

	t.Run("opening an item off the v2 runtime is rejected", func(t *testing.T) {
		var state = stateWith(t, streamV2Checkpoint{fullKeyRange: {ChannelName: channel, Routed: 3}})
		_, _, _, err := NewRuntimePrereqDriver().NewTransactor(context.Background(), pm.Request_Open{Materialization: specOf(t, false), StateJson: state}, nil)
		require.ErrorContains(t, err, boilerplate.RuntimeV2FlagName)
		require.ErrorContains(t, err, channel)
	})
}
