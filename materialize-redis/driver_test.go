package connector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	response, err := NewDriver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))

	var endpoint, resource map[string]any
	require.NoError(t, json.Unmarshal(response.ConfigSchemaJson, &endpoint))
	require.NoError(t, json.Unmarshal(response.ResourceConfigSchemaJson, &resource))
	require.Equal(t, []any{"address"}, endpoint["required"])
	require.Equal(t, []any{"keyPrefix"}, resource["required"])

	properties := endpoint["properties"].(map[string]any)
	require.Equal(t, true, properties["password"].(map[string]any)["secret"])
	require.Equal(t, float64(0), properties["database"].(map[string]any)["default"])
	for _, field := range []string{"tls", "hardDelete"} {
		require.Equal(t, false, properties[field].(map[string]any)["default"])
	}
	advanced := properties["advanced"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{"ssl_server_ca", "ssl_client_cert", "ssl_client_key"} {
		require.Equal(t, true, advanced[field].(map[string]any)["secret"])
		require.Equal(t, true, advanced[field].(map[string]any)["multiline"])
	}
	sentinel := properties["sentinel"].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, true, sentinel["password"].(map[string]any)["secret"])
	tunnel := properties["networkTunnel"].(map[string]any)["properties"].(map[string]any)
	ssh := tunnel["sshForwarding"].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, true, ssh["privateKey"].(map[string]any)["secret"])
	properties = resource["properties"].(map[string]any)
	require.Equal(t, true, properties["keyPrefix"].(map[string]any)["x-collection-name"])
	require.Equal(t, true, properties["delta_updates"].(map[string]any)["x-delta-updates"])
	require.Equal(t, "string", properties["format"].(map[string]any)["default"])
	require.Equal(t, "legacy", properties["keyEncoding"].(map[string]any)["default"])

	// RunMain fills the protocol version before sending the response.
	response.Protocol = 3032023
	require.NoError(t, response.Validate())
}

func TestConfig(t *testing.T) {
	for _, tt := range []struct {
		name    string
		input   string
		wantErr string
	}{
		{"defaults", `{"address":"localhost:6379"}`, ""},
		{"IPv6", `{"address":"[::1]:6380"}`, ""},
		{"all options", `{"address":"redis.example:6380","username":"test-user","password":"test-password","database":2,"tls":true,"hardDelete":true}`, ""},
		{"missing address", `{}`, "missing 'address'"},
		{"missing port", `{"address":"localhost"}`, "host:port"},
		{"missing host", `{"address":":6379"}`, "host:port"},
		{"URL", `{"address":"redis://localhost:6379"}`, "host:port"},
		{"port name", `{"address":"localhost:redis"}`, "port must be an integer"},
		{"zero port", `{"address":"localhost:0"}`, "port must be an integer"},
		{"large port", `{"address":"localhost:65536"}`, "port must be an integer"},
		{"negative database", `{"address":"localhost:6379","database":-1}`, "database must be non-negative"},
		{"fractional database", `{"address":"localhost:6379","database":1.5}`, "cannot unmarshal"},
		{"unknown field", `{"address":"localhost:6379","unexpected":true}`, "unknown field"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var cfg config
			err := pf.UnmarshalStrict(json.RawMessage(tt.input), &cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}

	var cfg config
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage(`{"address":"localhost:6379"}`), &cfg))
	require.Zero(t, cfg.Database)
	require.False(t, cfg.TLS)
	require.False(t, cfg.HardDelete)

	client, err := cfg.client()
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	require.Nil(t, client.Options().TLSConfig)
	require.Equal(t, time.Minute, client.Options().ReadTimeout)
	require.Equal(t, time.Minute, client.Options().WriteTimeout)
	require.True(t, client.Options().ContextTimeoutEnabled)

	cfg.TLS, cfg.Database, cfg.Username, cfg.Password = true, 2, "test-user", "test-password"
	secureClient, err := cfg.client()
	require.NoError(t, err)
	t.Cleanup(func() { secureClient.Close() })
	opts := secureClient.Options()
	require.Equal(t, cfg.Address, opts.Addr)
	require.Equal(t, cfg.Database, opts.DB)
	require.Equal(t, cfg.Username, opts.Username)
	require.Equal(t, cfg.Password, opts.Password)
	require.Equal(t, uint16(tls.VersionTLS12), opts.TLSConfig.MinVersion)
	require.False(t, opts.TLSConfig.InsecureSkipVerify)
}

func TestResource(t *testing.T) {
	for _, tt := range []struct {
		name    string
		input   string
		wantErr string
	}{
		{"defaults", `{"keyPrefix":"orders"}`, ""},
		{"TTL", `{"keyPrefix":"events","ttlSeconds":86400}`, ""},
		{"no expiry", `{"keyPrefix":"orders","ttlSeconds":0}`, ""},
		{"delta", `{"keyPrefix":"orders","delta_updates":true}`, ""},
		{"JSON format", `{"keyPrefix":"orders","format":"json"}`, ""},
		{"safe key encoding", `{"keyPrefix":"orders","keyEncoding":"json"}`, ""},
		{"invalid format", `{"keyPrefix":"orders","format":"hash"}`, "format must be"},
		{"invalid key encoding", `{"keyPrefix":"orders","keyEncoding":"unknown"}`, "keyEncoding must be"},
		{"maximum TTL", `{"keyPrefix":"orders","ttlSeconds":9223372036}`, ""},
		{"missing prefix", `{}`, "missing 'keyPrefix'"},
		{"empty prefix", `{"keyPrefix":""}`, "missing 'keyPrefix'"},
		{"negative TTL", `{"keyPrefix":"orders","ttlSeconds":-1}`, "ttlSeconds must be between"},
		{"overflow TTL", `{"keyPrefix":"orders","ttlSeconds":9223372037}`, "ttlSeconds must be between"},
		{"fractional TTL", `{"keyPrefix":"orders","ttlSeconds":0.5}`, "cannot unmarshal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var res resource
			err := pf.UnmarshalStrict(json.RawMessage(tt.input), &res)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProjectionConstraints(t *testing.T) {
	for _, tt := range []struct {
		projection pf.Projection
		want       pm.Response_Validated_Constraint_Type
	}{
		{pf.Projection{Field: "id", Ptr: "/id", IsPrimaryKey: true}, pm.Response_Validated_Constraint_LOCATION_REQUIRED},
		{pf.Projection{Field: "flow_document", Ptr: ""}, pm.Response_Validated_Constraint_LOCATION_REQUIRED},
		{pf.Projection{Field: "status", Ptr: "/status"}, pm.Response_Validated_Constraint_FIELD_FORBIDDEN},
	} {
		t.Run(tt.projection.Field, func(t *testing.T) {
			constraint := projectionConstraint(tt.projection)
			require.Equal(t, tt.want, constraint.Type)
			require.NotEmpty(t, constraint.Reason)
		})
	}
}

func TestApply(t *testing.T) {
	var patterns []string
	d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
		require.Equal(t, "scan", cmd.Name())
		patterns = append(patterns, cmd.Args()[3].(string))
		cmd.(*redis.ScanCmd).SetVal(nil, 0)
		return nil
	}})
	response, err := d.Apply(context.Background(), &pm.Request_Apply{
		Materialization: &pf.MaterializationSpec{
			ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
			Bindings: []*pf.MaterializationSpec_Binding{
				{ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`)},
				{ResourceConfigJson: json.RawMessage(`{"keyPrefix":"events","ttlSeconds":86400}`)},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"orders:*", "events:*"}, patterns)
	require.Equal(t, "Cleared Redis keys for keyPrefix \"orders\"\nCleared Redis keys for keyPrefix \"events\"", response.ActionDescription)
	require.NotNil(t, response.State)
	require.False(t, response.State.MergePatch)
	require.JSONEq(t, `{"name":"","bindings":{"orders":0,"events":0}}`, string(response.State.UpdatedJson))
}

func TestApplyExistingBinding(t *testing.T) {
	for _, tt := range []struct {
		name       string
		lastPrefix string
		backfill   uint32
		delta      bool
		inactive   bool
		wantClear  bool
	}{
		{"unchanged", "orders", 1, false, false, false},
		{"backfill", "orders", 2, false, false, true},
		{"lower backfill", "orders", 0, false, false, false},
		{"new binding", "events", 1, false, false, true},
		{"new delta binding", "events", 1, true, false, false},
		{"delta backfill", "orders", 2, true, false, false},
		{"inactive unchanged", "orders", 1, false, true, false},
		{"inactive backfill", "orders", 2, false, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var unlinked []any
			d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				require.True(t, tt.wantClear, "unchanged and delta bindings must retain their values")
				switch cmd.Name() {
				case "scan":
					require.Equal(t, "orders:*", cmd.Args()[3])
					cmd.(*redis.ScanCmd).SetVal([]string{"orders:1"}, 0)
				case "unlink":
					unlinked = append(unlinked, cmd.Args()[1:]...)
				default:
					return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
				}
				return nil
			}})
			lastSpec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{ResourcePath: []string{tt.lastPrefix}, Backfill: 1}}}
			if tt.inactive {
				lastSpec.Bindings, lastSpec.InactiveBindings = nil, lastSpec.Bindings
			}
			response, err := d.Apply(context.Background(), &pm.Request_Apply{
				LastMaterialization: lastSpec,
				Materialization: &pf.MaterializationSpec{
					ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
					Bindings: []*pf.MaterializationSpec_Binding{{
						ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":"orders","delta_updates":%t,"ttlSeconds":86400}`, tt.delta)),
						ResourcePath:       []string{"orders"},
						DeltaUpdates:       tt.delta,
						Backfill:           tt.backfill,
					}},
				},
			})
			require.NoError(t, err)
			if tt.wantClear {
				require.Equal(t, []any{"orders:1"}, unlinked)
				require.NotEmpty(t, response.ActionDescription)
			} else {
				require.Empty(t, unlinked)
				require.Empty(t, response.ActionDescription)
			}
			if tt.delta {
				require.Nil(t, response.State)
			} else {
				require.NotNil(t, response.State)
				require.False(t, response.State.MergePatch)
				require.JSONEq(t, fmt.Sprintf(`{"name":"","bindings":{"orders":%d}}`, tt.backfill), string(response.State.UpdatedJson))
			}
		})
	}
}

func TestClearPrefixBatches(t *testing.T) {
	prefix := `orders[*?]\`
	var keys []string
	for i := 0; i < 1001; i++ {
		keys = append(keys, fmt.Sprintf("%s:%d", prefix, i))
	}
	var scans int
	var sizes []int
	var unlinked []string
	client := testClient(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "scan":
			require.Equal(t, `orders\[\*\?\]\\:*`, cmd.Args()[3])
			switch scans {
			case 0:
				require.Equal(t, uint64(0), cmd.Args()[1])
				cmd.(*redis.ScanCmd).SetVal(nil, 7)
			case 1:
				require.Equal(t, uint64(7), cmd.Args()[1])
				cmd.(*redis.ScanCmd).SetVal(keys, 9)
			case 2:
				require.Equal(t, uint64(9), cmd.Args()[1])
				cmd.(*redis.ScanCmd).SetVal(keys[:1], 0)
			default:
				t.Fatal("SCAN continued after cursor reached zero")
			}
			scans++
		case "unlink":
			sizes = append(sizes, len(cmd.Args())-1)
			for _, key := range cmd.Args()[1:] {
				unlinked = append(unlinked, key.(string))
			}
		default:
			return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
		}
		return nil
	}})
	require.NoError(t, clearPrefix(context.Background(), client, prefix))
	require.Equal(t, 3, scans)
	require.Equal(t, []int{1000, 1, 1}, sizes)
	require.Equal(t, append(keys, keys[0]), unlinked)
}

func TestApplyErrors(t *testing.T) {
	for _, command := range []string{"scan", "unlink"} {
		t.Run(command, func(t *testing.T) {
			redisErr := errors.New("Redis command failed")
			d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				if cmd.Name() == command {
					return redisErr
				}
				require.Equal(t, "scan", cmd.Name())
				cmd.(*redis.ScanCmd).SetVal([]string{"orders:1"}, 0)
				return nil
			}})
			response, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
				ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
				Bindings:   []*pf.MaterializationSpec_Binding{{ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`)}},
			}})
			require.ErrorIs(t, err, redisErr)
			require.ErrorContains(t, err, `keyPrefix "orders"`)
			require.Nil(t, response)
		})
	}
}

func TestValidateUpdateMode(t *testing.T) {
	for _, tt := range []struct {
		name      string
		lastDelta bool
		delta     bool
		backfill  uint32
		inactive  bool
		wantErr   bool
	}{
		{"standard to standard", false, false, 1, false, false},
		{"standard to delta", false, true, 1, false, false},
		{"delta to delta", true, true, 1, false, false},
		{"delta to standard", true, false, 1, false, true},
		{"delta to standard with backfill", true, false, 2, false, false},
		{"delta to standard with lower backfill", true, false, 0, false, true},
		{"inactive delta to standard", true, false, 1, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				require.Equal(t, []any{"ping"}, cmd.Args())
				cmd.(*redis.StatusCmd).SetVal("PONG")
				return nil
			}})
			last := &pf.MaterializationSpec_Binding{ResourcePath: []string{"orders"}, DeltaUpdates: tt.lastDelta, Backfill: 1}
			lastSpec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{last}}
			if tt.inactive {
				lastSpec.Bindings, lastSpec.InactiveBindings = nil, lastSpec.Bindings
			}
			response, err := d.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson:          json.RawMessage(`{"address":"redis.invalid:6379"}`),
				LastMaterialization: lastSpec,
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":"orders","delta_updates":%t}`, tt.delta)),
					Backfill:           tt.backfill,
					Collection: pf.CollectionSpec{Projections: []pf.Projection{
						{Field: "id", Ptr: "/id", IsPrimaryKey: true},
						{Field: "flow_document", Ptr: ""},
					}},
				}},
			})
			if tt.wantErr {
				require.ErrorContains(t, err, "changing from delta updates to standard updates is not allowed")
			} else {
				require.NoError(t, err)
				require.Len(t, response.Bindings, 1)
				require.Equal(t, tt.delta, response.Bindings[0].DeltaUpdates)
				require.Equal(t, []string{"orders"}, response.Bindings[0].ResourcePath)
			}
		})
	}
}

func TestValidatePrefixes(t *testing.T) {
	for _, encoding := range []string{"legacy", "json"} {
		for _, delta := range []bool{false, true} {
			for _, tt := range []struct {
				name     string
				prefixes []string
				active   []string
				inactive []string
				wantErr  bool
			}{
				{"duplicate", []string{"orders", "orders"}, nil, nil, true},
				{"nested", []string{"orders", "orders:archive"}, nil, nil, true},
				{"nested reverse", []string{"orders:archive", "orders"}, nil, nil, true},
				{"trailing colon", []string{"orders", "orders:"}, nil, nil, true},
				{"nonadjacent overlap", []string{"orders", "orders-archive", "orders:pending"}, nil, nil, true},
				{"shared text", []string{"orders", "orders-archive"}, nil, nil, false},
				{"sibling namespaces", []string{"orders:current", "orders:archive"}, nil, nil, false},
				{"inactive child", []string{"orders"}, nil, []string{"orders:archive"}, true},
				{"inactive parent", []string{"orders:archive"}, nil, []string{"orders"}, true},
				{"unrelated inactive", []string{"orders"}, nil, []string{"events"}, false},
				{"reactivate", []string{"orders"}, nil, []string{"orders"}, false},
				{"disable or remove child and add parent", []string{"orders"}, []string{"orders:archive"}, nil, true},
				{"disable or remove parent and add child", []string{"orders:archive"}, []string{"orders"}, nil, true},
				{"unchanged", []string{"orders", "events"}, []string{"orders", "events"}, nil, false},
				{"exact prefix reuse", []string{"orders"}, []string{"orders"}, nil, false},
				{"duplicate reused prefix", []string{"orders", "orders"}, []string{"orders"}, nil, true},
				{"duplicate reactivated prefix", []string{"orders", "orders"}, nil, []string{"orders"}, true},
				{"unrelated removed", []string{"orders"}, []string{"events"}, nil, false},
			} {
				t.Run(fmt.Sprintf("%s/delta=%t/%s", encoding, delta, tt.name), func(t *testing.T) {
					d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
						require.Equal(t, "ping", cmd.Name())
						cmd.(*redis.StatusCmd).SetVal("PONG")
						return nil
					}})
					req := &pm.Request_Validate{ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`)}
					for _, prefix := range tt.prefixes {
						req.Bindings = append(req.Bindings, &pm.Request_Validate_Binding{
							ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":%q,"keyEncoding":%q,"delta_updates":%t}`, prefix, encoding, delta)),
							Collection:         pf.CollectionSpec{},
						})
					}
					if len(tt.active)+len(tt.inactive) > 0 {
						req.LastMaterialization = &pf.MaterializationSpec{}
						for _, prefix := range tt.active {
							req.LastMaterialization.Bindings = append(req.LastMaterialization.Bindings,
								&pf.MaterializationSpec_Binding{ResourcePath: []string{prefix}})
						}
						for _, prefix := range tt.inactive {
							req.LastMaterialization.InactiveBindings = append(req.LastMaterialization.InactiveBindings,
								&pf.MaterializationSpec_Binding{ResourcePath: []string{prefix}})
						}
					}
					_, err := d.Validate(context.Background(), req)
					if tt.wantErr {
						require.Error(t, err)
						require.ErrorContains(t, err, `"orders"`)
						for _, prefix := range append(append(tt.prefixes, tt.active...), tt.inactive...) {
							if strings.HasPrefix(prefix, "orders:") {
								require.ErrorContains(t, err, fmt.Sprintf("%q", prefix))
							}
						}
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestActivePrefixOverlapDiagnostic(t *testing.T) {
	for _, source := range []string{"new", "previous active", "previous inactive"} {
		for _, prefixes := range [][]string{{"orders", "orders:archive"}, {"orders:archive", "orders"}} {
			t.Run(source+"/"+strings.Join(prefixes, ","), func(t *testing.T) {
				d := testDriver(t, redisHook{process: func(context.Context, redis.Cmder) error {
					t.Fatal("overlapping prefixes must be rejected before connecting to Redis")
					return nil
				}})
				req := &pm.Request_Validate{ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`)}
				for _, prefix := range prefixes {
					req.Bindings = append(req.Bindings, &pm.Request_Validate_Binding{
						ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":%q}`, prefix)),
					})
				}
				last := &pf.MaterializationSpec_Binding{ResourcePath: []string{"orders"}}
				switch source {
				case "previous active":
					req.LastMaterialization = &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{last}}
				case "previous inactive":
					req.LastMaterialization = &pf.MaterializationSpec{InactiveBindings: []*pf.MaterializationSpec_Binding{last}}
				}
				_, err := d.Validate(context.Background(), req)
				require.Error(t, err)
				require.Contains(t, []string{
					`overlapping keyPrefixes "orders" and "orders:archive"`,
					`overlapping keyPrefixes "orders:archive" and "orders"`,
				}, err.Error())

				spec := &pf.MaterializationSpec{ConfigJson: req.ConfigJson}
				for i, b := range req.Bindings {
					spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
						ResourceConfigJson: b.ResourceConfigJson, ResourcePath: []string{prefixes[i]}, Backfill: 1,
					})
				}
				_, err = d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec, LastMaterialization: req.LastMaterialization})
				require.Error(t, err)
				require.Contains(t, []string{
					`overlapping keyPrefixes "orders" and "orders:archive"`,
					`overlapping keyPrefixes "orders:archive" and "orders"`,
				}, err.Error())
			})
		}
	}
}

func TestRetainedBindingPrefixes(t *testing.T) {
	for _, encoding := range []string{"legacy", "json"} {
		for _, prefix := range []string{"orders", "orders:archive:eu"} {
			t.Run(encoding+"/"+prefix, func(t *testing.T) {
				ctx := context.Background()
				res := resource{KeyPrefix: "orders:archive", KeyEncoding: encoding}
				raw, err := json.Marshal(res)
				require.NoError(t, err)
				retained := &pf.MaterializationSpec_Binding{ResourceConfigJson: raw, ResourcePath: []string{res.KeyPrefix}}
				values := make(map[string]string)
				for _, id := range []int64{1, 2} {
					key, err := res.redisKey(tuple.Tuple{id})
					require.NoError(t, err)
					values[key] = fmt.Sprintf(`{"id":%d}`, id)
				}
				want := maps.Clone(values)
				d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
					switch cmd.Name() {
					case "ping":
						cmd.(*redis.StatusCmd).SetVal("PONG")
					case "scan":
						var keys []string
						prefix := strings.TrimSuffix(cmd.Args()[3].(string), "*")
						for key := range values {
							if strings.HasPrefix(key, prefix) {
								keys = append(keys, key)
							}
						}
						cmd.(*redis.ScanCmd).SetVal(keys, 0)
					case "unlink":
						for _, key := range cmd.Args()[1:] {
							delete(values, key.(string))
						}
					default:
						return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
					}
					return nil
				}})
				last := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{retained}}
				req := &pm.Request_Validate{
					ConfigJson:          json.RawMessage(`{"address":"redis.invalid:6379"}`),
					LastMaterialization: last,
					Bindings: []*pm.Request_Validate_Binding{{
						ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":%q,"keyEncoding":%q}`, prefix, encoding)),
						Collection:         pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{"type":"object","reduce":{"strategy":"merge"}}`)},
					}},
				}
				// Validate sees the old active binding before Flow moves it to inactive bindings.
				for _, inactive := range []bool{false, true} {
					t.Run(fmt.Sprintf("inactive=%t", inactive), func(t *testing.T) {
						if inactive {
							last.Bindings, last.InactiveBindings = nil, last.Bindings
						}
						_, err := d.Validate(ctx, req)
						require.ErrorContains(t, err, fmt.Sprintf(`overlapping keyPrefixes %q and %q`, res.KeyPrefix, prefix))
						require.ErrorContains(t, err, "disabled or previously removed bindings")
						require.ErrorContains(t, err, "retained keys")
						require.ErrorContains(t, err, "choose a non-nested keyPrefix")
						require.Equal(t, want, values)
					})
				}
				req.Bindings = []*pm.Request_Validate_Binding{{ResourceConfigJson: raw}}
				_, err = d.Validate(ctx, req)
				require.NoError(t, err)
				response, err := d.Apply(ctx, &pm.Request_Apply{
					LastMaterialization: last,
					Materialization: &pf.MaterializationSpec{
						ConfigJson: req.ConfigJson,
						Bindings:   []*pf.MaterializationSpec_Binding{retained},
					},
				})
				require.NoError(t, err)
				require.Empty(t, response.ActionDescription, "reactivation without a backfill must preserve retained keys")
				require.Equal(t, want, values)
			})
		}
	}
}

func TestApplyRetainedPrefixes(t *testing.T) {
	for _, encoding := range []string{"legacy", "json"} {
		for _, tt := range []struct {
			name     string
			prefix   string
			retained string
		}{
			{"child", "orders", "orders:archive"},
			{"parent", "orders:archive", "orders"},
		} {
			for _, source := range []string{"previous active", "previous inactive", "current inactive"} {
				for _, backfill := range []uint32{0, 1} {
					t.Run(fmt.Sprintf("%s/%s/%s/backfill=%d", encoding, tt.name, source, backfill), func(t *testing.T) {
						retained := &pf.MaterializationSpec_Binding{ResourcePath: []string{tt.retained}}
						last := &pf.MaterializationSpec{}
						spec := &pf.MaterializationSpec{
							ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
							Bindings: []*pf.MaterializationSpec_Binding{
								{ResourceConfigJson: json.RawMessage(`{"keyPrefix":"events"}`), ResourcePath: []string{"events"}, Backfill: 1},
								{
									ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":%q,"keyEncoding":%q}`, tt.prefix, encoding)),
									ResourcePath:       []string{tt.prefix}, Backfill: backfill,
									Collection: pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{"type":"object","reduce":{"strategy":"merge"}}`)},
								},
							},
						}
						if backfill > 0 {
							spec.Bindings[1].Collection.WriteSchemaJson = json.RawMessage(`{"type":"object"}`)
						}
						switch source {
						case "previous active":
							last.Bindings = []*pf.MaterializationSpec_Binding{retained}
						case "previous inactive":
							last.InactiveBindings = []*pf.MaterializationSpec_Binding{retained}
						case "current inactive":
							spec.InactiveBindings = []*pf.MaterializationSpec_Binding{retained}
						}
						values := map[string]string{"events:1": `{"id":1}`}
						for _, id := range []int64{1, 2} {
							key, err := (resource{KeyPrefix: tt.retained, KeyEncoding: encoding}).redisKey(tuple.Tuple{id})
							require.NoError(t, err)
							values[key] = fmt.Sprintf(`{"id":%d}`, id)
						}
						want := maps.Clone(values)
						var commands []string
						d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
							commands = append(commands, cmd.Name())
							switch cmd.Name() {
							case "scan":
								var keys []string
								prefix := strings.TrimSuffix(cmd.Args()[3].(string), "*")
								for key := range values {
									if strings.HasPrefix(key, prefix) {
										keys = append(keys, key)
									}
								}
								cmd.(*redis.ScanCmd).SetVal(keys, 0)
							case "unlink":
								for _, key := range cmd.Args()[1:] {
									delete(values, key.(string))
								}
							default:
								return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
							}
							return nil
						}})
						response, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec, LastMaterialization: last})
						require.ErrorContains(t, err, "overlapping keyPrefixes")
						require.Nil(t, response)
						require.Empty(t, commands, "all prefixes must be checked before clearing any keys")
						require.Equal(t, want, values)
					})
				}
			}
		}
	}
}

func TestApplyDistinctPrefixes(t *testing.T) {
	for _, encoding := range []string{"legacy", "json"} {
		t.Run(encoding, func(t *testing.T) {
			values := make(map[string]string)
			spec := &pf.MaterializationSpec{ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`)}
			validate := &pm.Request_Validate{ConfigJson: spec.ConfigJson}
			for _, prefix := range []string{"orders", "orders-archive"} {
				res := resource{KeyPrefix: prefix, KeyEncoding: encoding}
				raw, err := json.Marshal(res)
				require.NoError(t, err)
				key, err := res.redisKey(tuple.Tuple{"1"})
				require.NoError(t, err)
				values[key] = `{"id":"1"}`
				spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
					ResourceConfigJson: raw, ResourcePath: []string{prefix}, Backfill: 1,
				})
				validate.Bindings = append(validate.Bindings, &pm.Request_Validate_Binding{ResourceConfigJson: raw})
			}
			d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				switch cmd.Name() {
				case "ping":
					cmd.(*redis.StatusCmd).SetVal("PONG")
				case "scan":
					var keys []string
					prefix := strings.TrimSuffix(cmd.Args()[3].(string), "*")
					for key := range values {
						if strings.HasPrefix(key, prefix) {
							keys = append(keys, key)
						}
					}
					cmd.(*redis.ScanCmd).SetVal(keys, 0)
				case "unlink":
					for _, key := range cmd.Args()[1:] {
						delete(values, key.(string))
					}
				default:
					return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
				}
				return nil
			}})
			_, err := d.Validate(context.Background(), validate)
			require.NoError(t, err)
			last := &pf.MaterializationSpec{}
			for _, b := range spec.Bindings {
				previous := *b
				last.Bindings = append(last.Bindings, &previous)
			}
			spec.Bindings[0].Backfill++
			response, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec, LastMaterialization: last})
			require.NoError(t, err)
			require.Equal(t, `Cleared Redis keys for keyPrefix "orders"`, response.ActionDescription)
			key, err := (resource{KeyPrefix: "orders-archive", KeyEncoding: encoding}).redisKey(tuple.Tuple{"1"})
			require.NoError(t, err)
			require.Equal(t, map[string]string{key: `{"id":"1"}`}, values)
		})
	}
}

func TestApplyReplay(t *testing.T) {
	for _, tt := range []struct {
		name     string
		last     *pf.MaterializationSpec_Binding
		backfill uint32
	}{
		{"new materialization", nil, 0},
		{"backfill", &pf.MaterializationSpec_Binding{ResourcePath: []string{"counters"}}, 1},
		{"delta to standard with backfill", &pf.MaterializationSpec_Binding{ResourcePath: []string{"counters"}, DeltaUpdates: true}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			values := map[string]string{
				"counters:1":     `{"id":1,"total":3}`,
				"counters:2":     `{"id":2,"total":5}`,
				"counters:stale": `{"id":99,"total":10}`,
				"other:1":        `{"id":1}`,
			}
			hook := redisHook{
				process: func(ctx context.Context, cmd redis.Cmder) error {
					switch cmd.Name() {
					case "scan":
						require.Equal(t, []any{"scan", uint64(0), "match", "counters:*", "count", int64(1000)}, cmd.Args())
						cmd.(*redis.ScanCmd).SetVal([]string{"counters:1", "counters:2", "counters:stale"}, 0)
					case "unlink":
						for _, key := range cmd.Args()[1:] {
							delete(values, key.(string))
						}
					case "mget":
						require.Equal(t, []any{"mget", "counters:1"}, cmd.Args())
						var value any
						if doc, ok := values["counters:1"]; ok {
							value = doc
						}
						cmd.(*redis.SliceCmd).SetVal([]any{value})
					default:
						return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
					}
					return nil
				},
				pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
					for _, cmd := range cmds {
						require.Equal(t, "set", cmd.Name())
						values[cmd.Args()[1].(string)] = cmd.Args()[2].(string)
					}
					return nil
				},
			}
			req := &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
				ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
				Bindings: []*pf.MaterializationSpec_Binding{{
					ResourceConfigJson: json.RawMessage(`{"keyPrefix":"counters"}`),
					ResourcePath:       []string{"counters"},
					Backfill:           tt.backfill,
					Collection: pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{
						"type":"object","reduce":{"strategy":"merge"},
						"properties":{"id":{"type":"integer"},"total":{"type":"integer","reduce":{"strategy":"sum"}}}
					}`)},
				}},
			}}
			if tt.last != nil {
				req.LastMaterialization = &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{tt.last}}
			}
			_, err := testDriver(t, hook).Apply(ctx, req)
			require.NoError(t, err)

			tx := &transactor{
				client:   testClient(t, hook),
				be:       m.NewBindingEvents(),
				bindings: []binding{{resource: resource{KeyPrefix: "counters"}, path: []string{"counters"}}},
			}
			// Commit the larger key first, so the runtime will load the smaller key next.
			_, err = tx.Store(storeIterator(storeRequest(0, tuple.Tuple{int64(2)}, `{"id":2,"total":5}`)))
			require.NoError(t, err)
			_, err = tx.Acknowledge(ctx, nil, nil)
			require.NoError(t, err)
			var loaded []json.RawMessage
			require.NoError(t, tx.load(ctx, []string{"counters:1"}, []int{0}, func(_ int, doc json.RawMessage) error {
				loaded = append(loaded, doc)
				return nil
			}))
			require.Empty(t, loaded, "replay must not reduce against a value from before Apply")
			_, err = tx.Store(storeIterator(storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1,"total":3}`)))
			require.NoError(t, err)
			require.Equal(t, map[string]string{
				"counters:1": `{"id":1,"total":3}`,
				"counters:2": `{"id":2,"total":5}`,
				"other:1":    `{"id":1}`,
			}, values)
		})
	}
}

func TestApplyResume(t *testing.T) {
	ctx := context.Background()
	values := make(map[string]string)
	hook := redisHook{
		process: func(ctx context.Context, cmd redis.Cmder) error {
			switch cmd.Name() {
			case "ping":
				cmd.(*redis.StatusCmd).SetVal("PONG")
			case "scan":
				var keys []string
				prefix := strings.TrimSuffix(cmd.Args()[3].(string), "*")
				for key := range values {
					if strings.HasPrefix(key, prefix) {
						keys = append(keys, key)
					}
				}
				cmd.(*redis.ScanCmd).SetVal(keys, 0)
			case "unlink":
				for _, key := range cmd.Args()[1:] {
					delete(values, key.(string))
				}
			case "mget":
				docs := make([]any, len(cmd.Args())-1)
				for i, key := range cmd.Args()[1:] {
					if value, ok := values[key.(string)]; ok {
						docs[i] = value
					}
				}
				cmd.(*redis.SliceCmd).SetVal(docs)
			default:
				return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
			}
			return nil
		},
		pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
			for _, cmd := range cmds {
				switch cmd.Name() {
				case "set":
					values[cmd.Args()[1].(string)] = cmd.Args()[2].(string)
				case "del":
					delete(values, cmd.Args()[1].(string))
				default:
					return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
				}
			}
			return nil
		},
	}
	req := &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
		Name:       "acme/redis",
		ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379","hardDelete":true}`),
		Bindings: []*pf.MaterializationSpec_Binding{
			{ResourcePath: []string{"orders"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`)},
			{ResourcePath: []string{"events"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"events"}`)},
		},
	}}
	d := testDriver(t, hook)
	applied, err := d.Apply(ctx, req)
	require.NoError(t, err)
	if applied.State != nil {
		req.StateJson = applied.State.UpdatedJson
	}
	tx := &transactor{
		client:     testClient(t, hook),
		hardDelete: true,
		be:         m.NewBindingEvents(),
		bindings: []binding{
			{resource: resource{KeyPrefix: "orders"}, path: []string{"orders"}},
			{resource: resource{KeyPrefix: "events"}, path: []string{"events"}},
		},
	}
	_, err = tx.Store(storeIterator(
		storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1,"canary":"o1"}`),
		storeRequest(0, tuple.Tuple{int64(2)}, `{"id":2,"canary":"o2"}`),
		storeRequest(0, tuple.Tuple{int64(3)}, `{"id":3,"canary":"o3"}`),
		storeRequest(1, tuple.Tuple{int64(7)}, `{"id":7,"canary":"e7"}`),
	))
	require.NoError(t, err)
	_, err = tx.Acknowledge(ctx, nil, nil)
	require.NoError(t, err)

	// Runtime-next supplies saved Apply state even without the last specification.
	applied, err = d.Apply(ctx, req)
	require.NoError(t, err)
	require.Equal(t, &pm.Response_Applied{}, applied)
	resumed, opened, _, err := d.NewTransactor(ctx, pm.Request_Open{
		Materialization: req.Materialization,
		StateJson:       req.StateJson,
		Range:           &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}, m.NewBindingEvents())
	require.NoError(t, err)
	t.Cleanup(resumed.Destroy)
	require.True(t, opened.DisableLoadOptimization, "resuming with connector state may reset runtime maximum-key tracking")
	var loaded []string
	require.NoError(t, resumed.(*transactor).load(ctx, []string{"orders:2"}, []int{0}, func(_ int, doc json.RawMessage) error {
		loaded = append(loaded, string(doc))
		return nil
	}))
	require.Equal(t, []string{`{"id":2,"canary":"o2"}`}, loaded)
	_, err = resumed.Store(storeIterator(
		storeRequest(0, tuple.Tuple{int64(2)}, `{"id":2,"canary":"o2-v2"}`),
		storeRequest(0, tuple.Tuple{int64(3)}, `{"id":3,"_meta":{"op":"d"}}`),
		storeRequest(0, tuple.Tuple{int64(4)}, `{"id":4,"canary":"o4"}`),
	))
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"orders:1": `{"id":1,"canary":"o1"}`,
		"orders:2": `{"id":2,"canary":"o2-v2"}`,
		"orders:4": `{"id":4,"canary":"o4"}`,
		"events:7": `{"id":7,"canary":"e7"}`,
	}, values)

	// Saved state must not suppress a new backfill or a different materialization.
	req.Materialization.Bindings[0].Backfill++
	applied, err = d.Apply(ctx, req)
	require.NoError(t, err)
	require.NotEmpty(t, applied.ActionDescription)
	require.NotNil(t, applied.State)
	require.Equal(t, map[string]string{"events:7": `{"id":7,"canary":"e7"}`}, values)
	req.StateJson = applied.State.UpdatedJson
	req.LastMaterialization = &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{ResourcePath: []string{"orders"}}}}
	applied, err = d.Apply(ctx, req)
	require.NoError(t, err)
	require.Equal(t, &pm.Response_Applied{}, applied, "Apply must converge even while the last specification still predates the backfill")
	req.LastMaterialization = nil
	req.Materialization.Name = "acme/other"
	_, err = d.Apply(ctx, req)
	require.NoError(t, err)
	require.Empty(t, values)
}

func TestApplyInactiveResume(t *testing.T) {
	for _, name := range []pf.Materialization{"acme/redis", "acme/other"} {
		for _, backfill := range []uint32{1, 2} {
			t.Run(fmt.Sprintf("%s/backfill=%d", name, backfill), func(t *testing.T) {
				values := make(map[string]string)
				d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
					switch cmd.Name() {
					case "scan":
						require.Equal(t, "counters:*", cmd.Args()[3])
						var keys []string
						for key := range values {
							if strings.HasPrefix(key, "counters:") {
								keys = append(keys, key)
							}
						}
						cmd.(*redis.ScanCmd).SetVal(keys, 0)
					case "unlink":
						for _, key := range cmd.Args()[1:] {
							delete(values, key.(string))
						}
					default:
						return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
					}
					return nil
				}})
				binding := &pf.MaterializationSpec_Binding{
					ResourcePath: []string{"counters"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"counters"}`), Backfill: 1,
					Collection: pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{
						"type":"object","reduce":{"strategy":"merge"},
						"properties":{"id":{"type":"string"},"total":{"type":"integer","reduce":{"strategy":"sum"}}}
					}`)},
				}
				req := &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
					Name: "acme/redis", ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
					Bindings: []*pf.MaterializationSpec_Binding{binding},
				}}
				applied, err := d.Apply(context.Background(), req)
				require.NoError(t, err)
				require.NotNil(t, applied.State)
				req.StateJson = applied.State.UpdatedJson
				values["counters:untouched"] = `{"id":"untouched","total":7}`
				values["other:1"] = `{"id":1}`

				// Adding another binding forces replacement state while counters is disabled.
				req.Materialization.Name = name
				binding.Backfill = backfill
				req.Materialization.InactiveBindings = req.Materialization.Bindings
				req.Materialization.Bindings = []*pf.MaterializationSpec_Binding{{
					ResourcePath: []string{"events"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"events"}`),
					Collection: pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{"type":"object"}`)},
				}}
				applied, err = d.Apply(context.Background(), req)
				require.NoError(t, err)
				require.Empty(t, applied.ActionDescription)
				require.NotNil(t, applied.State)
				require.False(t, applied.State.MergePatch)
				req.StateJson = applied.State.UpdatedJson

				req.Materialization.Bindings = append(req.Materialization.Bindings, binding)
				req.Materialization.InactiveBindings = nil
				applied, err = d.Apply(context.Background(), req)
				require.NoError(t, err)
				if name == "acme/redis" && backfill == 1 {
					require.Equal(t, map[string]string{
						"counters:untouched": `{"id":"untouched","total":7}`,
						"other:1":            `{"id":1}`,
					}, values)
					require.Empty(t, applied.ActionDescription)
				} else {
					require.Equal(t, map[string]string{"other:1": `{"id":1}`}, values)
					require.Equal(t, `Cleared Redis keys for keyPrefix "counters"`, applied.ActionDescription)
				}
			})
		}
	}
}

func TestLegacyPreviewInitialState(t *testing.T) {
	testPreviewInitialState(t, false)
}

func TestRuntimeNextPreviewInitialState(t *testing.T) {
	testPreviewInitialState(t, true)
}

func testPreviewInitialState(t *testing.T, runtimeNext bool) {
	t.Helper()
	// Preview behavior must not depend on release notes or other repository files.
	t.Chdir(t.TempDir())

	for _, tt := range []struct {
		name      string
		schema    string
		backfill  uint32
		wantClear bool
	}{
		{"backfill", `{"type":"object"}`, 1, true},
		{"merge", `{"type":"object","reduce":{"strategy":"merge"}}`, 0, true},
		{"reference", `{"$ref":"schema.json"}`, 0, true},
		{"unavailable schema", "", 0, true},
		{"default replacement", `{"type":"object"}`, 0, false},
		{"explicit replacement", `{"type":"object","reduce":{"strategy":"lastWriteWins"}}`, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{"orders:untouched": `{"id":"untouched"}`}
			d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				switch cmd.Name() {
				case "scan":
					require.Equal(t, "orders:*", cmd.Args()[3])
					cmd.(*redis.ScanCmd).SetVal([]string{"orders:untouched"}, 0)
				case "unlink":
					for _, key := range cmd.Args()[1:] {
						delete(values, key.(string))
					}
				case "ping":
					cmd.(*redis.StatusCmd).SetVal("PONG")
				default:
					return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
				}
				return nil
			}})
			spec := &pf.MaterializationSpec{
				Name: "acme/redis", ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
				Bindings: []*pf.MaterializationSpec_Binding{{
					ResourcePath: []string{"orders"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`), Backfill: tt.backfill,
					Collection: pf.CollectionSpec{WriteSchemaJson: json.RawMessage(tt.schema)},
				}},
			}
			state := json.RawMessage(fmt.Sprintf(`{"name":"acme/redis","bindings":{"orders":%d}}`, tt.backfill))
			req := &pm.Request_Apply{Materialization: spec}
			// Runtime-next supplies initial state to Apply; legacy previews supply it only to Open.
			if runtimeNext {
				req.StateJson = state
			}
			applied, err := d.Apply(context.Background(), req)
			require.NoError(t, err)
			if runtimeNext {
				require.Nil(t, applied.State, "matching saved state must converge without the previous specification")
			}
			tx, _, _, err := d.NewTransactor(context.Background(), pm.Request_Open{
				Materialization: spec, StateJson: state,
				Range: &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
			}, m.NewBindingEvents())
			require.NoError(t, err)
			t.Cleanup(tx.Destroy)
			require.NoError(t, tx.UnmarshalState(state))
			if tt.wantClear && !runtimeNext {
				require.Empty(t, values)
				require.Equal(t, `Cleared Redis keys for keyPrefix "orders"`, applied.ActionDescription)
			} else {
				require.Equal(t, map[string]string{"orders:untouched": `{"id":"untouched"}`}, values)
				require.Empty(t, applied.ActionDescription)
			}
		})
	}
}

func TestApplyInvalidState(t *testing.T) {
	d := testDriver(t, redisHook{process: func(context.Context, redis.Cmder) error {
		t.Fatal("invalid state must not clear Redis keys")
		return nil
	}})
	response, err := d.Apply(context.Background(), &pm.Request_Apply{
		StateJson: json.RawMessage(`{"bindings":`),
		Materialization: &pf.MaterializationSpec{
			ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
			Bindings:   []*pf.MaterializationSpec_Binding{{ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`)}},
		},
	})
	require.ErrorContains(t, err, "parsing applied state")
	require.Nil(t, response)
}

func TestApplyReplacementUpdates(t *testing.T) {
	ctx := context.Background()
	values := map[string]string{
		"orders:1": `{"id":1,"canary":"o1"}`,
		"orders:2": `{"id":2,"canary":"o2"}`,
		"orders:3": `{"id":3,"canary":"o3"}`,
		"events:7": `{"id":7,"canary":"e7"}`,
	}
	hook := redisHook{
		process: func(ctx context.Context, cmd redis.Cmder) error {
			switch cmd.Name() {
			case "ping":
				cmd.(*redis.StatusCmd).SetVal("PONG")
			case "scan":
				var keys []string
				prefix := strings.TrimSuffix(cmd.Args()[3].(string), "*")
				for key := range values {
					if strings.HasPrefix(key, prefix) {
						keys = append(keys, key)
					}
				}
				cmd.(*redis.ScanCmd).SetVal(keys, 0)
			case "unlink":
				for _, key := range cmd.Args()[1:] {
					delete(values, key.(string))
				}
			default:
				return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
			}
			return nil
		},
		pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
			for _, cmd := range cmds {
				switch cmd.Name() {
				case "set":
					values[cmd.Args()[1].(string)] = cmd.Args()[2].(string)
				case "del":
					delete(values, cmd.Args()[1].(string))
				default:
					return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
				}
			}
			return nil
		},
	}
	spec := &pf.MaterializationSpec{
		Name:       "acme/redis",
		ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379","hardDelete":true}`),
	}
	for _, prefix := range []string{"orders", "events"} {
		spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
			ResourcePath:       []string{prefix},
			ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":%q}`, prefix)),
			Collection: pf.CollectionSpec{
				WriteSchemaJson: json.RawMessage(`{"type":"object","required":["id"],"properties":{"id":{"type":"integer"},"canary":{"type":"string"}}}`),
			},
		})
	}
	d := testDriver(t, hook)
	// Whole-document replacements can resume without a previous specification or state.
	_, err := d.Apply(ctx, &pm.Request_Apply{Materialization: spec})
	require.NoError(t, err)
	tx, _, _, err := d.NewTransactor(ctx, pm.Request_Open{
		Materialization: spec,
		Range:           &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}, m.NewBindingEvents())
	require.NoError(t, err)
	t.Cleanup(tx.Destroy)
	_, err = tx.Store(storeIterator(
		storeRequest(0, tuple.Tuple{int64(2)}, `{"id":2,"canary":"o2-v2"}`),
		storeRequest(0, tuple.Tuple{int64(3)}, `{"id":3,"_meta":{"op":"d"}}`),
		storeRequest(0, tuple.Tuple{int64(4)}, `{"id":4,"canary":"o4"}`),
	))
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"orders:1": `{"id":1,"canary":"o1"}`,
		"orders:2": `{"id":2,"canary":"o2-v2"}`,
		"orders:4": `{"id":4,"canary":"o4"}`,
		"events:7": `{"id":7,"canary":"e7"}`,
	}, values)

	spec.Bindings[0].Backfill++
	_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: spec})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"events:7": `{"id":7,"canary":"e7"}`}, values)
}

func TestApplyLinkedCollections(t *testing.T) {
	for _, indirect := range []bool{false, true} {
		for _, readSchema := range []bool{false, true} {
			t.Run(fmt.Sprintf("indirect=%t/readSchema=%t", indirect, readSchema), func(t *testing.T) {
				collection := pf.CollectionSpec{WriteSchemaJson: json.RawMessage(`{"type":"object","reduce":{"strategy":"lastWriteWins"}}`)}
				if readSchema {
					collection.ReadSchemaJson = collection.WriteSchemaJson
					collection.WriteSchemaJson = json.RawMessage(`{"type":"object","reduce":{"strategy":"merge"}}`)
				}
				spec := &pf.MaterializationSpec{
					ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
					Bindings: []*pf.MaterializationSpec_Binding{{
						ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`),
						ResourcePath:       []string{"orders"},
						Collection:         collection,
					}},
				}
				if indirect {
					spec.LinkedCollections = []pf.CollectionSpec{
						{WriteSchemaJson: json.RawMessage(`{"type":"object","reduce":{"strategy":"merge"}}`)},
						collection,
					}
					spec.Bindings[0].CollectionIndex = 1
					spec.Bindings[0].Collection = pf.CollectionSpec{}
				}
				require.NoError(t, pf.ValidateBindingCollection(len(spec.LinkedCollections), spec.Bindings[0].Collection.ProtoSize(), spec.Bindings[0].CollectionIndex))
				values := map[string]string{"orders:untouched": `{"id":"untouched"}`}
				var unlinked []any
				d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
					switch cmd.Name() {
					case "scan":
						require.Equal(t, "orders:*", cmd.Args()[3])
						cmd.(*redis.ScanCmd).SetVal([]string{"orders:untouched"}, 0)
					case "unlink":
						unlinked = append(unlinked, cmd.Args()[1:]...)
						for _, key := range cmd.Args()[1:] {
							delete(values, key.(string))
						}
					default:
						return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
					}
					return nil
				}})
				response, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: spec})
				require.NoError(t, err)
				require.Empty(t, unlinked)
				require.Empty(t, response.ActionDescription)
				require.Equal(t, map[string]string{"orders:untouched": `{"id":"untouched"}`}, values)
			})
		}
	}
}

func TestApplyReductionSchemas(t *testing.T) {
	for _, tt := range []struct {
		name        string
		writeSchema string
		readSchema  string
		wantClear   bool
	}{
		{"default replacement", `{"type":"object"}`, "", false},
		{"explicit replacement", `{"type":"object","reduce":{"strategy":"lastWriteWins"}}`, "", false},
		{"reduce property", `{"type":"object","properties":{"reduce":{"type":"number"}}}`, "", false},
		{"instance data", `{"type":"object","default":{"reduce":1,"$ref":"data"},"examples":[{"reduce":"x"}],"const":{"reduce":2},"enum":[{"reduce":3}]}`, "", false},
		{"schema map names", `{"properties":{"$ref":{"type":"string"}},"patternProperties":{"reduce":{"type":"number"}},"$defs":{"reduce":{"type":"number"}},"definitions":{"reduce":{"type":"number"}},"dependentSchemas":{"reduce":{"type":"object"}}}`, "", false},
		{"property dependencies", `{"dependencies":{"reduce":["$ref","other"]},"dependentRequired":{"reduce":["$ref","other"]}}`, "", false},
		{"conditional deletion", `{"type":"object","if":{"required":["deleted"]},"then":{"reduce":{"strategy":"lastWriteWins","delete":true}}}`, "", false},
		{"merge", `{"type":"object","reduce":{"strategy":"merge"}}`, "", true},
		{"sum", `{"type":"object","reduce":{"strategy":"merge"},"properties":{"total":{"reduce":{"strategy":"sum"}}}}`, "", true},
		{"append", `{"type":"object","reduce":{"strategy":"merge"},"properties":{"items":{"reduce":{"strategy":"append"}}}}`, "", true},
		{"first write wins", `{"type":"object","reduce":{"strategy":"firstWriteWins"}}`, "", true},
		{"conditional merge", `{"anyOf":[{"type":"object"},{"type":"object","reduce":{"strategy":"merge"}}]}`, "", true},
		{"reference", `{"$ref":"schema.json"}`, "", true},
		{"dynamic reference", `{"$dynamicRef":"schema.json#node"}`, "", true},
		{"recursive reference", `{"$recursiveRef":"#"}`, "", true},
		{"missing schema", "", "", true},
		{"read schema merge", `{"type":"object"}`, `{"type":"object","reduce":{"strategy":"merge"}}`, true},
		{"read schema replacement", `{"type":"object","reduce":{"strategy":"merge"}}`, `{"type":"object"}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, backfill := range []uint32{0, 1} {
				t.Run(fmt.Sprintf("backfill=%d", backfill), func(t *testing.T) {
					var unlinked []any
					d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
						switch cmd.Name() {
						case "scan":
							require.Equal(t, "orders:*", cmd.Args()[3])
							cmd.(*redis.ScanCmd).SetVal([]string{"orders:1"}, 0)
						case "unlink":
							unlinked = append(unlinked, cmd.Args()[1:]...)
						default:
							return fmt.Errorf("unexpected Redis command: %v", cmd.Args())
						}
						return nil
					}})
					response, err := d.Apply(context.Background(), &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
						ConfigJson: json.RawMessage(`{"address":"redis.invalid:6379"}`),
						Bindings: []*pf.MaterializationSpec_Binding{{
							ResourcePath:       []string{"orders"},
							ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`),
							Backfill:           backfill,
							Collection: pf.CollectionSpec{
								WriteSchemaJson: json.RawMessage(tt.writeSchema),
								ReadSchemaJson:  json.RawMessage(tt.readSchema),
							},
						}},
					}})
					require.NoError(t, err)
					if tt.wantClear || backfill > 0 {
						require.Equal(t, []any{"orders:1"}, unlinked)
						require.NotEmpty(t, response.ActionDescription)
					} else {
						require.Empty(t, unlinked)
						require.Empty(t, response.ActionDescription)
					}
				})
			}
		})
	}
}

func TestRetainPrefixSubschemas(t *testing.T) {
	for _, tt := range []struct {
		keywords []string
		value    string
	}{
		{[]string{"properties", "patternProperties", "$defs", "definitions", "dependentSchemas", "dependencies"}, `{"reduce":%s}`},
		{[]string{"items", "additionalItems", "additionalProperties", "not", "if", "then", "else", "contains", "propertyNames", "unevaluatedItems", "unevaluatedProperties", "contentSchema"}, `%s`},
		{[]string{"items", "prefixItems", "allOf", "anyOf", "oneOf"}, `[%s]`},
	} {
		for _, keyword := range tt.keywords {
			t.Run(keyword+"/"+tt.value, func(t *testing.T) {
				for _, schema := range []string{`{"reduce":{"strategy":"sum"}}`, `{"$ref":"schema.json"}`} {
					raw := json.RawMessage(fmt.Sprintf(`{%q:%s}`, keyword, fmt.Sprintf(tt.value, schema)))
					require.False(t, canRetainPrefix(raw), string(raw))
				}
			})
		}
	}
}

func testDriver(t *testing.T, hook redisHook) driver {
	t.Helper()
	return driver{newClient: func(config) (*redis.Client, error) { return testClient(t, hook), nil }}
}
