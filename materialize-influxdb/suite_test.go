package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/estuary/connectors/go/common"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	bptest "github.com/estuary/connectors/materialize-boilerplate/testutil"
)

func TestIntegrationShared(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker Compose and flowctl")
	}
	t.Cleanup(func() { runFixtureScript(t, "cleanup.sh") })
	runFixtureScript(t, "setup.sh")

	makeResource := func(name string, delta bool) testResource {
		return testResource{Measurement: name, TimestampField: defaultTimestampField}
	}
	runtime := bptest.RuntimeConfig{
		Shards: 1, Fidelity: m.FidelityTotal,
		ConfigOverrides: map[string]any{"hardDelete": false},
	}
	t.Run("materialize", func(t *testing.T) {
		bptest.RunMaterializationTest(t, newMaterializerUnderTest, "testdata/shared.flow.yaml", makeResource, nil, runtime)
	})
	t.Run("apply", func(t *testing.T) {
		bptest.RunApplyTest(t, Driver(), newMaterializerUnderTest, "testdata/apply.flow.yaml", makeResource, runtime)
	})
}

type testConfig config

func (c testConfig) Validate() error        { return config(c).Validate() }
func (testConfig) DefaultNamespace() string { return "" }
func (testConfig) FeatureFlags() (string, map[string]common.FlagDefault) {
	return "", nil
}

type testResource resource

func (r testResource) WithDefaults(testConfig) testResource {
	if r.TimestampField == "" {
		r.TimestampField = defaultTimestampField
	}
	return r
}

func (r testResource) Validate() error {
	return resource(r.WithDefaults(testConfig{})).Validate()
}

func (r testResource) Parameters() ([]string, bool, error) {
	return []string{r.Measurement}, true, r.Validate()
}

type testFieldConfig struct{}

func (testFieldConfig) Validate() error    { return nil }
func (testFieldConfig) CastToString() bool { return false }

type testMappedType struct{}

func (testMappedType) String() string                            { return "field" }
func (testMappedType) Compatible(boilerplate.ExistingField) bool { return false }
func (testMappedType) CanMigrate(boilerplate.ExistingField) bool { return false }

// The shared suites invoke the production driver through flowctl or RPCs.
// This adapter supplies destination inspection and cleanup, not lifecycle methods.
type materializationUnderTest struct {
	boilerplate.Materializer[testConfig, testFieldConfig, testResource, testMappedType]
	config config
}

func newMaterializerUnderTest(ctx context.Context, name string, cfg testConfig, flags map[string]bool) (boilerplate.Materializer[testConfig, testFieldConfig, testResource, testMappedType], error) {
	return &materializationUnderTest{config: config(cfg)}, nil
}

func (m *materializationUnderTest) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{NoCreateNamespaces: true}
}

func (m *materializationUnderTest) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, paths [][]string) error {
	for _, path := range paths {
		is.PushResource(path...)
	}
	points, err := queryPoints(ctx, m.config)
	if err != nil {
		return err
	}
	for _, point := range points {
		res := is.PushResource(point.Measurement)
		pushField := func(field boilerplate.ExistingField) {
			if res.GetField(field.Name) == nil {
				res.PushField(field)
			}
		}
		pushField(boilerplate.ExistingField{Name: "time", Type: "timestamp"})
		for name := range point.Tags {
			pushField(boilerplate.ExistingField{Name: name, Type: "tag"})
		}
		for name, datatype := range point.Types {
			pushField(boilerplate.ExistingField{Name: name, Type: datatype, Nullable: true})
		}
	}
	return nil
}

func (m *materializationUnderTest) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	points, err := queryPoints(ctx, m.config)
	if err != nil {
		return nil, nil, err
	}
	var payloads []string
	for _, point := range points {
		if point.Measurement != path[0] {
			continue
		}
		// Points were grouped by their actual timestamps before normalization,
		// preserving the number of distinct points written by each transaction.
		point.Measurement = ""
		point.Time = "publication-time"
		raw, err := json.Marshal(point)
		if err != nil {
			return nil, nil, err
		}
		payloads = append(payloads, string(raw))
	}
	sort.Strings(payloads)
	rows := make([][]any, 0, len(payloads))
	for _, payload := range payloads {
		rows = append(rows, []any{json.RawMessage(payload)})
	}
	return []string{"point"}, rows, nil
}

func (m *materializationUnderTest) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("delete measurement %q", path[0]), func(ctx context.Context) error {
		c, err := m.config.client(nil)
		if err != nil {
			return err
		}
		defer c.httpClient.CloseIdleConnections()
		return c.deletePoints(ctx, deleteRequest{
			Start:     "1677-09-21T00:12:43.145224194Z",
			Stop:      "2262-04-11T23:47:16.854775806Z",
			Predicate: "_measurement=" + quoteString(path[0]),
		})
	}, nil
}

func (m *materializationUnderTest) ListTestTasks(context.Context) ([]string, error) { return nil, nil }
func (m *materializationUnderTest) CleanupTestTask(context.Context, string) error   { return nil }
func (m *materializationUnderTest) Close(context.Context)                           {}
