package main

import (
	"context"
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/protocol"
)

func TestSpec(t *testing.T) {
	response, err := driver{}.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestDiscover(t *testing.T) {
	response, err := driver{}.Discover(context.Background(), &pc.Request_Discover{
		ConfigJson: json.RawMessage(`{"rate": 1.0}`),
	})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestCapture(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(`.*`))
	setShutdownAfter(5) // Shut down after 5 messages emitted
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureAlteredMessage(t *testing.T) {
	var ctx, cs = context.Background(), testCaptureSpec(t)
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(`.*`))
	setBindingMessage(cs.Bindings[0], "Hello, altered world!")
	setShutdownAfter(5) // Shut down after 5 messages emitted
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

func testCaptureSpec(t testing.TB) *st.CaptureSpec {
	t.Helper()
	return &st.CaptureSpec{
		Driver:       driver{},
		EndpointSpec: &config{Rate: 1.0},
		Validator:    new(st.OrderedCaptureValidator),
		Sanitizers:   make(map[string]*regexp.Regexp),
	}
}

func discoverBindings(ctx context.Context, t testing.TB, cs *st.CaptureSpec, matchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
	t.Helper()
	var bindings []*pf.CaptureSpec_Binding
	for _, d := range cs.Discover(ctx, t, matchers...) {
		// Unmarshal resource configuration and extract a resource path
		var res resource
		require.NoError(t, json.Unmarshal(d.ResourceConfigJson, &res))
		var resourcePath = []string{res.Name}

		// Generate projections from discovered collection keys.
		var projections []pf.Projection
		for _, keyPtr := range d.Key {
			projections = append(projections, pf.Projection{
				Ptr:          keyPtr,
				Field:        strings.ReplaceAll(strings.TrimPrefix(keyPtr, "/"), "/", "_"),
				IsPrimaryKey: true,
			})
		}
		sort.Slice(projections, func(i, j int) bool {
			return projections[i].Field < projections[j].Field
		})

		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: d.ResourceConfigJson,
			Collection: pf.CollectionSpec{
				Name:           pf.Collection("acmeCo/test-capture/" + d.RecommendedName),
				ReadSchemaJson: d.DocumentSchemaJson,
				Key:            d.Key,
				Projections:    projections,
				// This whole PartitionTemplate is just a dummy to satisfy protocol validation.
				PartitionTemplate: &protocol.JournalSpec{
					Name:        protocol.Journal("acmeCo/test-capture/" + d.RecommendedName + "/pivot=00"),
					Replication: 3,
					Fragment: protocol.JournalSpec_Fragment{
						Length:           512 * 1024 * 1024, // 512 MiB
						CompressionCodec: protocol.CompressionCodec_GZIP,
						RefreshInterval:  30 * time.Second,
					},
				},
			},
			ResourcePath: resourcePath,
			StateKey:     url.QueryEscape(strings.Join(resourcePath, "/")),
		})
	}
	return bindings
}

// setShutdownAfter sets a test flag so that capture runs will shut down after N messages sent.
func setShutdownAfter(numMessages int) {
	TestShutdownAfter = numMessages
}

// setBindingMessage sets the message to be emitted by the binding.
func setBindingMessage(binding *pf.CaptureSpec_Binding, message string) {
	binding.ResourceConfigJson, _ = json.Marshal(resource{
		Name:    binding.ResourcePath[0],
		Message: message,
	})
}
