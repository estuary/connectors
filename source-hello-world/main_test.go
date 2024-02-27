package main

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestCapture(t *testing.T) {
	// Always run the test since it doesn't rely on any external system.
	t.Setenv("TEST_DATABASE", "yes")

	resource := resource{
		Name:   "helloCollection",
		Prefix: "Hello {}",
	}
	resourceBytes, err := json.Marshal(resource)
	require.NoError(t, err)

	capture := &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Rate: 10},
		Bindings: []*flow.CaptureSpec_Binding{
			{
				ResourceConfigJson: resourceBytes,
				ResourcePath:       []string{"helloCollection"},
				Collection: flow.CollectionSpec{
					Name: "helloCollection",
				},
				StateKey: "helloStateKey",
			},
		},
		Validator:  &st.SortedCaptureValidator{},
		Sanitizers: testSanitizers,
	}

	captureCtx, cancelCapture := context.WithCancel(context.Background())
	count := 0
	capture.Capture(captureCtx, t, func(msg json.RawMessage) {
		count++
		if count > 10 {
			cancelCapture()
		}
	})

	cupaloy.SnapshotT(t, capture.Summary())
	capture.Reset()
}

var testSanitizers = map[string]*regexp.Regexp{
	`"<TIMESTAMP>"`: regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`),
}
