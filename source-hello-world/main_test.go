package main

import (
	"context"
	"encoding/json"
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
				ResourcePath:       []string{"hellCollection"},
			},
		},
		Validator:  &st.SortedCaptureValidator{},
		Sanitizers: st.DefaultSanitizers,
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
