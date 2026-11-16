package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	response, err := driver{}.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestSpecProtocol(t *testing.T) {
	t.Setenv("FLOW_RUNTIME_CODEC", "json")
	var output bytes.Buffer
	require.NoError(t, boilerplate.InnerMain(context.Background(), &driver{}, strings.NewReader("{\"spec\":{}}\n"), &output))

	var response pc.Response
	require.NoError(t, jsonpb.Unmarshal(&output, &response))
	require.NoError(t, response.Validate())
	require.Equal(t, []string{"/queue"}, response.Spec.ResourcePathPointers)

	var endpoint struct {
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
		Else       struct {
			Required []string `json:"required"`
		} `json:"else"`
		Then struct {
			Required []string `json:"required"`
		} `json:"then"`
	}
	require.NoError(t, json.Unmarshal(response.Spec.ConfigSchemaJson, &endpoint))
	require.Equal(t, []string{"host"}, endpoint.Required)
	require.ElementsMatch(t, []string{"username", "password"}, endpoint.Else.Required)
	require.ElementsMatch(t, []string{"tls", "tlsConfig"}, endpoint.Then.Required)
	require.Len(t, endpoint.Properties, 10)
	require.Equal(t, true, endpoint.Properties["password"]["secret"])
	require.Equal(t, float64(5672), endpoint.Properties["port"]["default"])
	require.Equal(t, float64(15672), endpoint.Properties["managementPort"]["default"])
	require.Equal(t, "/", endpoint.Properties["virtualHost"]["default"])
	require.Equal(t, false, endpoint.Properties["tls"]["default"])
	require.Equal(t, "PLAIN", endpoint.Properties["authMechanism"]["default"])
	require.Equal(t, float64(10), endpoint.Properties["prefetchCount"]["default"])
	require.Equal(t, float64(1), endpoint.Properties["prefetchCount"]["minimum"])
	require.Equal(t, float64(65535), endpoint.Properties["prefetchCount"]["maximum"])
	certificates := endpoint.Properties["tlsConfig"]["properties"].(map[string]any)
	for _, key := range []string{"ssl_server_ca", "ssl_client_cert", "ssl_client_key"} {
		property := certificates[key].(map[string]any)
		require.Equal(t, true, property["secret"])
		require.Equal(t, true, property["multiline"])
	}

	var res struct {
		Required   []string                  `json:"required"`
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(response.Spec.ResourceConfigSchemaJson, &res))
	require.Equal(t, []string{"queue"}, res.Required)
	require.Equal(t, "first", res.Properties["streamStartOffset"]["default"])
}

func TestSpecProtobufWithoutEOF(t *testing.T) {
	t.Setenv("FLOW_RUNTIME_CODEC", "proto")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	input, sendRequest := io.Pipe()
	receiveResponse, output := io.Pipe()
	stop := context.AfterFunc(ctx, func() {
		input.CloseWithError(ctx.Err())
		output.CloseWithError(ctx.Err())
	})
	defer stop()
	t.Cleanup(func() { input.Close(); sendRequest.Close(); receiveResponse.Close(); output.Close() })
	done := make(chan error, 1)
	go func() {
		err := boilerplate.InnerMain(ctx, &driver{}, input, output)
		output.CloseWithError(err)
		done <- err
	}()

	writer := protoio.NewUint32DelimitedWriter(sendRequest, binary.LittleEndian)
	require.NoError(t, writer.WriteMsg(&pc.Request{Spec: &pc.Request_Spec{}}))
	reader := protoio.NewUint32DelimitedReader(receiveResponse, binary.LittleEndian, 1<<20)
	var response pc.Response
	// The runtime waits for a response while keeping stdin open for requests.
	require.NoError(t, reader.ReadMsg(&response))
	require.NoError(t, response.Validate())
	require.Equal(t, []string{"/queue"}, response.Spec.ResourcePathPointers)
	require.NoError(t, sendRequest.Close())
	require.NoError(t, receive(t, done))
}
