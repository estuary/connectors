package main

import (
	"context"
	"encoding/json"
	"testing"

	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

func TestParseQueueURL(t *testing.T) {
	for _, tc := range []struct {
		url        string
		wantName   string
		wantRegion string
		wantErr    bool
	}{
		{
			url:        "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
			wantName:   "my-queue",
			wantRegion: "us-east-1",
		},
		{
			url:        "https://sqs.eu-central-1.amazonaws.com/123456789012/my-queue.fifo",
			wantName:   "my-queue.fifo",
			wantRegion: "eu-central-1",
		},
		{
			url:        "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/cn-queue",
			wantName:   "cn-queue",
			wantRegion: "cn-north-1",
		},
		{
			// Custom endpoints yield the name but no region.
			url:      "http://localhost:4566/000000000000/test-queue",
			wantName: "test-queue",
		},
		{
			url:      "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/test-queue",
			wantName: "test-queue",
		},
		{url: "not a url", wantErr: true},
		{url: "https://sqs.us-east-1.amazonaws.com/only-one-segment", wantErr: true},
		{url: "https://sqs.us-east-1.amazonaws.com/1234/queue/extra", wantErr: true},
		{url: "", wantErr: true},
	} {
		name, region, err := parseQueueURL(tc.url)
		if tc.wantErr {
			require.Error(t, err, "url %q", tc.url)
			continue
		}
		require.NoError(t, err, "url %q", tc.url)
		require.Equal(t, tc.wantName, name, "url %q", tc.url)
		require.Equal(t, tc.wantRegion, region, "url %q", tc.url)
	}
}

func TestIsFifoQueueName(t *testing.T) {
	require.True(t, isFifoQueueName("orders.fifo"))
	require.False(t, isFifoQueueName("orders"))
	require.False(t, isFifoQueueName("fifo"))
}

func TestConfigValidate(t *testing.T) {
	validCreds := &CredentialsConfig{
		AuthType: AWSAccessKey,
		AccessKeyCredentials: AccessKeyCredentials{
			AWSAccessKeyID:     "id",
			AWSSecretAccessKey: "secret",
		},
	}

	require.NoError(t, (&config{Region: "us-east-1", Credentials: validCreds}).Validate())
	require.ErrorContains(t, (&config{Credentials: validCreds}).Validate(), "region")
	require.ErrorContains(t, (&config{Region: "us-east-1"}).Validate(), "credentials")
	require.ErrorContains(t, (&config{
		Region:      "us-east-1",
		Credentials: &CredentialsConfig{AuthType: AWSAccessKey},
	}).Validate(), "aws_access_key_id")
}

func TestCredentialsConfigUnmarshal(t *testing.T) {
	var creds CredentialsConfig
	require.NoError(t, json.Unmarshal([]byte(`{
		"auth_type": "AWSAccessKey",
		"aws_access_key_id": "id",
		"aws_secret_access_key": "secret"
	}`), &creds))
	require.Equal(t, AWSAccessKey, creds.AuthType)
	require.Equal(t, "id", creds.AWSAccessKeyID)
	require.NoError(t, creds.Validate())

	require.Error(t, json.Unmarshal([]byte(`{"auth_type": "Bogus"}`), &creds))
}

// A region mismatch between the endpoint config and a binding's queue URL is
// caught before any API call is made, so this needs no backend.
func TestValidateRegionMismatch(t *testing.T) {
	configJSON, err := json.Marshal(config{
		Region: "us-west-2",
		Credentials: &CredentialsConfig{
			AuthType: AWSAccessKey,
			AccessKeyCredentials: AccessKeyCredentials{
				AWSAccessKeyID:     "id",
				AWSSecretAccessKey: "secret",
			},
		},
	})
	require.NoError(t, err)
	resourceJSON, err := json.Marshal(resource{
		QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
	})
	require.NoError(t, err)

	_, err = driver{}.Validate(context.Background(), &pc.Request_Validate{
		ConfigJson: configJSON,
		Bindings: []*pc.Request_Validate_Binding{
			{ResourceConfigJson: resourceJSON},
		},
	})
	require.ErrorContains(t, err, `region "us-east-1"`)
	require.ErrorContains(t, err, `region "us-west-2"`)
}
