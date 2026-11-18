package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

func (c config) client(options *azcosmos.ClientOptions) (*azcosmos.Client, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	credential, err := azcosmos.NewKeyCredential(c.Key)
	if err != nil {
		return nil, fmt.Errorf("creating account key credential: %w", err)
	}

	var opts azcosmos.ClientOptions
	if options != nil {
		opts = *options
	}
	if opts.Retry.MaxRetries == 0 {
		// Allow 20 attempts during RU throttling. The SDK honors x-ms-retry-after-ms
		// and context cancellation, with a maximum retry delay of 60 seconds.
		opts.Retry.MaxRetries = 19
	}
	client, err := azcosmos.NewClientWithKey(c.Endpoint, credential, &opts)
	if err != nil {
		return nil, fmt.Errorf("creating Cosmos DB client: %w", err)
	}
	return client, nil
}

// accountProperties reads the account's id and default consistency level. The
// SDK reads the same account properties to discover regions but does not expose them.
func (c config) accountProperties(ctx context.Context, options *azcosmos.ClientOptions) (account, error) {
	key, err := base64.StdEncoding.DecodeString(c.Key)
	if err != nil {
		return account{}, fmt.Errorf("decoding account key: %w", err)
	}
	var opts azcore.ClientOptions
	if options != nil {
		opts = options.ClientOptions
	}
	pipeline := runtime.NewPipeline("materialize-cosmosdb-nosql", "", runtime.PipelineOptions{PerRetry: []policy.Policy{accountKeyPolicy(key)}}, &opts)
	req, err := runtime.NewRequest(ctx, http.MethodGet, c.Endpoint)
	if err != nil {
		return account{}, err
	}
	req.Raw().Header.Set("x-ms-version", "2020-11-05")
	resp, err := pipeline.Do(req)
	if err != nil {
		return account{}, err
	} else if !runtime.HasStatusCode(resp, http.StatusOK) {
		return account{}, runtime.NewResponseError(resp)
	}
	var props struct {
		ID                    string `json:"id"`
		UserConsistencyPolicy struct {
			DefaultConsistencyLevel azcosmos.ConsistencyLevel `json:"defaultConsistencyLevel"`
		} `json:"userConsistencyPolicy"`
	}
	if err := runtime.UnmarshalAsJSON(resp, &props); err != nil {
		return account{}, fmt.Errorf("decoding account properties: %w", err)
	}
	return account{id: props.ID, consistency: props.UserConsistencyPolicy.DefaultConsistencyLevel}, nil
}

type account struct {
	id          string
	consistency azcosmos.ConsistencyLevel
}

// isEmulator reports whether the account is the single-node Cosmos DB
// emulator, which reads its own writes but always reports Eventual
// consistency. Azure account endpoints are under a documents.* domain.
func (a account) isEmulator(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && a.id == "cosmosdev" && !strings.Contains(strings.ToLower(u.Hostname()), ".documents.")
}

// accountKeyPolicy signs a read of the account root, which has an empty
// resource type and resource link, as the SDK's key credential does.
type accountKeyPolicy []byte

func (key accountKeyPolicy) Do(req *policy.Request) (*http.Response, error) {
	date := time.Now().UTC().Format(http.TimeFormat)
	req.Raw().Header.Set("x-ms-date", date)
	req.Raw().Header.Set("Authorization", key.sign(date))
	return req.Next()
}

func (key accountKeyPolicy) sign(date string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("get\n\n\n" + strings.ToLower(date) + "\n\n"))
	return url.QueryEscape("type=master&ver=1.0&sig=" + base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}

func isStatus(err error, status int) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == status
}

func isItemNotFound(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusNotFound &&
		(responseErr.RawResponse == nil || responseErr.RawResponse.Header.Get("x-ms-substatus") != "1002")
}
