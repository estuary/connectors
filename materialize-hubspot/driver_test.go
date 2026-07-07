package hubspot

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"iter"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var integration = flag.Bool("interactive", false, "run interactive tests")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

type TestAccount struct {
	AccountName string `json:"accountName"`
	ID          int    `json:"id"`
	Status      string `json:"status"`
	TrialEndsAt string `json:"trialEndsAt"`
}

type AccessToken struct {
	Token           Secret `json:"oauthAccessToken"`
	ExpiresAtMillis int    `json:"expiresAtMillis"`
}

type TestAccountConfig struct {
	AccountName    string `json:"accountName"`
	Description    string `json:"description"`
	MarketingLevel string `json:"marketingLevel"`
	OpsLevel       string `json:"opsLevel"`
	ServiceLevel   string `json:"serviceLevel"`
	SalesLevel     string `json:"salesLevel"`
	ContentLevel   string `json:"contentLevel"`
	CommerceLevel  string `json:"commerceLevel"`
}

func NewTestAccountConfig(name string) TestAccountConfig {
	return TestAccountConfig{
		AccountName:    name,
		Description:    name,
		MarketingLevel: "ENTERPRISE",
		OpsLevel:       "ENTERPRISE",
		ServiceLevel:   "ENTERPRISE",
		SalesLevel:     "ENTERPRISE",
		ContentLevel:   "ENTERPRISE",
		CommerceLevel:  "ENTERPRISE",
	}
}

// Portal is the information for accessing the parent account.
type Portal struct {
	ID                string `json:"portal_id"`
	PersonalAccessKey Secret `json:"personal_access_key"`

	accessToken *AccessToken
}

func NewPortal(filename string) (*Portal, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	rawJSON, err := decryptYAMLConfig(data)
	if err != nil {
		return nil, err
	}

	var portal Portal
	err = json.Unmarshal(rawJSON, &portal)
	if err != nil {
		return nil, err
	}

	return &portal, nil
}

func (p *Portal) RefreshToken(ctx context.Context, client *LocalDevClient) error {
	accessToken, err := client.RefreshToken(ctx, p.PersonalAccessKey)
	if err != nil {
		return err
	}
	p.accessToken = accessToken
	return nil
}

func (p *Portal) Token() *Secret {
	if p.accessToken == nil {
		return nil
	}
	return &p.accessToken.Token
}

// decryptYAMLConfig decrypts yaml bytes with sops and returns it.
func decryptYAMLConfig(data []byte) (json.RawMessage, error) {
	sopsCmd := exec.Command("sops", "--decrypt", "--input-type=yaml", "--output-type=json", "/dev/stdin")
	sopsCmd.Stdin = bytes.NewReader(data)

	// Remove "_sops" encrypted suffix.
	jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	var err error
	jqCmd.Stdin, err = sopsCmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := sopsCmd.Start(); err != nil {
		return nil, err
	}
	stdoutStderr, err := jqCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", stdoutStderr, err)
	}
	if err := sopsCmd.Wait(); err != nil {
		return nil, err
	}

	var config json.RawMessage
	err = json.Unmarshal(stdoutStderr, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func encryptConfig(data []byte) ([]byte, error) {
	sopsCmd := exec.Command("sops", "--encrypt", "--input-type=json", "--output-type=json", "/dev/stdin")
	sopsCmd.Stdin = bytes.NewReader(data)

	stdoutStderr, err := sopsCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("error running sops: %w", err)
	}

	return stdoutStderr, nil
}

// LocalDevClient uses undocumented APIs to create and remove test-accounts.
//
// Normally you would use the javascript hubspot/cli to do this, but that's a
// lot to run in our test environment for a couple http requests.
type LocalDevClient struct {
	httpClient *http.Client
}

func NewLocalDevClient() *LocalDevClient {
	return &LocalDevClient{
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// CreateTestAccount creates a new HubSpot test account.
//
// A test account is a temporary nested account that can have customized
// subscription tiers.
//
// You must use a PAK to create a test account.
//
// https://developers.hubspot.com/docs/developer-tooling/local-development/configurable-test-accounts
func (c *LocalDevClient) CreateTestAccount(ctx context.Context, portal *Portal, config TestAccountConfig) (*TestAccount, error) {
	uri := baseURL.JoinPath("/integrators/test-portals/v3")
	params := url.Values{}
	params.Set("portalId", portal.ID)
	uri.RawQuery = params.Encode()

	data, err := json.Marshal(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal config: %w", err)
	}
	var testAccount TestAccount
	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := http.NewRequestWithContext(ctx, "POST", uri.String(), bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+portal.Token().Expose())

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		if resp.ContentLength > int64(MaxResponseBytes) {
			return fmt.Errorf("response body too large: %d", resp.ContentLength)
		}

		return parseBody(resp, &testAccount, MaxResponseBytes)
	})
	if err != nil {
		return nil, err
	}

	// Poll test-account status until we receive a sucess response
	for range 10 {
		status, err := c.AccountStatus(ctx, portal, &testAccount)
		if err != nil {
			return nil, err
		}

		if status == "SUCCESS" {
			break
		}

		time.Sleep(2 * time.Second)
	}

	// The HubSpot CLI always waits an additional 5 seconds since the account
	// may still not be ready.
	//
	// https://github.com/HubSpot/hubspot-cli/blob/6061d30ccf8ce57a0a04d6852e1efa05381747bb/lib/buildAccount.ts#L130-L132
	time.Sleep(5 * time.Second)

	return &testAccount, err
}

func (c *LocalDevClient) RefreshToken(ctx context.Context, personalAccessKey Secret) (*AccessToken, error) {
	uri := baseURL.JoinPath("/localdevauth/v1/auth/refresh")

	request := struct {
		EncodedOAuthRefreshToken string `json:"encodedOAuthRefreshToken"`
	}{
		EncodedOAuthRefreshToken: personalAccessKey.Expose(),
	}
	data, err := json.Marshal(&request)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal refresh request: %w", err)
	}

	var response AccessToken
	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := http.NewRequestWithContext(ctx, "POST", uri.String(), bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		if resp.ContentLength > int64(MaxResponseBytes) {
			return fmt.Errorf("response body too large: %d", resp.ContentLength)
		}

		return parseBody(resp, &response, MaxResponseBytes)
	})
	if err != nil {
		return nil, err
	}

	return &response, err
}

func (c *LocalDevClient) AccountStatus(ctx context.Context, portal *Portal, testAccount *TestAccount) (string, error) {
	uri := baseURL.JoinPath(fmt.Sprintf("/integrators/test-portals/v3/gate-sync-status/%d", testAccount.ID))
	params := url.Values{}
	params.Set("portalId", portal.ID)
	uri.RawQuery = params.Encode()

	var response struct {
		Status string `json:"status"`
	}
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := http.NewRequestWithContext(ctx, "GET", uri.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+portal.Token().Expose())

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		if resp.ContentLength > int64(MaxResponseBytes) {
			return fmt.Errorf("response body too large: %d", resp.ContentLength)
		}

		return parseBody(resp, &response, MaxResponseBytes)
	})
	if err != nil {
		return "", err
	}

	return response.Status, nil
}

func (c *LocalDevClient) CreatePersonalAccessKey(ctx context.Context, portal *Portal, testAccount *TestAccount) (Secret, error) {
	uri := baseURL.JoinPath(fmt.Sprintf("/integrators/test-portals/v3/generate-pak/%d", testAccount.ID))
	params := url.Values{}
	params.Set("portalId", portal.ID)
	uri.RawQuery = params.Encode()

	var response struct {
		PersonalAccessKey Secret `json:"personalAccessKey"`
	}
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := http.NewRequestWithContext(ctx, "GET", uri.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+portal.Token().Expose())

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		if resp.ContentLength > int64(MaxResponseBytes) {
			return fmt.Errorf("response body too large: %d", resp.ContentLength)
		}

		return parseBody(resp, &response, MaxResponseBytes)
	})
	if err != nil {
		return "", err
	}

	return response.PersonalAccessKey, nil
}

// DeleteTestAccount deletes a test-account.
//
// If deleting the account fails it can be manually removed in the webapp from
// the main account at:
//
//	Development -> Testing -> Test Accounts
func (c *LocalDevClient) DeleteTestAccount(ctx context.Context, portal *Portal, account *TestAccount) error {
	uri := baseURL.JoinPath(fmt.Sprintf("/integrators/test-portals/v3/%d", account.ID))
	params := url.Values{}
	params.Set("portalId", portal.ID)
	uri.RawQuery = params.Encode()

	return Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := http.NewRequestWithContext(ctx, "DELETE", uri.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+portal.Token().Expose())

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 204 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}
		return nil
	})
}

func (c *LocalDevClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// taskNames returns the ordered list of materialization task names from a bundled spec.
func taskNames(bundled []byte) []string {
	var names []string
	gjson.GetBytes(bundled, "materializations").ForEach(func(task, _ gjson.Result) bool {
		names = append(names, task.String())
		return true
	})
	return names
}

type Task struct {
	Name     string
	Bundle   []byte
	Source   string
	Config   *Config
	Bindings []TestBinding
}

type TestBinding struct {
	Resource   Resource
	Collection string
	Properties []string
	Key        string
}

// getTokens starts the callback server, displays the authorization URL, and
// waits for it to be called.  This is an interactive step.
func getTokens(ctx context.Context, creds Credentials) (*TokenUpdate, error) {
	redirectBaseURL := &url.URL{
		Scheme: "http",
		Host:   "localhost:3000",
		Path:   "oauth",
	}

	client, err := NewClientDefaultLimiter(creds)
	if err != nil {
		return nil, err
	}
	server, err := client.StartOAuth2CallbackServer(ctx, redirectBaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to start server: %w", err)
	}

	params := url.Values{}
	params.Set("client_id", creds.ClientID)
	params.Set("scope", strings.Join(Oauth2Scopes, " "))
	params.Set("redirect_uri", redirectBaseURL.String())
	uri := *AuthURL
	uri.RawQuery = params.Encode()

	fmt.Printf("Authorization URL: %s\n", uri.String())

	tokens, err := server.WaitTokens()
	if err != nil {
		return nil, fmt.Errorf("server error: %w", err)
	}

	return tokens, nil
}

// createConfig creates a new encrypted config file in the tempDir.  It is
// based on template but has the tokens updated to match tokens.
func createConfig(ctx context.Context, template string, tempDir string) (*Config, string, error) {
	configBytes, err := os.ReadFile(template)
	if err != nil {
		return nil, "", fmt.Errorf("unable to read template config: %w", err)
	}

	configJSON, err := decryptYAMLConfig(configBytes)
	if err != nil {
		return nil, "", fmt.Errorf("unable to decrypt template config: %w", err)
	}

	var config Config
	err = json.Unmarshal(configJSON, &config)
	if err != nil {
		return nil, "", fmt.Errorf("unable to parse template config: %w", err)
	}

	// Timeout 1 minute before the test timeout, this will hopefully give us a
	// chance to cleanup the test account.
	timeoutFlag := flag.Lookup("test.timeout")
	timeout, err := time.ParseDuration(timeoutFlag.Value.String())
	if err != nil {
		return nil, "", fmt.Errorf("unable to parse test.timeout: %q", timeoutFlag)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout-time.Minute)
	defer cancel()

	tokens, err := getTokens(ctx, config.Credentials)
	if err != nil {
		return nil, "", err
	}

	config.Credentials.RefreshToken = tokens.RefreshToken
	config.Credentials.AccessToken = tokens.AccessToken
	config.Credentials.AccessTokenExpiresAt = tokens.AccessTokenExpiresAt

	updates := map[string]any{
		"credentials.refresh_token":           tokens.RefreshToken.Expose(),
		"credentials.access_token":            tokens.AccessToken.Expose(),
		"credentials.access_token_expires_at": tokens.AccessTokenExpiresAt,
	}
	for path, update := range updates {
		configJSON, err = sjson.SetBytes(configJSON, path, update)
		if err != nil {
			return nil, "", fmt.Errorf("unable to expose %s: %w", path, err)
		}
	}

	tempConfigBytes, err := encryptConfig(configJSON)
	if err != nil {
		return nil, "", fmt.Errorf("unable to encrypt temp config: %w", err)
	}
	tempConfigFile := filepath.Join(tempDir, "materialize-hubspot.config.yaml")
	err = os.WriteFile(tempConfigFile, tempConfigBytes, 0o600)
	if err != nil {
		return nil, "", fmt.Errorf("unable to write temp config: %w", err)
	}

	return &config, tempConfigFile, nil
}

// projectionFields returns the projection.Field values for the collection.
func projectionFields(schema gjson.Result, prefix string) []string {
	var ptrs []string
	props := schema.Get("properties")
	props.ForEach(func(key, value gjson.Result) bool {
		ptr := key.String()
		if prefix != "" {
			ptr = prefix + "/" + ptr
		}
		if value.Get("type").String() == "object" {
			ptrs = append(ptrs, projectionFields(value, ptr)...)
		} else {
			ptrs = append(ptrs, ptr)
		}
		return true
	})
	return ptrs
}

func tasks(
	t *testing.T,
	ctx context.Context,
	sourcePath string,
	config *Config,
	configFile string,
) iter.Seq2[Task, error] {
	return func(yield func(Task, error) bool) {
		tempDir := t.TempDir()

		bundle := testutil.RunFlowctl(t, "raw", "bundle", "--source", sourcePath)

		for _, taskName := range taskNames(bundle) {
			bundle, err := sjson.SetBytes(bundle, fmt.Sprintf("materializations.%s.endpoint.local.config", taskName), configFile)
			if err != nil {
				yield(Task{}, fmt.Errorf("unable to update task config for %s: %w", taskName, err))
				return
			}

			var bindings []TestBinding
			gjson.GetBytes(bundle, fmt.Sprintf("materializations.%s.bindings", taskName)).ForEach(func(_, binding gjson.Result) bool {
				var resource Resource
				err := boilerplate.UnmarshalStrict(json.RawMessage(gjson.Get(binding.Raw, "resource").Raw), &resource)
				if err != nil {
					log.WithError(err).Error("unable to unmarshal resource")
					return false
				}

				collectionName := gjson.Get(binding.Raw, "source").Str
				collectionSchema := gjson.GetBytes(bundle, fmt.Sprintf("collections.%s.schema", collectionName))
				var properties []string
				for _, ptr := range projectionFields(collectionSchema, "") {
					propertyName, err := PropertyName(ptr)
					if err != nil {
						log.WithError(err).Error("unable convert schema property")
						return false
					}
					properties = append(properties, propertyName)
				}

				var key string
				gjson.GetBytes(bundle, fmt.Sprintf("collections.%s.key", collectionName)).ForEach(func(_, value gjson.Result) bool {
					key = strings.TrimLeft(value.Str, "/")
					return true
				})

				bindings = append(bindings, TestBinding{
					Resource:   resource,
					Collection: collectionName,
					Properties: properties,
					Key:        key,
				})

				return true
			})

			source := filepath.Join(tempDir, "test.flow.yaml")
			err = os.WriteFile(source, bundle, 0o600)
			if err != nil {
				yield(Task{}, fmt.Errorf("unable to write source: %w", err))
				return
			}

			task := Task{
				Name:     taskName,
				Source:   source,
				Bundle:   bundle,
				Config:   config,
				Bindings: bindings,
			}
			if !yield(task, nil) {
				return
			}
		}
	}
}

func snapshotBinding(ctx context.Context, task Task, binding TestBinding, actionDesc []byte) (string, error) {
	resource := binding.Resource
	object, err := resource.CRMObject()
	if err != nil {
		return "", err
	}

	client, err := NewClientDefaultLimiter(task.Config.Credentials)
	if err != nil {
		return "", err
	}

	var builder strings.Builder
	builder.WriteString("Resource: " + resource.Object)
	builder.WriteString("\n")
	builder.Write(actionDesc)
	builder.WriteString("\n")

	request := &GetRequest{
		Properties: binding.Properties,
		Limit:      100,
	}
	resp, err := client.Get(ctx, object, request)
	if err != nil {
		return "", err
	}

	// Order results by key's property name.
	keyPropertyName, err := PropertyName(binding.Key)
	if err != nil {
		return "", err
	}
	sort.Slice(resp.Results, func(i, j int) bool {
		lhs := resp.Results[i].Properties[keyPropertyName]
		if _, ok := lhs.(string); !ok {
			lhs = ""
		}
		rhs := resp.Results[j].Properties[keyPropertyName]
		if _, ok := rhs.(string); !ok {
			rhs = ""
		}
		return lhs.(string) < rhs.(string)
	})

	for _, result := range resp.Results {
		// Remove properties that we didn't set since we can't predict their
		// value.  Removing instead of sanitizing since they vary depending on
		// the object type.
		for k, _ := range result.Properties {
			if !slices.Contains(binding.Properties, k) {
				delete(result.Properties, k)
			}
		}

		// In a fresh test account, some objects have sample records, these
		// have dynamic values so we remove them.
		switch binding.Resource.Object {
		case "Meetings":
			title := result.Properties["hs_meeting_title"]
			if s, ok := title.(string); ok {
				if strings.HasPrefix(s, "(Sample meeting)") {
					continue
				}
			}
		case "Calls":
			body := result.Properties["hs_call_body"]
			if s, ok := body.(string); ok {
				if strings.HasPrefix(s, "(Sample call)") {
					continue
				}
			}
		}

		record, err := json.Marshal(result.Properties)
		if err != nil {
			return "", err
		}
		builder.Write(record)
		builder.WriteString("\n")
	}

	return builder.String(), nil
}

func runMaterializationTestForTask(
	t *testing.T,
	ctx context.Context,
	newMaterializer NewMaterializerFn,
	task Task,
	actionDescSanitizers []func(string) string,
) (string, error) {
	var snap strings.Builder

	actionDescription := testutil.RunFlowctl(
		t,
		"preview",
		"--name", task.Name,
		"--source", task.Source,
		"--fixture", "testdata/fixture.materialize.json",
		"--network", "flow-test",
		"--output-apply",
		"--output-state",
	)
	for _, sanitize := range actionDescSanitizers {
		actionDescription = []byte(sanitize(string(actionDescription)))
	}

	for idx, binding := range task.Bindings {
		if idx != 0 {
			snap.WriteString("\n\n")
		}
		snapshot, err := snapshotBinding(ctx, task, binding, actionDescription)
		if err != nil {
			return "", err
		}
		snap.WriteString(snapshot)
	}

	return snap.String(), nil
}

type NewMaterializerFn = func(context.Context, string, *Config, map[string]bool) (*materialization, error)

func RunMaterializationTest(
	t *testing.T,
	newMaterializer NewMaterializerFn,
	sourcePath string,
	config *Config,
	configFile string,
	actionDescSanitizers []func(string) string,
) {
	var snap strings.Builder

	ctx := t.Context()

	for task, err := range tasks(t, ctx, sourcePath, config, configFile) {
		require.NoError(t, err)

		snap.WriteString(fmt.Sprintf("Task: %s\n\n", task.Name))
		results, err := runMaterializationTestForTask(t, ctx, newMaterializer, task, actionDescSanitizers)
		require.NoError(t, err)
		snap.WriteString(results)
	}

	cupaloy.SnapshotT(t, snap.String())
}

// Cleanup when test ends or on SIGINT.
func Cleanup(t *testing.T, f func()) {
	t.Cleanup(f)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		f()
		os.Exit(1)
	}()
}

func TestIntegration(t *testing.T) {
	if !*integration {
		t.Skip("skipping integration test; add -interactive to enable")
	}
	ctx := t.Context()

	client := NewLocalDevClient()
	t.Cleanup(client.Close)

	portal, err := NewPortal("testdata/portal.yaml")
	require.NoError(t, err)
	err = portal.RefreshToken(ctx, client)
	require.NoError(t, err)

	accountName := fmt.Sprintf("driver_test_%d_%s", time.Now().Unix(), uuid.NewString()[:4])
	accountConfig := NewTestAccountConfig(accountName)

	testAccount, err := client.CreateTestAccount(ctx, portal, accountConfig)
	require.NoError(t, err)

	Cleanup(t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		err := client.DeleteTestAccount(ctx, portal, testAccount)
		if err != nil {
			t.Logf("unable to delete test account: %v", err)
		} else {
			t.Logf("removed test-account: %s", testAccount.AccountName)
		}
	})
	t.Logf("created test-account: %s", testAccount.AccountName)

	// This is an interactive step.
	config, configFile, err := createConfig(ctx, "testdata/materialize-hubspot.template.config.yaml", t.TempDir())
	require.NoError(t, err)

	t.Run("materialize", func(t *testing.T) {
		RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", config, configFile, nil)
	})
}
