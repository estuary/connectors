// This file implements OAuth2 refresh token authentication for the Microsoft Graph SDK.
//
// The challenge: Microsoft Graph SDK uses Azure Identity authentication providers,
// but we need to use OAuth2 refresh tokens stored in our endpoint configuration.
//
// The solution: We implement a custom azcore.TokenCredential that wraps the OAuth2
// refresh token flow, allowing automatic token refresh for every API request.
//
// Architecture:
// 1. oauth2.Config + oauth2.TokenSource: Handles OAuth2 refresh token flow
// 2. oauth2TokenCredential: Wraps TokenSource to implement azcore.TokenCredential
// 3. AzureIdentityAuthenticationProvider: Uses our credential for Graph SDK requests
// 4. Microsoft Graph SDK: Calls GetToken() automatically for every API request
//
// This ensures we never make API calls with expired access tokens, as the OAuth2
// library automatically refreshes them using our stored refresh token.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	pf "github.com/estuary/flow/go/protocols/flow"
	auth "github.com/microsoftgraph/msgraph-sdk-go-core/authentication"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/microsoft"
)

var scopes = []string{
	"https://graph.microsoft.com/Files.Read.All",
	"https://graph.microsoft.com/Sites.Read.All",
}

func oauth2Spec() *pf.OAuth2 {
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_mode=query&scope={{#urlencode}}%s{{/urlencode}}&state={{{ state }}}"

	spec := pf.OAuth2{
		Provider:                  "microsoft",
		AuthUrlTemplate:           fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate:    "https://login.microsoftonline.com/common/oauth2/v2.0/token",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`)},
		AccessTokenBody:           "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token": json.RawMessage(`"/refresh_token"`),
		},
	}

	return &spec
}

type credentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
}

func (c *credentials) validate() error {
	requiredProperties := [][]string{
		{"RefreshToken", c.RefreshToken},
		{"ClientID", c.ClientID},
		{"ClientSecret", c.ClientSecret},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

// getTokenCredential creates a token credential from our OAuth2 setup for the Microsoft Graph SDK.
// This bridges the OAuth2 refresh token flow with Azure SDK's authentication interface.
//
// The key insight here is that we create an oauth2.TokenSource that automatically handles
// token refresh using our stored refresh token, then wrap it in a custom azcore.TokenCredential
// implementation that the Microsoft Graph SDK can use.
func (c *credentials) getTokenCredential(ctx context.Context) (azcore.TokenCredential, error) {
	// Configure OAuth2 with Microsoft's endpoints and our stored credentials
	oauthConfig := &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		Endpoint:     microsoft.AzureADEndpoint("common"),
		Scopes:       scopes,
	}

	// Create a token with our refresh token, but set expiry to now to force immediate refresh.
	// This ensures we get a fresh access token on first use rather than trying to use
	// a potentially stale or invalid access token.
	token := &oauth2.Token{
		RefreshToken: c.RefreshToken,
		Expiry:       time.Now(), // Force immediate refresh on first use
	}

	// Create a TokenSource that will automatically refresh access tokens using the refresh token.
	// This TokenSource will:
	// 1. Check if the current access token is expired on each call to Token()
	// 2. If expired, automatically use the refresh token to get a new access token
	// 3. Cache the new access token for subsequent requests
	// 4. Return the cached token if it's still valid
	tokenSource := oauthConfig.TokenSource(ctx, token)

	// Wrap the OAuth2 TokenSource in our custom azcore.TokenCredential implementation
	// so the Microsoft Graph SDK can use it
	return &oauth2TokenCredential{
		tokenSource: tokenSource,
	}, nil
}

// oauth2TokenCredential wraps an OAuth2 TokenSource to implement azcore.TokenCredential.
// This allows us to use OAuth2 refresh token flow with the Microsoft Graph SDK.
//
// The Microsoft Graph SDK calls GetToken() for every API request, ensuring we always
// have a valid access token. The underlying OAuth2 TokenSource handles all the
// complexity of checking expiration and refreshing tokens automatically.
type oauth2TokenCredential struct {
	tokenSource oauth2.TokenSource
}

// GetToken implements azcore.TokenCredential.GetToken().
// This method is called by the Microsoft Graph SDK for every API request.
//
// Flow:
// 1. Microsoft Graph SDK needs to make an API request
// 2. SDK calls this GetToken() method to get an access token
// 3. We call tokenSource.Token() which automatically:
//   - Returns cached token if still valid
//   - Uses refresh token to get new access token if expired
//
// 4. We convert the OAuth2 token format to Azure SDK format
// 5. SDK uses the access token in the Authorization header
//
// This ensures we never make API requests with expired tokens.
func (tc *oauth2TokenCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	// Get token from OAuth2 TokenSource (automatically refreshes if needed)
	token, err := tc.tokenSource.Token()
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf("getting OAuth2 token: %w", err)
	}

	// Convert OAuth2 token format to Azure SDK token format
	return azcore.AccessToken{
		Token:     token.AccessToken,
		ExpiresOn: token.Expiry,
	}, nil
}

// getAuthProvider creates a Microsoft Graph authentication provider using our token credential.
// This completes the bridge from OAuth2 refresh tokens to Microsoft Graph SDK authentication.
//
// Flow: OAuth2 refresh token → TokenCredential → AuthenticationProvider → Microsoft Graph SDK
func (c *credentials) getAuthProvider(ctx context.Context) (*auth.AzureIdentityAuthenticationProvider, error) {
	// Get our custom token credential that wraps the OAuth2 refresh token flow
	credential, err := c.getTokenCredential(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating token credential: %w", err)
	}

	// Create authentication provider using Azure Identity pattern.
	// This provider will call our credential's GetToken() method for every API request.
	// We use ".default" scope which represents all the permissions we've been granted.
	authProvider, err := auth.NewAzureIdentityAuthenticationProviderWithScopes(
		credential,
		[]string{"https://graph.microsoft.com/.default"},
	)
	if err != nil {
		return nil, fmt.Errorf("creating auth provider: %w", err)
	}

	return authProvider, nil
}
