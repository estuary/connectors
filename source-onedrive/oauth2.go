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
	auth "github.com/microsoft/kiota-authentication-azure-go"
	"golang.org/x/oauth2"
)

var scopes = []string{"Files.Read.All", "offline_access", "User.Read"}

func oauth2Spec() *pf.OAuth2 {
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	spec := pf.OAuth2{
		Provider: "microsoft",
		AuthUrlTemplate: fmt.Sprintf(
			AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		AccessTokenBody: "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
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
		{"ClientID", c.ClientID},
		{"ClientSecret", c.ClientSecret},
		{"RefreshToken", c.RefreshToken},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

type tokenCredential struct {
	tokenSource oauth2.TokenSource
}

func (c *tokenCredential) GetToken(
	ctx context.Context, opts policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	token, err := c.tokenSource.Token()
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf("error getting token: %w", err)
	}

	return azcore.AccessToken{
		Token:     token.AccessToken,
		ExpiresOn: token.Expiry,
	}, nil
}

func (c *credentials) getAuthProvider(ctx context.Context) (
	*auth.AzureIdentityAuthenticationProvider, error,
) {
	azureEndpoint := oauth2.Endpoint{
		AuthURL:   "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
		TokenURL:  "https://login.microsoftonline.com/common/oauth2/v2.0/token",
		AuthStyle: oauth2.AuthStyleInParams,
	}

	oauthConfig := &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		Endpoint:     azureEndpoint,
		Scopes:       scopes,
	}

	token := &oauth2.Token{
		RefreshToken: c.RefreshToken,
		Expiry:       time.Now(), // Require a new access token to be obtained.
	}

	tokenSource := oauthConfig.TokenSource(ctx, token)

	cred := &tokenCredential{
		tokenSource: tokenSource,
	}

	authProvider, err := auth.
		NewAzureIdentityAuthenticationProviderWithScopes(cred, scopes)
	if err != nil {
		return nil, fmt.Errorf("error creating auth provider: %w", err)
	}

	return authProvider, nil
}
