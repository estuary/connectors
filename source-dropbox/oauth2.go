package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/oauth2"
)

func GetScopes() []string {
	return []string{
		"files.metadata.read",
		"files.content.read",
		// "account_info.read",  // Not sure if we need this
	}
}

// Most of this code was converted from the Google Auth specification updating with
// the info on https://developers.dropbox.com/oauth-guide
func OAuth2Spec() *pf.OAuth2 {
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#oauth2-authorize
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://www.dropbox.com/oauth2/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	scopes := GetScopes()

	spec := pf.OAuth2{
		Provider:        "dropbox",
		AuthUrlTemplate: fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		// Dropbox Docs: you’ll need to include `token_access_type=offline`` as a parameter on your
		// Authorization URL in order to return a refresh_token inside your access token payload.
		AccessTokenUrlTemplate:     "https://api.dropboxapi.com/oauth2/token?token_access_type=offline",
		AccessTokenBody:            `{"grant_type": "authorization_code", "client_id": "{{{ client_id }}}", "client_secret": "{{{ client_secret }}}", "redirect_uri": "{{{ redirect_uri }}}", "code": "{{{ code }}}"}`,
		AccessTokenResponseJsonMap: map[string]json.RawMessage{"refresh_token": json.RawMessage(`"/refresh_token"`)},
	}

	return &spec
}

type Credentials struct {
	AuthType     string `json:"auth_type"`
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
}

func (c *Credentials) Validate() error {
	requiredProperties := [][]string{
		{"AuthType", c.AuthType},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c *Credentials) GetClient(ctx context.Context) (*http.Client, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	oauthConfig := &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://www.dropbox.com/oauth2/authorize",
			TokenURL: "https://api.dropboxapi.com/oauth2/token",
		},
		Scopes: GetScopes(),
	}

	token := &oauth2.Token{
		RefreshToken: c.RefreshToken,
		Expiry:       time.Now(),
	}
	tokenSource := oauthConfig.TokenSource(ctx, token)
	token, err := tokenSource.Token()
	if err != nil {
		return nil, err
	}
	client := oauthConfig.Client(ctx, token)

	return client, nil
}
