package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
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
		AccessTokenUrlTemplate:    "https://api.dropboxapi.com/oauth2/token", //"https://b6b837c0-fc5a-4498-ac2a-d6a7c907d9e8.mock.pstmn.io/oauth",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`)},
		AccessTokenBody:           "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"access_token": json.RawMessage(`"/access_token"`),
			"expires_in":   json.RawMessage(`"/expires_in"`),
		},
	}

	return &spec
}

type Credentials struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpireTime  int    `json:"expires_in"`
	Scope       string `json:"scope"`
	UID         string `json:"uid"`
	AccountID   string `json:"account_id"`
	// RefreshToken string `json:"refresh_token"`
}

func (c *Credentials) Validate() error {
	requiredProperties := [][]string{
		// {"RefreshToken", c.RefreshToken},
		{"AccessToken", c.AccessToken},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c *Credentials) GetAccessToken(ctx context.Context) (string, error) {
	// if err := c.Validate(); err != nil {
	// 	return "", err
	// }

	// oauthConfig := &oauth2.Config{
	// 	ClientID:     c.ClientID,
	// 	ClientSecret: c.ClientSecret,
	// 	Endpoint: oauth2.Endpoint{
	// 		AuthURL:  "https://www.dropbox.com/oauth2/authorize",
	// 		TokenURL: "https://api.dropboxapi.com/oauth2/token",
	// 	},
	// 	Scopes: GetScopes(),
	// }
	// verifier := oauth2.GenerateVerifier()
	// token, err := oauthConfig.Exchange(ctx, c.AccessToken, oauth2.VerifierOption(verifier))
	// if err != nil {
	// 	return "", err
	// }

	return c.AccessToken, nil
}
