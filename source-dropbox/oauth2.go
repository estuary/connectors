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
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://www.dropbox.com/oauth2/authorize?token_access_type=offline&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	scopes := GetScopes()

	spec := pf.OAuth2{
		Provider:                  "dropbox",
		AuthUrlTemplate:           fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate:    "https://api.dropboxapi.com/oauth2/token",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`)},
		AccessTokenBody:           "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"access_token":  json.RawMessage(`"/access_token"`),
			"refresh_token": json.RawMessage(`"/refresh_token"`),
			"token_type":    json.RawMessage(`"/token_type"`),
			"expires_in":    json.RawMessage(`"/expires_in"`),
			"scope":         json.RawMessage(`"/scope"`),
			"uid":           json.RawMessage(`"/uid"`),
			"account_id":    json.RawMessage(`"/account_id"`),
		},
	}

	return &spec
}

type Credentials struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpireTime   int    `json:"expires_in"`
	Scope        string `json:"scope"`
	UID          string `json:"uid"`
	AccountID    string `json:"account_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

func (c *Credentials) Validate() error {
	requiredProperties := [][]string{
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
	return c.AccessToken, nil
}
