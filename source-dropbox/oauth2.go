package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/oauth2"
)

var scopes = []string{"files.metadata.read", "files.content.read"}

// Most of this code was converted from the Google Auth specification updating with
// the info on https://developers.dropbox.com/oauth-guide
func oauth2Spec() *pf.OAuth2 {
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#oauth2-authorize
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://www.dropbox.com/oauth2/authorize?token_access_type=offline&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	spec := pf.OAuth2{
		Provider:                  "dropbox",
		AuthUrlTemplate:           fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate:    "https://api.dropboxapi.com/oauth2/token",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`)},
		AccessTokenBody:           "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token": json.RawMessage(`"/refresh_token"`),
			// "access_token":  json.RawMessage(`"/access_token"`),
			// "token_type":    json.RawMessage(`"/token_type"`),
			// "expires_in":    json.RawMessage(`"/expires_in"`),
			// "scope":         json.RawMessage(`"/scope"`),
			// "uid":           json.RawMessage(`"/uid"`),
			// "account_id":    json.RawMessage(`"/account_id"`),
		},
	}

	return &spec
}

type Credentials struct {
	ClientID     string `json:"client_id" yaml:"client_id_sops"`
	ClientSecret string `json:"client_secret" yaml:"client_secret_sops"`
	RefreshToken string `json:"refresh_token" yaml:"refresh_token_sops"`
	// ExpiresIn    time.Duration `json:"expires_in"`
}

func (c *Credentials) Validate() error {
	requiredProperties := [][]string{
		{"RefreshToken", c.RefreshToken},
		// {"ClientID", c.ClientID},
		// {"ClientSecret", c.ClientSecret},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c *Credentials) GetOauthClient(ctx context.Context) *http.Client {
	oauthConfig := &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		Endpoint:     dropbox.OAuthEndpoint(""),
		Scopes:       scopes,
	}

	token := &oauth2.Token{
		RefreshToken: c.RefreshToken,
		Expiry:       time.Now(), // Require a new access token to be obtained.
	}

	return oauth2.NewClient(ctx, oauthConfig.TokenSource(ctx, token))
}
