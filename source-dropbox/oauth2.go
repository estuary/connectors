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

func oauth2Spec() *pf.OAuth2 {
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://www.dropbox.com/oauth2/authorize?token_access_type=offline&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	spec := pf.OAuth2{
		Provider:                  "dropbox",
		AuthUrlTemplate:           fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate:    "https://api.dropboxapi.com/oauth2/token",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{"Content-Type": json.RawMessage(`"application/x-www-form-urlencoded"`)},
		AccessTokenBody:           "client_id={{{ client_id }}}&client_secret={{{ client_secret }}}&code={{{ code }}}&grant_type=authorization_code&redirect_uri={{{ redirect_uri }}}",
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token": json.RawMessage(`"/refresh_token"`),
		},
	}

	return &spec
}

type Credentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
}

func (c *Credentials) Validate() error {
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
