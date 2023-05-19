package main

import (
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/slack-go/slack"
)

type CredentialConfig struct {
	// Provided by runtime
	ClientID     string `json:"client_id,omitempty" jsonschema_extras:"secret=true"`
	ClientSecret string `json:"client_secret,omitempty" jsonschema_extras:"secret=true"`

	// Extracted by us in AccessTokenResponseJsonMap
	AccessToken string `json:"access_token,omitempty" jsonschema_extras:"secret=true"`
}

func (c *CredentialConfig) validateClientCreds() error {
	if c.AccessToken == "" {
		return fmt.Errorf("missing access token for oauth2")
	} else if c.ClientID == "" {
		return fmt.Errorf("missing client ID for oauth2")
	} else if c.ClientSecret == "" {
		return fmt.Errorf("missing client secret for oauth2")
	}

	return nil
}

func (c *CredentialConfig) SlackAPI(config SlackSenderConfig) *SlackAPI {
	return &SlackAPI{
		Client:       *slack.New(c.AccessToken),
		SenderConfig: config,
	}
}

const AUTH_URL_TEMPLATE_FORMAT_STR = "https://slack.com/oauth/v2/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}&scope={{#urlencode}}%s{{/urlencode}}"

func Spec(scopes ...string) *pf.OAuth2 {
	return &pf.OAuth2{
		Provider:               "slack",
		AuthUrlTemplate:        fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate: "https://slack.com/api/oauth.v2.access",
		AccessTokenBody:        "grant_type=authorization_code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}",
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"content-type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"access_token": json.RawMessage(`"/access_token"`),
		},
	}
}
