package main

import (
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// Most of this code was converted from the Google Auth specification updating with
// the info on https://developers.dropbox.com/oauth-guide
func OAuth2Spec() *pf.OAuth2 {
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#oauth2-authorize
	AUTH_URL_TEMPLATE_FORMAT_STR := "https://www.dropbox.com/oauth2/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&response_type=code&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&state={{{ state }}}&scope={{#urlencode}}%s{{/urlencode}}"

	scopes := []string{
		"files.metadata.read,files.content.read",
		"files.content.read",
		// "account_info.read",  // Not sure if we need this
	}

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
