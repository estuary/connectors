package google_auth

import (
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

const AUTH_URL_TEMPLATE_FORMAT_STR = "https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&scope={{#urlencode}}%s{{/urlencode}}&state={{#urlencode}}{{{ state }}}{{/urlencode}}"

func Spec(scopes ...string) *pf.OAuth2Spec {
	return &pf.OAuth2Spec{
		Provider:                   "google",
		AuthUrlTemplate:            fmt.Sprintf(AUTH_URL_TEMPLATE_FORMAT_STR, strings.Join(scopes, " ")),
		AccessTokenUrlTemplate:     "https://oauth2.googleapis.com/token",
		AccessTokenBody:            "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{{ client_id }}}\", \"client_secret\": \"{{{ client_secret }}}\", \"redirect_uri\": \"{{{ redirect_uri }}}\", \"code\": \"{{{ code }}}\"}",
		AccessTokenResponseMapJson: json.RawMessage("{\"refresh_token\": \"/refresh_token\"}"),
	}
}
