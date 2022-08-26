package google_auth

import (
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

func Spec(scopes ...string) *pf.OAuth2Spec {
	authUrlTemplate := fmt.Sprintf(
		"https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent&client_id={{ client_id }}&redirect_uri={{ redirect_uri }}&response_type=code&scope=%s&state={{ state }}",
		strings.Join(scopes, " "),
	)

	return &pf.OAuth2Spec{
		Provider:                   "google",
		AuthUrlTemplate:            authUrlTemplate,
		AccessTokenUrlTemplate:     "https://oauth2.googleapis.com/token",
		AccessTokenBody:            "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{ client_id }}\", \"client_secret\": \"{{ client_secret }}\", \"redirect_uri\": \"{{ redirect_uri }}\", \"code\": \"{{ code }}\"}",
		AccessTokenResponseMapJson: json.RawMessage("{\"refresh_token\": \"/refresh_token\"}"),
	}
}
