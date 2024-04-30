package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
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

func (c *Credentials) GetOauthClient(ctx context.Context) (*http.Client, error) {
	ts := &TokenSource{cred: c, ctx: ctx}

	return oauth2.NewClient(ctx, ts), nil
}

type TokenSource struct {
	cred *Credentials
	ctx  context.Context
}

func (ts *TokenSource) Token() (*oauth2.Token, error) {
	tokenUrl := "https://api.dropboxapi.com/oauth2/token"
	method := "POST"

	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	writer.WriteField("client_id", ts.cred.ClientID)
	writer.WriteField("client_secret", ts.cred.ClientSecret)
	writer.WriteField("grant_type", "refresh_token")
	writer.WriteField("refresh_token", ts.cred.RefreshToken)

	err := writer.Close()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, tokenUrl, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var token oauth2.Token
	var respPayload TokenResponsePayload
	err = json.Unmarshal(body, &respPayload)
	if err != nil {
		return nil, err
	}

	token.Expiry = time.Unix(int64(respPayload.Expires), 0)
	token.AccessToken = respPayload.AccessToken
	token.RefreshToken = ts.cred.RefreshToken
	token.TokenType = respPayload.TokenType

	return &token, nil
}

type TokenResponsePayload struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Expires     int    `json:"expires_in"`
}
