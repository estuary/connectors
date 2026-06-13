package hubspot

import (
	"encoding/json"
	"net/url"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
)

var (
	authURL        = MustParseURL("https://app.hubspot.com/oauth/authorize")
	oauth2Provider = "hubspot"

	oauth2Scopes = []string{
		"oauth",

		"crm.objects.companies.read",
		"crm.objects.companies.write",
		"crm.schemas.companies.read",

		"crm.objects.contacts.read",
		"crm.objects.contacts.write",
		"crm.schemas.contacts.read",

		"crm.objects.deals.read",
		"crm.objects.deals.write",
		"crm.schemas.deals.read",

		"crm.objects.line_items.read",
		"crm.objects.line_items.write",
		"crm.schemas.line_items.read",

		"crm.objects.products.read",
		"crm.objects.products.write",

		"crm.objects.quotes.read",
		"crm.objects.quotes.write",
		"crm.schemas.quotes.read",

		"tickets",
	}

	oauth2OptionalScopes = []string{}
)

func OAuth2Spec() *pf.OAuth2 {
	authParams := "" +
		"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}" +
		"&scope=" + url.QueryEscape(strings.Join(oauth2Scopes, " ")) +
		"&optional_scope=" + url.QueryEscape(strings.Join(oauth2OptionalScopes, " ")) +
		"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}" +
		"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
	authURLTemplate := *authURL
	authURLTemplate.RawQuery = authParams

	accessParams := "" +
		"grant_type=authorization_code" +
		"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}" +
		"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}" +
		"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}" +
		"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"

	return &pf.OAuth2{
		Provider:        oauth2Provider,
		AccessTokenBody: accessParams,
		AuthUrlTemplate: authURLTemplate.String(),
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"content-type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token":           json.RawMessage(`"/refresh_token"`),
			"access_token":            json.RawMessage(`"/access_token"`),
			"access_token_expires_at": json.RawMessage(`"{{#now_plus}}{{ expires_in }}{{/now_plus}}"`),
		},
		AccessTokenUrlTemplate: "https://api.hubspot.com/oauth/2026-03/token",
	}
}
