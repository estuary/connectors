package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"golang.org/x/oauth2"
)

// TODO: Replace the client ID, client secret and redirect URL with the real ones
var (
	dropboxConfig = &oauth2.Config{
		Scopes: []string{
			"files.metadata.read",
			"files.content.read",
			// "account_info.read",  // Not sure if we need this
		},
		Endpoint:     dropbox.OAuthEndpoint(""),
		ClientID:     "client ID goes here",
		ClientSecret: "client secret goes here",
		RedirectURL:  "http://localhost:8080/oauth2/callback",
	}
)

func GetOAuth2HTTPClient(ctx context.Context, config *oauth2.Config) (*http.Client, error) {

	// token := new(oauth2.Token)
	// tokenSource := config.TokenSource(ctx, token)

	// use PKCE to protect against CSRF attacks
	// https://www.ietf.org/archive/id/draft-ietf-oauth-security-topics-22.html#name-countermeasures-6
	verifier := oauth2.GenerateVerifier()

	// TODO: Handle the code in the UI
	// Redirect user to consent page to ask for permission
	// for the scopes specified above.
	url := config.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v", url)

	var code string

	tok, err := config.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		log.Fatal(err)
	}

	client := config.Client(ctx, tok)

	return client, nil
}
