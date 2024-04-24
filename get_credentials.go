package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"golang.org/x/oauth2"
)

func main() {
	ctx := context.Background()
	conf := &oauth2.Config{
		ClientID:     "2hq0vjwfvjht1sm",
		ClientSecret: "x6iv3ifklo2nbia",
		Scopes: []string{
			"files.metadata.read",
			"files.content.read",
			// "account_info.read",  // Not sure if we need this
		},
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://www.dropbox.com/oauth2/authorize?response_type=offline",
			TokenURL: "https://api.dropboxapi.com/oauth2/token",
		},
	}

	// use PKCE to protect against CSRF attacks
	// https://www.ietf.org/archive/id/draft-ietf-oauth-security-topics-22.html#name-countermeasures-6
	verifier := oauth2.GenerateVerifier()

	// Redirect user to consent page to ask for permission
	// for the scopes specified above.
	url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v", url)
	fmt.Println()

	// Use the authorization code that is pushed to the redirect
	// URL. Exchange will do the handshake to retrieve the
	// initial access token. The HTTP Client returned by
	// conf.Client will refresh the token as necessary.
	fmt.Println("Enter the code:")
	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatal(err)
	}
	tok, err := conf.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		log.Fatal(err)
	}

	// Print the token variable as JSON
	tokenJSON, err := json.Marshal(tok)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(tokenJSON))

	client := conf.Client(ctx, tok)
	client.Get("...")
}
