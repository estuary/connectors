package main

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type config struct {
	ProjectID          string `json:"projectId" jsonschema:"title=Project ID,description=Google Cloud Project ID that contains the PubSub topics." jsonschema_extras:"order=0"`
	CredentialsJSON    string `json:"googleCredentials" jsonschema:"title=Credentials,description=Google Cloud Service Account JSON credentials to use for authentication." jsonschema_extras:"secret=true,multiline=true,order=1"`
	SubscriptionPrefix string `json:"subscriptionPrefix,omitempty" jsonschema:"title=Subscription Prefix,description=Prefix to prepend to the PubSub topics subscription names. Subscription names will be in the form of <prefix>_EstuaryFlow_<random string> if a prefix is provided vs. EstuaryFlow_<random string> if no prefix is provided." jsonschema_extras:"order=2"`
}

func (c *config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("missing project ID")
	} else if c.CredentialsJSON == "" {
		return fmt.Errorf("missing service account credentials JSON")
	} else if !json.Valid([]byte(c.CredentialsJSON)) {
		return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
	}

	return nil
}

func (c *config) client(ctx context.Context) (*pubsub.Client, error) {
	creds, err := google.CredentialsFromJSON(ctx, []byte(c.CredentialsJSON), pubsub.ScopePubSub)
	if err != nil {
		return nil, fmt.Errorf("creating credentials: %w", err)
	}

	client, err := pubsub.NewClient(
		ctx,
		c.ProjectID,
		option.WithCredentials(creds),
	)
	if err != nil {
		return nil, err
	}

	return client, err
}

type resource struct {
	Topic string `json:"topic" jsonschema:"title=Topic,description=Name of the PubSub topic to subscribe to."`
}

func (r *resource) Validate() error {
	if r.Topic == "" {
		return fmt.Errorf("missing topic")
	}

	return nil
}

func main() {
	boilerplate.RunMain(new(driver))
}
