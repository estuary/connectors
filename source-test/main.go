package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
)

type Config struct {
	Greetings        int              `json:"greetings"`
	SkipState        bool             `json:"skip_state"`
	FailAfter        int              `json:"fail_after"`
	ExitAfter        int              `json:"exit_after"`
	OAuthCredentials oauthCredentials `json:"credentials"`
}

type ResourceConfig struct {
	Stream   string `json:"stream"`
	SyncMode string `json:"syncMode"`
}

func (c *ResourceConfig) Validate() error { return nil }

type oauthCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type State struct {
	Cursor int `json:"cursor"`
}

type Greeting struct {
	Count   int    `json:"count"`
	Message string `json:"message"`
}

func (c *Config) Validate() error {
	if c.Greetings == 0 {
		return fmt.Errorf("missing greetings")
	}
	return nil
}

func (c *State) Validate() error {
	return nil
}

const configSchema = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title":   "Test Source Spec",
	"type":    "object",
	"required": [
		"greetings"
	],
	"properties": {
		"greetings": {
			"type":        "integer",
			"title":       "Number of Greetings",
			"description": "Number of greeting documents to produce when running in non-tailing mode",
			"default":     1000
		},
    "skip_state": {
      "type": "boolean",
      "title": "Skip sending a checkpoint",
      "description": "Some Airbyte connectors do not send a state message. This option can be used to emulate those cases",
      "default": false
    },
    "fail_after": {
      "type": ["integer", "null"],
      "title": "Fail after sending N number of greetings",
      "description": "Fail after sending N number of greetings"
    },
    "exit_after": {
      "type": ["integer", "null"],
      "title": "Exit after sending N number of greetings",
      "description": "Exit after sending N number of greetings"
    },
    "credentials": {
      "type": "object",
      "title": "Authentication",
      "description": "Google API Credentials",
      "x-oauth2-provider": "google",
      "properties": {
        "client_id": {
          "title": "Client ID",
          "type": "string",
          "secret": true
        },
        "client_secret": {
          "title": "Client Secret",
          "type": "string",
          "secret": true
        },
        "access_token": {
          "title": "Access Token",
          "type": "string",
          "secret": true
        },
        "refresh_token": {
          "title": "Refresh Token",
          "type": "string",
          "secret": true
        }
      }
    }
	}
}`

func main() {
	boilerplate.RunMain(connector{})
}

type connector struct{}

var _ boilerplate.Connector = &connector{}

func (connector) Spec(context.Context, *pc.Request_Spec) (*pc.Response_Spec, error) {
	resourceSchema, err := schemagen.GenerateSchema("Test Resource Spec", &ResourceConfig{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(configSchema),
		ResourceConfigSchemaJson: resourceSchema,
		Oauth2: &flow.OAuth2{
			Provider:               "google",
			AuthUrlTemplate:        "https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent&client_id={{ client_id }}&redirect_uri={{ redirect_uri }}&response_type=code&scope=email&state={{ state }}",
			AccessTokenUrlTemplate: "https://oauth2.googleapis.com/token",
			AccessTokenBody:        "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{ client_id }}\", \"client_secret\": \"{{ client_secret }}\", \"redirect_uri\": \"{{ redirect_uri }}\", \"code\": \"{{ code }}\"}",
			AccessTokenResponseJsonMap: map[string]json.RawMessage{
				"access_token":  json.RawMessage(`"/access_token"`),
				"refresh_token": json.RawMessage(`"/refresh_token"`),
			},
		},
	}, nil
}

func (connector) Discover(context.Context, *pc.Request_Discover) (*pc.Response_Discovered, error) {
	greetingSchema, err := schemagen.GenerateSchema("Greeting", &Greeting{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating greeting schema: %w", err)
	}

	return &pc.Response_Discovered{
		Bindings: []*pc.Response_Discovered_Binding{
			{
				RecommendedName:    "greetings",
				ResourceConfigJson: json.RawMessage(`{"stream":"greetings"}`),
				DocumentSchemaJson: greetingSchema,
				Key:                []string{"/count"},
			},
		},
	}, nil
}

func (connector) Validate(_ context.Context, validate *pc.Request_Validate) (*pc.Response_Validated, error) {
	var config Config
	if err := flow.UnmarshalStrict(validate.ConfigJson, &config); err != nil {
		return nil, err
	}
	var out []*pc.Response_Validated_Binding

	for i, binding := range validate.Bindings {
		if err := flow.UnmarshalStrict(binding.ResourceConfigJson, &ResourceConfig{}); err != nil {
			return nil, fmt.Errorf("binding %d: %w", i, err)
		}
		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{"greetings", strconv.Itoa(i)},
		})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

func (connector) Apply(_ context.Context, apply *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{}, nil
}

func (connector) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var config Config
	if err := flow.UnmarshalStrict(open.Capture.ConfigJson, &config); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}
	var state State
	if err := flow.UnmarshalStrict(open.StateJson, &state); err != nil {
		return fmt.Errorf("parsing state: %w", err)
	}

	var bindings = open.Capture.Bindings

	// Notify Flow that we're starting.
	if err := stream.Ready(false); err != nil {
		return err
	}
	for {
		if config.FailAfter != 0 && state.Cursor >= config.FailAfter {
			return fmt.Errorf("a horrible, no good error!")
		}
		if config.ExitAfter != 0 && state.Cursor >= config.ExitAfter {
			return nil // All done.
		}

		for binding := range bindings {

			var b, err = json.Marshal(struct {
				Count   int    `json:"count"`
				Message string `json:"message"`
			}{
				state.Cursor,
				fmt.Sprintf("Hello #%d", state.Cursor),
			})
			if err != nil {
				return err
			}

			if err := stream.Documents(binding, b); err != nil {
				return err
			}
		}

		state.Cursor++

		if !config.SkipState {
			if b, err := json.Marshal(state); err != nil {
				return err
			} else if err = stream.Checkpoint(b, false); err != nil {
				return err
			}
		}

		if config.ExitAfter == 0 && config.FailAfter == 0 {
			<-time.After(time.Millisecond * 500)
		}
	}
}
