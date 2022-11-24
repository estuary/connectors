package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/protocols/airbyte"
)

type Config struct {
	Greetings        int              `json:"greetings"`
	SkipState        bool             `json:"skip_state"`
	FailAfter        int              `json:"fail_after"`
	ExitAfter        int              `json:"exit_after"`
	OAuthCredentials oauthCredentials `json:"credentials"`
}

type oauthCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type State struct {
	Cursor int `json:"cursor"`
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
      "title": "Skip sending an Airbyte state message",
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

const greetingSchema = `{
	"type":"object",
	"properties": {
		"count":   {"type": "integer"},
		"message": {"type": "string"}
	},
	"required": ["count", "message"]
}`

const oauthSpec = `{
  "provider": "google",
  "authUrlTemplate": "https://accounts.google.com/o/oauth2/auth?access_type=offline&prompt=consent&client_id={{ client_id }}&redirect_uri={{ redirect_uri }}&response_type=code&scope=email&state={{ state }}",
  "accessTokenUrlTemplate": "https://oauth2.googleapis.com/token",
  "accessTokenBody": "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{ client_id }}\", \"client_secret\": \"{{ client_secret }}\", \"redirect_uri\": \"{{ redirect_uri }}\", \"code\": \"{{ code }}\"}",
  "accessTokenResponseMap": "{\"access_token\": \"/access_token\",\"refresh_token\": \"/refresh_token\"}"
}`

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

var spec = airbyte.Spec{
	SupportsIncremental:           true,
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
	ConnectionSpecification:       json.RawMessage(configSchema),
	AuthSpecification:             json.RawMessage(oauthSpec),
}

func doCheck(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}

	if err := args.ConfigFile.Parse(new(Config)); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	var config Config

	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	}

	var catalog = new(airbyte.Catalog)
	catalog.Streams = append(catalog.Streams, airbyte.Stream{
		Name:                    "greetings",
		JSONSchema:              json.RawMessage(greetingSchema),
		SupportedSyncModes:      []airbyte.SyncMode{airbyte.SyncModeIncremental},
		SourceDefinedCursor:     true,
		SourceDefinedPrimaryKey: [][]string{{"count"}},
	})

	var encoder = airbyte.NewStdoutEncoder()
	return encoder.Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

func doRead(args airbyte.ReadCmd) error {
	var config Config
	var state State
	var catalog airbyte.ConfiguredCatalog

	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	} else if err := args.CatalogFile.Parse(&catalog); err != nil {
		return err
	} else if args.StateFile != "" {
		if err := args.StateFile.Parse(&state); err != nil {
			return err
		}
	}

	var enc = airbyte.NewStdoutEncoder()
	var now = time.Now()
	for {
		if config.FailAfter != 0 && state.Cursor >= config.FailAfter {
			return fmt.Errorf("a horrible, no good error!")
		}
		if config.ExitAfter != 0 && state.Cursor >= config.ExitAfter {
			return nil // All done.
		}

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
		state.Cursor++

		if err = enc.Encode(&airbyte.Message{
			Type: airbyte.MessageTypeRecord,
			Record: &airbyte.Record{
				Stream:    "greetings",
				EmittedAt: now.UTC().UnixNano() / int64(time.Millisecond),
				Data:      b,
			},
		}); err != nil {
			return err
		}

		if !config.SkipState {
			if b, err = json.Marshal(state); err != nil {
				return err
			} else if err = enc.Encode(airbyte.Message{
				Type:  airbyte.MessageTypeState,
				State: &airbyte.State{Data: b},
			}); err != nil {
				return err
			}
		}

		now = <-time.After(time.Millisecond * 500)
	}
}
