package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type config struct {
	Dummy string `json:"dummy,omitempty" jsonschema:"title=Dummy Field,description=Discovery does not like a completely empty config"`
}

type state struct{}

func (c *config) Validate() error {
	return nil
}

func (c *state) Validate() error {
	return nil
}

type event struct {
	Stream string    `json:"stream" jsonschema:"title=Stream Name"`
	Time   time.Time `json:"ts" jsonschema:"title=Event Timestamp,description=The timestamp at which this tick event was generated."`
}

func main() {
	log.Info("connector starting")
	var configSchema, err = schemagen.GenerateSchema("Clock Settings", &config{}).MarshalJSON()
	if err != nil {
		panic(err)
	}
	var spec = airbyte.Spec{
		SupportsIncremental:     true,
		ConnectionSpecification: json.RawMessage(configSchema),
		DocumentationURL:        "https://go.estuary.dev/source-clock",
	}
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

func doCheck(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}

	if err := args.ConfigFile.Parse(new(config)); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	if err := args.ConfigFile.Parse(new(config)); err != nil {
		return err
	}

	var eventSchema, err = schemagen.GenerateSchema("Clock Events", &event{}).MarshalJSON()
	if err != nil {
		return fmt.Errorf("error generating event schema: %w", err)
	}

	var catalog = new(airbyte.Catalog)
	for _, interval := range []string{"5s", "1m", "1h"} {
		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    "tick" + interval,
			JSONSchema:              json.RawMessage(eventSchema),
			SupportedSyncModes:      []airbyte.SyncMode{airbyte.SyncModeIncremental},
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: [][]string{{"ts"}},
		})
	}

	var encoder = airbyte.NewStdoutEncoder()
	return encoder.Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

func doRead(args airbyte.ReadCmd) error {
	var config config
	var catalog airbyte.ConfiguredCatalog
	var state state
	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	} else if err := args.CatalogFile.Parse(&catalog); err != nil {
		return err
	} else if args.StateFile != "" {
		if err := args.StateFile.Parse(&state); err != nil {
			return err
		}
	}

	var out = make(chan airbyte.Message)
	var encoder = airbyte.NewStdoutEncoder()
	var eg, ctx = errgroup.WithContext(context.Background())
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-out:
				if err := encoder.Encode(msg); err != nil {
					return fmt.Errorf("error emitting message: %w", err)
				}
			}
		}
	})
	for _, stream := range catalog.Streams {
		var name = stream.Stream.Name
		var interval = name
		if strings.HasPrefix(interval, "tick") {
			interval = strings.TrimPrefix(interval, "tick")
		}
		var duration, err = time.ParseDuration(interval)
		if err != nil {
			return fmt.Errorf("invalid stream name %q", name)
		}
		eg.Go(func() error {
			var ticker = time.NewTicker(duration)
			for {
				select {
				case ts := <-ticker.C:
					var event = event{Stream: name, Time: ts.UTC()}
					bs, err := json.Marshal(event)
					if err != nil {
						return fmt.Errorf("error marshalling tick event: %w", err)
					}
					out <- airbyte.Message{
						Type: airbyte.MessageTypeRecord,
						Record: &airbyte.Record{
							Stream:    name,
							Data:      bs,
							EmittedAt: time.Now().UnixNano() / int64(time.Millisecond),
						},
					}
					out <- airbyte.Message{
						Type: airbyte.MessageTypeState,
						State: &airbyte.State{
							Data:  json.RawMessage(`{}`),
							Merge: true,
						},
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	return eg.Wait()
}
