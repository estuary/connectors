package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	firestore "cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/schema_inference"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type State struct {
	// Maps each collection to the latest DocumentSnapshot that we have received
	Snapshots map[string]string
}

type config struct {
	// Service account JSON key to use as Application Default Credentials
	CredentialsJSON string `json:"googleCredentials" jsonschema:"title=Credentials,description=Google Cloud Service Account JSON credentials." jsonschema_extras:"secret=true,multiline=true"`
}

func (c *config) Validate() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("googleCredentials is required")
	}
	return nil
}

func (c *State) Validate() error {
	return nil
}

func main() {
	var endpointSchema, err = schemagen.GenerateSchema("Google Firestore", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}

	var spec = airbyte.Spec{
		SupportsIncremental:           true,
		SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
		ConnectionSpecification:       json.RawMessage(endpointSchema),
	}

	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

func doCheck(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}

	var cfg config
	if err := args.ConfigFile.Parse(&cfg); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}

	ctx := context.Background()
	sa := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	defer client.Close()

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

const DISCOVER_DOC_LIMIT = 40000

func doDiscover(args airbyte.DiscoverCmd) error {
	var cfg config
	if err := args.ConfigFile.Parse(&cfg); err != nil {
		return err
	}

	ctx := context.Background()
	sa := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	var catalog = new(airbyte.Catalog)
	var collections = client.Collections(ctx)
	for {
		var collection, err = collections.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		var query = collection.Query.Limit(DISCOVER_DOC_LIMIT)
		var docs = query.Documents(ctx)

		var docsCh = make(chan schema_inference.Document)
		var logEntry = log.WithField("collection", collection.ID)

		var eg = new(errgroup.Group)
		var schema schema_inference.Schema

		eg.Go(func() error {
			schema, err = schema_inference.Run(ctx, logEntry, docsCh)
			if err != nil {
				return fmt.Errorf("schema inference: %w", err)
			}
			return nil
		})

		for {
			var doc, err = docs.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return err
			}
			var data = doc.Data()
			data[firestore.DocumentID] = doc.Ref.ID
			docJson, err := json.Marshal(data)
			docsCh <- docJson
		}
		close(docsCh)

		if err := eg.Wait(); err != nil {
			return err
		}

		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    collection.ID,
			JSONSchema:              schema,
			SupportedSyncModes:      []airbyte.SyncMode{airbyte.SyncModeIncremental},
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: [][]string{{firestore.DocumentID}},
		})
	}

	var encoder = airbyte.NewStdoutEncoder()
	return encoder.Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

func doRead(args airbyte.ReadCmd) error {
	var cfg config
	var state State
	var catalog airbyte.ConfiguredCatalog

	if err := args.ConfigFile.Parse(&cfg); err != nil {
		return err
	} else if err := args.CatalogFile.Parse(&catalog); err != nil {
		return err
	} else if args.StateFile != "" {
		if err := args.StateFile.Parse(&state); err != nil {
			return err
		}
	}

	if state.Snapshots == nil {
		state.Snapshots = map[string]string{}
	}

	ctx := context.Background()
	sa := option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	var enc = airbyte.NewStdoutEncoder()
	for {
		var now = time.Now()
		for _, stream := range catalog.Streams {
			var streamName = stream.Stream.Name
			var streamState = state.Snapshots[streamName]

			var collection = client.Collection(streamName)
			// FIXME: DocumentID is not ordered, so the user must have a field for us to order the documents on
			var query = collection.Query.OrderBy(firestore.DocumentID, firestore.Asc)
			if streamState != "" {
				query = query.StartAt(streamState)
			}
			var docs = query.Documents(ctx)

			for {
				var doc, err = docs.Next()
				if err == iterator.Done {
					break
				} else if err != nil {
					return err
				}

				var data = doc.Data()
				data[firestore.DocumentID] = doc.Ref.ID
				docJson, err := json.Marshal(data)
				if err != nil {
					return err
				}
				if err = enc.Encode(&airbyte.Message{
					Type: airbyte.MessageTypeRecord,
					Record: &airbyte.Record{
						Stream:    streamName,
						EmittedAt: now.UTC().UnixNano() / int64(time.Millisecond),
						Data:      docJson,
					},
				}); err != nil {
					return err
				}

				state.Snapshots[streamName] = doc.Ref.ID
			}
		}

		newStateJson, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err = enc.Encode(airbyte.Message{
			Type:  airbyte.MessageTypeState,
			State: &airbyte.State{Data: newStateJson},
		}); err != nil {
			return err
		}

		if !catalog.Tail {
			return nil
		}

		now = <-time.After(time.Millisecond * 500)
	}
}
