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
	// Last time a full table scan was run, we use this to know if we need to do a full scan
	// to reach eventual consistency
	LastScan string
}

type config struct {
	// Service account JSON key to use as Application Default Credentials
	CredentialsJSON string `json:"googleCredentials" jsonschema:"title=Credentials,description=Google Cloud Service Account JSON credentials." jsonschema_extras:"secret=true,multiline=true"`

	// How frequently should we scan all collections to ensure consistency
	ScanInterval string `json:"scan_interval" jsonschema:"title=Scan Interval,description=How frequently should all collections be scanned to ensure consistency. See https://pkg.go.dev/time#ParseDuration for supported values.,default=12h"`
}

func (c *config) Validate() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("googleCredentials is required")
	}
	if c.ScanInterval != "" {
		var _, err = time.ParseDuration(c.ScanInterval)

		if err != nil {
			return fmt.Errorf("parsing scan interval failed: %w", err)
		}
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

// struct sent over channel to encoder
type encDocument struct {
	doc        []byte
	streamName string
	state      []byte
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

	var lastScan = time.UnixMilli(0)
	if state.LastScan != "" {
		var err error
		lastScan, err = time.Parse(time.RFC3339, state.LastScan)
		if err != nil {
			return fmt.Errorf("parsing last scan checkpoint failed: %w", err)
		}
	}

	var scanInterval, _ = time.ParseDuration("12h")
	if cfg.ScanInterval != "" {
		var err error
		scanInterval, err = time.ParseDuration(cfg.ScanInterval)

		if err != nil {
			return fmt.Errorf("parsing scan interval failed: %w", err)
		}
	}

	log.WithFields(log.Fields{
		"last_scan":     lastScan,
		"scan_interval": scanInterval,
	}).Info("state check for scan")

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

	// The channel where documents will be sent to from different streams
	// this is here to ensure we write documents sequentially and avoid scrambled outputs
	var docsCh = make(chan encDocument)

	var eg = new(errgroup.Group)

	for _, stream := range catalog.Streams {
		var streamName = stream.Stream.Name

		var collection = client.Collection(streamName)
		eg.Go(func() error {
			return fullScan(ctx, lastScan, scanInterval, collection, docsCh, catalog.Tail)
		})

		if catalog.Tail {
			eg.Go(func() error {
				return listenForChanges(ctx, collection, docsCh)
			})
		}
	}

	eg.Go(func() error {
		return encodeDocs(docsCh)
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	close(docsCh)

	return nil
}

func encodeDocs(docsCh chan encDocument) error {
	var enc = airbyte.NewStdoutEncoder()

	for {
		encDoc := <-docsCh

		if len(encDoc.doc) != 0 {
			log.WithFields(log.Fields{
				"doc":        string(encDoc.doc),
				"streamName": encDoc.streamName,
			}).Info("encoding doc")

			if err := enc.Encode(&airbyte.Message{
				Type: airbyte.MessageTypeRecord,
				Record: &airbyte.Record{
					Stream:    encDoc.streamName,
					EmittedAt: time.Now().UTC().UnixNano() / int64(time.Millisecond),
					Data:      encDoc.doc,
				},
			}); err != nil {
				return err
			}
			if err := enc.Encode(airbyte.Message{
				Type:  airbyte.MessageTypeState,
				State: &airbyte.State{Data: []byte("{}"), Merge: true},
			}); err != nil {
				return err
			}
		} else if len(encDoc.state) != 0 {
			if err := enc.Encode(airbyte.Message{
				Type:  airbyte.MessageTypeState,
				State: &airbyte.State{Data: encDoc.state},
			}); err != nil {
				return err
			}
		}
	}
}

func listenForChanges(ctx context.Context, collection *firestore.CollectionRef, docsCh chan encDocument) error {
	var query = collection.Query
	var snapshotsIterator = query.Snapshots(ctx)

	log.WithFields(log.Fields{
		"collection": collection.ID,
	}).Info("listening for changes on collection")

	for {
		var snapshot, err = snapshotsIterator.Next()

		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		for _, change := range snapshot.Changes {
			if change.Kind == firestore.DocumentAdded || change.Kind == firestore.DocumentModified {
				log.WithFields(log.Fields{
					"kind": change.Kind,
					"id":   change.Doc.Ref.ID,
				}).Info("received change")

				var doc = change.Doc
				var data = doc.Data()
				data[firestore.DocumentID] = doc.Ref.ID
				docJson, err := json.Marshal(data)
				if err != nil {
					return err
				}
				log.WithFields(log.Fields{
					"doc": string(docJson),
					"id":  change.Doc.Ref.ID,
				}).Info("received change doc")
				docsCh <- encDocument{
					streamName: collection.ID,
					doc:        docJson,
				}
			}
		}
	}

	return nil
}

func fullScan(ctx context.Context, lastScan time.Time, scanInterval time.Duration, collection *firestore.CollectionRef, docsCh chan encDocument, tail bool) error {
	var nextScan = time.Until(lastScan.Add(scanInterval))
	// Wait until next scan
	time.Sleep(nextScan)

	log.WithFields(log.Fields{
		"collection": collection.ID,
	}).Info("full scan on collection")

	var query = collection.Query
	var docsIterator = query.Documents(ctx)

	var scanTime, _ = time.Now().MarshalText()
	for {
		var doc, err = docsIterator.Next()

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
		docsCh <- encDocument{
			streamName: collection.ID,
			doc:        docJson,
		}
	}

	var newState, err = json.Marshal(State{
		LastScan: string(scanTime),
	})
	if err != nil {
		return fmt.Errorf("marshalling state: %w", err)
	}
	docsCh <- encDocument{
		state: newState,
	}

	// If we are not tailing, we close the channel after the full scan
	// to allow the encoding routine to finish so the connector can exit
	if !tail {
		close(docsCh)
		return nil
	}

	return fullScan(ctx, time.Now(), scanInterval, collection, docsCh, tail)
}
