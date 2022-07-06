package filesource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/estuary/connectors/schema_inference"
	"github.com/estuary/flow/go/parser"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
)

const (
	// How many files should we sample to run inference against.
	DISCOVER_FILE_READ_LIMIT = 10
	// How many documents should we read from an individual file during discovery.
	DISCOVER_MAX_DOCS_PER_FILE = 100
	// Baseline document schema for resource streams we discover if we fail
	// during the schema-inference step.
	DISCOVER_FALLBACK_SCHEMA = `{
		"type": "object",
		"properties": {
			"_meta": {
				"type": "object",
				"properties": {
					"file": { "type": "string" },
					"offset": {
						"type": "integer",
						"minimum": 0
					}
				},
				"required": ["file", "offset"]
			}
		},
		"required": ["_meta"]
	}`
)

func (src Source) Discover(args airbyte.DiscoverCmd) error {
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var ctx = context.Background()
	var root = conn.config.DiscoverRoot()

	schema, err := schema_inference.Run(ctx, root, func(ctx context.Context, logEntry *log.Entry) (<-chan json.RawMessage, uint, error) {
		return peekDocuments(ctx, logEntry, conn, root)
	})

	if err != nil {
		// Failing schema inference isn't a fatal error. It just means we can't
		// auto-generate a schema from the files in this bucket and it's up to
		// the user to craft their own schema.
		log.WithField("err", err).Error("failed to infer schema from documents")
		schema = json.RawMessage(DISCOVER_FALLBACK_SCHEMA)
	}

	return writeCatalogMessage(newStream(root, schema))
}

func peekDocuments(ctx context.Context, logEntry *log.Entry, conn *connector, root string) (<-chan json.RawMessage, uint, error) {
	var (
		err         error
		listing     Listing
		waitGroup   *sync.WaitGroup      = new(sync.WaitGroup)
		documents   chan json.RawMessage = make(chan json.RawMessage, DISCOVER_FILE_READ_LIMIT*DISCOVER_MAX_DOCS_PER_FILE)
		objectCount uint                 = 0
	)

	listing, err = conn.store.List(ctx, Query{Prefix: root, StartAt: "", Recursive: true})
	if err != nil {
		return nil, objectCount, fmt.Errorf("listing bucket: %w", err)
	}

	for {
		obj, err := listing.Next()
		var logEntry = logEntry.WithField("objCount", objectCount).WithField("path", obj.Path)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, objectCount, fmt.Errorf("reading bucket listing: %w", err)
		} else if obj.IsPrefix {
			logEntry.Trace("Skipping prefix")
			continue
		} else if obj.Size == 0 {
			logEntry.Trace("Skipping empty file")
			continue
		}

		objectCount++
		logEntry.Debug("Discovered object")

		if objectCount < DISCOVER_FILE_READ_LIMIT {
			waitGroup.Add(1)
			go peekAtFile(ctx, waitGroup, conn, obj, documents)
		} else {
			logEntry.Info("Reached sampling limit")
			break
		}
	}
	waitGroup.Wait()
	close(documents)
	logEntry.WithField("objectCount", objectCount).Info("bucket sampling successful")

	return documents, objectCount, nil
}

func peekAtFile(ctx context.Context, waitGroup *sync.WaitGroup, conn *connector, file ObjectInfo, docCh chan<- json.RawMessage) {
	defer waitGroup.Done()

	var logEntry = log.WithField("path", file.Path)
	logEntry.Infof("Peeking at file")

	// Download file
	rr, file, err := conn.store.Read(ctx, file)
	if err != nil {
		logEntry.WithField("err", err).Error("Failed to download file")
		return
	}
	defer rr.Close()

	// Configure the parser
	var cfg = new(parser.Config)
	if c := conn.config.ParserConfig(); c != nil {
		*cfg = c.Copy()
	}
	cfg = configureParser(cfg, file)

	// Create the parser config file
	tmp, err := ioutil.TempFile("", "parser-config-*.json")
	if err != nil {
		logEntry.WithField("err", err).Error("Failed creating parser config")
		return
	}
	defer os.Remove(tmp.Name())

	if err = cfg.WriteToFile(tmp); err != nil {
		logEntry.WithField("err", err).Error("Failed writing parser config")
		return
	}

	// Parse documents from the downloaded file
	var docsSeen = 0
	err = parser.ParseStream(ctx, tmp.Name(), rr, func(docs []json.RawMessage) error {
		logEntry.WithField("docsSeen", docsSeen).WithField("count", len(docs)).Debug("Parser produced more documents")

		for _, doc := range docs {
			docsSeen++
			if docsSeen < DISCOVER_MAX_DOCS_PER_FILE {
				docCh <- copyJson(doc)
			} else {
				return closeParser
			}
		}
		return nil
	})
	if err != nil && err != closeParser {
		logEntry.WithField("err", err).Error("Failed parsing")
		return
	}

	logEntry.Info("Done peeking at file")
}

var closeParser = fmt.Errorf("caller closed")

func copyJson(doc json.RawMessage) json.RawMessage {
	var copy = append(json.RawMessage(nil), doc...)
	return json.RawMessage(copy)
}

func writeCatalogMessage(streams ...airbyte.Stream) error {
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type: airbyte.MessageTypeCatalog,
		Catalog: &airbyte.Catalog{
			Streams: streams,
		},
	})
}

func newStream(name string, discoveredSchema json.RawMessage) airbyte.Stream {
	return airbyte.Stream{
		Name:               name,
		JSONSchema:         discoveredSchema,
		SupportedSyncModes: airbyte.AllSyncModes,
		SourceDefinedPrimaryKey: [][]string{
			{"_meta", "file"},
			{"_meta", "offset"},
		},
	}
}
