package filesource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/estuary/connectors/schema_inference"
	"github.com/estuary/flow/go/parser"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
)

// How many documents should we read from an individual file during discovery.
const DISCOVER_DOC_LIMIT = 40000

// Baseline document schema for resource streams we discover if we fail
// during the schema-inference step.
var DISCOVER_FALLBACK_SCHEMA = schema_inference.Schema(`{
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
	}`)

func (src Source) Discover(args airbyte.DiscoverCmd) error {
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var ctx = context.Background()
	var root = conn.config.DiscoverRoot()

	schema, err := discoverSchema(ctx, conn, root)
	if err != nil {
		return err
	}

	return writeCatalogMessage(newStream(root, schema))
}

func discoverSchema(ctx context.Context, conn *connector, streamName string) (schema_inference.Schema, error) {
	var logEntry = log.WithField("stream", streamName)

	docCh, docCount, err := peekDocuments(ctx, logEntry, conn, streamName)
	if err != nil {
		return nil, fmt.Errorf("schema discovery for stream `%s` failed: %w", streamName, err)
	} else if docCount == 0 {
		return DISCOVER_FALLBACK_SCHEMA, nil
	}

	logEntry.WithField("documentCount", docCount).Info("Got documents for stream")

	schema, err := schema_inference.Run(ctx, logEntry, docCh)
	if err != nil {
		return nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	return schema, nil
}

func peekDocuments(ctx context.Context, logEntry *log.Entry, conn *connector, root string) (<-chan schema_inference.Document, uint, error) {
	var (
		err       error
		listing   Listing
		pathRegex *regexp.Regexp
		fileCount uint              = 0
		documents *documentSampling = NewDocumentSampling(DISCOVER_DOC_LIMIT)
	)
	pathRegex, err = initPathRegex(conn.config.PathRegex())
	if err != nil {
		return nil, 0, err
	}

	listing, err = conn.store.List(ctx, Query{Prefix: root, StartAt: "", Recursive: true})
	if err != nil {
		return nil, fileCount, fmt.Errorf("listing bucket: %w", err)
	}

	for {
		obj, err := listing.Next()
		var logEntry = logEntry.WithField("fileCount", fileCount).WithField("path", obj.Path)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fileCount, fmt.Errorf("reading bucket listing: %w", err)
		} else if pathRegex != nil && !pathRegex.MatchString(obj.Path) {
			logEntry.Trace("Skipping path that does not match PathRegex")
			continue
		} else if obj.IsPrefix {
			logEntry.Trace("Skipping prefix")
			continue
		} else if obj.Size == 0 {
			logEntry.Trace("Skipping empty file")
			continue
		}

		fileCount++
		logEntry.Debug("Discovered object")

		if documents.open {
			err = peekAtFile(ctx, conn, obj, documents)
			if err != nil {
				return nil, 0, fmt.Errorf("reading file `%s`: %w", obj.Path, err)
			}
		} else {
			logEntry.Info("Reached sampling limit")
			break
		}
	}

	documents.Close()
	logEntry.WithField("fileCount", fileCount).Info("Bucket sampling successful")

	return documents.ch, documents.count, nil
}

func peekAtFile(ctx context.Context, conn *connector, file ObjectInfo, documents *documentSampling) error {
	var logEntry = log.WithField("path", file.Path)
	logEntry.Infof("Peeking at file")

	// Download file
	rr, file, err := conn.store.Read(ctx, file)
	if err != nil {
		return fmt.Errorf("Failed to download file: %w", err)
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
		return fmt.Errorf("Failed creating parser config: %w", err)
	}
	defer os.Remove(tmp.Name())

	if err = cfg.WriteToFile(tmp); err != nil {
		return fmt.Errorf("Failed writing parser config: %w", err)
	}

	// Parse documents from the downloaded file
	err = parser.ParseStream(ctx, tmp.Name(), rr, func(docs []json.RawMessage) error {
		logEntry.WithField("docsSeen", documents.count).WithField("count", len(docs)).Debug("Parser produced more documents")

		for _, doc := range docs {
			if documents.Add(copyJson(doc)) {
				continue
			} else {
				// The document channel is now full. We've seen enough.
				return closeParser
			}
		}

		if documents.open {
			return nil
		} else {
			return closeParser
		}
	})
	if err != nil && err != closeParser {
		return fmt.Errorf("Failed parsing: %w", err)
	}

	logEntry.Info("Done peeking at file")
	return nil
}

type documentSampling struct {
	capacity uint
	ch       chan schema_inference.Document
	count    uint
	open     bool
}

func NewDocumentSampling(capacity uint) *documentSampling {
	return &documentSampling{
		capacity: capacity,
		ch:       make(chan schema_inference.Document, capacity),
		count:    0,
		open:     true,
	}
}

func (d *documentSampling) Add(doc schema_inference.Document) (more bool) {
	if d.open {
		d.count++
		d.ch <- doc

		if d.count >= d.capacity {
			d.Close()
		}
	}
	return d.open
}

func (d *documentSampling) Close() {
	if d.open {
		d.open = false
		close(d.ch)
	}
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

func newStream(name string, discoveredSchema schema_inference.Schema) airbyte.Stream {
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

func initPathRegex(regexStr string) (*regexp.Regexp, error) {
	if regexStr == "" {
		return nil, nil
	}

	pathRegex, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, fmt.Errorf("invalid PathRegex: %w", err)
	}
	return pathRegex, nil
}
