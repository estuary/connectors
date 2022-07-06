package filesource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sync"

	"github.com/estuary/flow/go/parser"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// How many files should we sample to run inference against.
	discoverReadLimit = 10
	// How many documents should we read from an individual file during discovery.
	discoverPeekDocuments = 100
	// Baseline document schema for resource streams we discover if we fail
	// during the schema-inference step.
	discoverFallbackSchema = `{
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
	var logEntry = log.WithField("stream", root)

	documents, err := sampleDocuments(ctx, logEntry, conn, root)
	if err != nil {
		return fmt.Errorf("sampling documents from bucket: %w", err)
	}

	schema, err := runInference(ctx, logEntry, documents)
	if err != nil {
		// Failing schema inference isn't a fatal error. It just means we can't
		// auto-generate a schema from the files in this bucket and it's up to
		// the user to craft their own schema.
		log.WithField("err", err).Error("failed to infer schema from documents")
		schema = json.RawMessage(discoverFallbackSchema)
	}

	return writeCatalogMessage(newStream(root, schema))
}

func sampleDocuments(ctx context.Context, logEntry *log.Entry, conn *connector, root string) (<-chan json.RawMessage, error) {
	var (
		err         error
		listing     Listing
		waitGroup   *sync.WaitGroup      = new(sync.WaitGroup)
		documents   chan json.RawMessage = make(chan json.RawMessage, discoverReadLimit*discoverPeekDocuments)
		objectCount int                  = 0
	)

	listing, err = conn.store.List(ctx, Query{Prefix: root, StartAt: "", Recursive: true})
	if err != nil {
		return nil, fmt.Errorf("listing bucket: %w", err)
	}

	for {
		obj, err := listing.Next()
		var logEntry = logEntry.WithField("objCount", objectCount).WithField("path", obj.Path)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading bucket listing: %w", err)
		} else if obj.IsPrefix {
			logEntry.Trace("Skipping prefix")
			continue
		} else if obj.Size == 0 {
			logEntry.Trace("Skipping empty file")
			continue
		}

		objectCount++
		logEntry.Debug("Discovered object")

		if objectCount < discoverReadLimit {
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

	return documents, nil
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
			if docsSeen < discoverPeekDocuments {
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

func runInference(ctx context.Context, logEntry *log.Entry, docsCh <-chan json.RawMessage) (json.RawMessage, error) {
	var (
		err      error
		errGroup *errgroup.Group
		schema   = json.RawMessage{}
		cmd      *exec.Cmd
		stdin    io.WriteCloser
		stdout   io.ReadCloser
	)

	errGroup, ctx = errgroup.WithContext(ctx)
	cmd = exec.CommandContext(ctx, "flow-schema-inference", "analyze")

	stdin, err = cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdin: %w", err)
	}
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to open stdout: %w", err)
	}

	errGroup.Go(func() error {
		defer stdin.Close()
		encoder := json.NewEncoder(stdin)

		for doc := range docsCh {
			if err := encoder.Encode(doc); err != nil {
				return fmt.Errorf("failed to encode document: %w", err)
			}
		}
		logEntry.Info("runInference: Done sending documents")

		return nil
	})

	errGroup.Go(func() error {
		defer stdout.Close()
		decoder := json.NewDecoder(stdout)

		if err := decoder.Decode(&schema); err != nil {
			return fmt.Errorf("failed to decode schema, %w", err)
		}
		logEntry.Debug("runInference: Done reading schema")

		return nil
	})

	logEntry.Info("runInference: launching")
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to run flow-schema-inference: %w", err)
	}

	errGroup.Go(cmd.Wait)

	err = errGroup.Wait()
	if err != nil {
		return nil, err
	}

	return schema, nil
}

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
