package filesource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"time"

	"github.com/estuary/flow/go/parser"
	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// discoverBudget is:
// * The maximum amount of time we'll list file objects for, and
// * The maximum amount of time we'll read sampled file object bytes for.
//
// These budgets are cumulative, so we can list for the full budget,
// and then read for the full budget. Note also that this is the duration
// we'll _read_ for, but if we're reading (say) compressed zip files we may
// produce a lot of input documents and spend yet more time crunching them
// within the parser and schema inference tools, so the _total_ time can still
// substantially exceed one minute (but empirically not, say, 10 minutes).
//
// NOTE(johnny): We'll probably rip all this out when we re-work when and how
// we offer schema inference...
const discoverBudget = time.Second * 30

// Number of file objects which we'll read in an attempt to get a representative
// schema sample.
const reservoirSize = 25

func (src Source) discoverSchema(args airbyte.DiscoverCmd) error {
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var backgroundCtx = context.Background()

	var pathRe *regexp.Regexp
	if r := conn.config.PathRegex(); r != "" {
		if pathRe, err = regexp.Compile(r); err != nil {
			return fmt.Errorf("building regex: %w", err)
		}
	}

	// Step 1: select up to N objects to read using reservoir sampling.
	// List until we read EOF or exceed our time budget.
	var reservoir = make([]ObjectInfo, 0, reservoirSize)

	var listCtx, listCancel = context.WithTimeout(backgroundCtx, discoverBudget)
	defer listCancel()

	listing, err := conn.store.List(listCtx, Query{
		Prefix:    conn.config.DiscoverRoot(),
		StartAt:   "",
		Recursive: true,
	})
	if err != nil {
		return fmt.Errorf("starting listing: %w", err)
	}
	logrus.Info("listing objects to select a sample set")

	for count := 0; true; count++ {
		var obj, err = listing.Next()
		if err == io.EOF {
			logrus.Info("finished listing all files")
			break
		} else if errors.Is(err, context.DeadlineExceeded) {
			logrus.Info("ran of out allotted time while listing files")
			break
		} else if err != nil {
			return fmt.Errorf("during listing: %w", err)
		} else if obj.IsPrefix {
			panic("implementation error (IsPrefix entry returned with Recursive: true Query)")
		}

		// Should we skip this file?
		if obj.Size == 0 {
			continue
		} else if pathRe != nil && !pathRe.MatchString(obj.Path) {
			continue
		}

		if len(reservoir) != cap(reservoir) {
			reservoir = append(reservoir, obj) // Initial fill of reservoir.
		} else if s := rand.Intn(count); s < len(reservoir) {
			reservoir[s] = obj // Replace an previous sampled object.
		}
	}

	if len(reservoir) == 0 {
		return fmt.Errorf("found no file objects to sample for their schema")
	}

	// Step 2: fetch up to N objects in parallel, feeding parsed documents
	// into a pipe. Concurrently dequeue documents from the pipe into the
	// inference engine.

	var pr, pw = io.Pipe()
	var group, groupCtx = errgroup.WithContext(backgroundCtx)

	var readCtx, readCancel = context.WithTimeout(groupCtx, discoverBudget)
	defer readCancel()

	for i := range reservoir {
		var obj = reservoir[i]
		logrus.WithField("path", obj.Path).Info("sampling from path")

		group.Go(func() error {
			var rr, _, err = conn.store.Read(readCtx, obj)
			if err != nil {
				return fmt.Errorf("reading path %s: %w", obj.Path, err)
			}
			defer rr.Close()

			// Configure the parser
			var parserCfg = new(parser.Config)
			if c := conn.config.ParserConfig(); c != nil {
				*parserCfg = c.Copy()
			}
			parserCfg = configureParser(parserCfg, obj)

			return parseToPipe(groupCtx, parserCfg, rr, pw)
		})
	}

	// Arrange for the pipe to close with the group's error (or nil on success).
	go func() {
		pw.CloseWithError(group.Wait())
	}()

	var inferOut, inferErr = inferSchema(backgroundCtx, pr)
	if inferErr != nil {
		return fmt.Errorf("during inference: %w", inferErr)
	}

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type: airbyte.MessageTypeCatalog,
		Catalog: &airbyte.Catalog{
			Streams: []airbyte.Stream{{
				Name:               conn.config.DiscoverRoot(),
				JSONSchema:         json.RawMessage(inferOut),
				SupportedSyncModes: airbyte.AllSyncModes,
				SourceDefinedPrimaryKey: [][]string{
					{"_meta", "file"},
					{"_meta", "offset"},
				},
			}},
		},
	})
}

func inferSchema(ctx context.Context, docs io.ReadCloser) (schema json.RawMessage, _ error) {
	defer docs.Close()

	var cmd = exec.CommandContext(ctx, "flow-schema-inference", "analyze")
	cmd.Stdin = docs
	cmd.Stderr = os.Stderr // Pass through stderr.
	var output, err = cmd.Output()

	logrus.WithFields(logrus.Fields{
		"cmd":    cmd.String(),
		"error":  err,
		"schema": string(schema),
	}).Debug("inferred document schema")

	return json.RawMessage(output), err
}

func parseToPipe(ctx context.Context, parserCfg *parser.Config, r io.Reader, w io.Writer) error {
	// Create the parser config file
	tmp, err := ioutil.TempFile("", "parser-config-*.json")
	if err != nil {
		return fmt.Errorf("creating parser config tmpfile: %w", err)
	}
	defer os.Remove(tmp.Name())

	if err = parserCfg.WriteToFile(tmp); err != nil {
		return fmt.Errorf("writing parser config: %w", err)
	}

	// Parse documents from the downloaded file.
	var buffer []byte
	err = parser.ParseStream(ctx, tmp.Name(), r, func(docs []json.RawMessage) error {
		logrus.WithFields(logrus.Fields{
			"docs": len(docs),
		}).Debug("parser produced documents")

		for _, doc := range docs {
			buffer = append(buffer, doc...)
			buffer = append(buffer, '\n')
		}
		if _, err := w.Write(buffer); err != nil {
			return fmt.Errorf("writing to pipe: %w", err)
		}
		buffer = buffer[:0]
		return nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("while parsing: %w", err)
	}
	return nil
}
