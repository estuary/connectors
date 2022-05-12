package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/sirupsen/logrus"
)

// AirbyteMain implements the main function for an Airbyte connector, given
// a function which parses the config file and returns a concrete implementation
// of the Database interface.
func AirbyteMain(spec airbyte.Spec, init func(airbyte.ConfigFile) (Database, error)) {
	airbyte.RunMain(spec,
		func(args airbyte.CheckCmd) error {
			var ctx = context.Background()
			var db, err = init(args.ConfigFile)
			if err != nil {
				return fmt.Errorf("error initializing database interface: %w", err)
			}

			var result = &airbyte.ConnectionStatus{Status: airbyte.StatusSucceeded}
			if _, err := DiscoverCatalog(ctx, db); err != nil {
				result.Status = airbyte.StatusFailed
				result.Message = err.Error()
			}
			return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
				Type:             airbyte.MessageTypeConnectionStatus,
				ConnectionStatus: result,
			})
		},
		func(args airbyte.DiscoverCmd) error {
			var ctx = context.Background()
			var db, err = init(args.ConfigFile)
			if err != nil {
				return fmt.Errorf("error initializing database interface: %w", err)
			}

			catalog, err := DiscoverCatalog(ctx, db)
			if err != nil {
				return err
			}

			// Filter the watermarks table out of the discovered catalog before output
			// It's basically never useful to capture so we shouldn't suggest it.
			var watermarkStreamID = db.WatermarksTable()
			var filteredStreams = []airbyte.Stream{} // Empty discovery must result in `[]` rather than `null`
			for _, stream := range catalog.Streams {
				var streamID = JoinStreamID(stream.Namespace, stream.Name)
				if streamID != watermarkStreamID {
					filteredStreams = append(filteredStreams, stream)
				} else {
					logrus.WithFields(logrus.Fields{
						"filtered":   streamID,
						"watermarks": watermarkStreamID,
					}).Debug("filtered watermarks table from discovery")
				}
			}
			catalog.Streams = filteredStreams

			return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
				Type:    airbyte.MessageTypeCatalog,
				Catalog: catalog,
			})
		},
		func(args airbyte.ReadCmd) error {
			var ctx = context.Background()
			var db, err = init(args.ConfigFile)
			if err != nil {
				return fmt.Errorf("error initializing database interface: %w", err)
			}

			var state = &PersistentState{Streams: make(map[string]TableState)}
			if args.StateFile != "" {
				if err := args.StateFile.Parse(state); err != nil {
					return fmt.Errorf("unable to parse state file: %w", err)
				}
			}

			var catalog = new(airbyte.ConfiguredCatalog)
			if err := args.CatalogFile.Parse(catalog); err != nil {
				return fmt.Errorf("unable to parse catalog: %w", err)
			}

			return RunCapture(ctx, db, catalog, state, json.NewEncoder(os.Stdout))
		})
}

// RunCapture is the top level of the database capture process. It  is responsible for opening DB
// connections, scanning tables, and then streaming replication events until shutdown conditions
// (if any) are met.
func RunCapture(ctx context.Context, db Database, catalog *airbyte.ConfiguredCatalog, state *PersistentState, dest MessageOutput) error {
	if err := db.Connect(ctx); err != nil {
		return err
	}
	defer db.Close(ctx)

	var c = Capture{
		Catalog:  catalog,
		State:    state,
		Encoder:  dest,
		Database: db,
	}
	return c.Run(ctx)
}
