package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	abdrv "github.com/estuary/flow/go/capture/driver/airbyte"
	"github.com/estuary/protocols/airbyte"
	"github.com/sirupsen/logrus"
)

type captureConnector struct {
	dockerImage string
	config      string
	table       string
	logOut      io.Writer
}

func newCapture(dockerImage, config, table string, logOut io.Writer) (*captureConnector, error) {
	return &captureConnector{
		dockerImage: dockerImage,
		config:      config,
		table:       table,
		logOut:      logOut,
	}, nil
}

func (c *captureConnector) Run(ctx context.Context, cb func(rec *airbyte.Record) error) error {
	var logFields = logrus.Fields{
		"image":  c.dockerImage,
		"config": c.config,
		"table":  c.table,
	}

	logrus.WithFields(logFields).Info("running connector")

	var catalog = airbyte.ConfiguredCatalog{
		Streams: []airbyte.ConfiguredStream{
			{
				Stream: airbyte.Stream{
					Name:               c.table,
					JSONSchema:         json.RawMessage(`{}`),
					SupportedSyncModes: []airbyte.SyncMode{airbyte.SyncModeIncremental},
					Namespace:          "public",
				},
				SyncMode:            airbyte.SyncModeIncremental,
				DestinationSyncMode: airbyte.DestinationSyncModeAppend,
			},
		},
		Tail: false,
	}

	var invokeArgs = []string{
		"read",
		"--config",
		"/tmp/config.json",
		"--catalog",
		"/tmp/catalog.json",
	}
	var invokeFiles = map[string]interface{}{
		"config.json":  json.RawMessage(c.config),
		"catalog.json": catalog,
	}
	abdrv.RunConnector(ctx, c.dockerImage, "host", invokeArgs, invokeFiles,
		func(w io.Writer) error { return nil },
		abdrv.NewConnectorJSONOutput(
			func() interface{} { return new(airbyte.Message) },
			func(i interface{}) error {
				var bs, err = json.Marshal(i)
				if err != nil {
					logrus.WithField("err", err).Warn("error marshalling message")
				}
				fmt.Fprintf(c.logOut, "%s\n", string(bs))

				if rec := i.(*airbyte.Message); rec.Log != nil {
					logrus.WithFields(logFields).WithField("msg", rec.Log.Message).Debug("connector log")
				} else if rec.State != nil {
					logrus.WithField("msg", string(rec.State.Data)).Debug("state update")
				} else if rec.Record != nil {
					logrus.WithField("table", rec.Record.Stream).WithField("msg", string(rec.Record.Data)).Debug("record capture")
					if err := cb(rec.Record); err != nil {
						return err
					}
				} else {
					return fmt.Errorf("unexpected connector message: %v", rec)
				}
				return nil
			},
		),
	)
	return nil
}
