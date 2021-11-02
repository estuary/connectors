package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgconn"
	"github.com/sirupsen/logrus"
)

var (
	fIterations    = flag.Int("iterations", 100, "The number of stress-test iterations to run")
	fPostgresURI   = flag.String("postgres_uri", "postgres://flow:flow@localhost:5432/flow", "The database connection URI to use for traffic generation")
	fFillTime      = flag.Float64("fill_time", 2.0, "How long to spend writing changes before starting the capture")
	fHoldTime      = flag.Float64("hold_time", 2.0, "How long to spend writing changes after starting the capture")
	fDockerImage   = flag.String("image", "ghcr.io/estuary/source-postgres:dev", "The capture connector image")
	fCaptureConfig = flag.String("config", "{\"connectionURI\": \"postgres://flow:flow@localhost:5432/flow\", \"poll_timeout_seconds\": 5}", "The capture configuration JSON to use")
)

func main() {
	flag.Parse()
	var ctx = context.Background()
	for iter := 1; iter <= *fIterations; iter++ {
		if err := stressTest(ctx); err != nil {
			logrus.WithField("err", err).WithField("iter", iter).Fatal("test run failed")
		}
		logrus.WithField("iter", iter).Info("test run passed")
	}
}

func stressTest(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var seed = time.Now().UnixNano()
	logrus.WithField("seed", seed).Debug("seeding rng")
	rand.Seed(seed)

	var tableName = fmt.Sprintf("stresstest_%d", rand.Intn(99999999))
	var trafficGenerator = newTrafficGenerator()
	var outputValidator = &validator{}

	// TODO(wgd): Once we start implementing CDC connectors for other databases
	// this will need to be made generic, but it's not immediately obvious how
	// best to do that so we should wait until there's an actual need.
	sink, err := newPostgresSink(ctx, *fPostgresURI, tableName, trafficGenerator.PrimaryKey(), trafficGenerator.TableDef())
	if err != nil {
		return fmt.Errorf("error creating PostgreSQL sink: %w", err)
	}
	defer sink.Close(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		logrus.WithField("table", tableName).Info("starting traffic generator")
		for {
			select {
			case <-ctx.Done():
				logrus.Warn("traffic generator context cancelled")
				return
			default:
			}

			// TODO: Produce a clustor of one to five changes to be applied as a transaction?
			var evts = trafficGenerator.Transaction()
			if evts == nil {
				logrus.Debug("traffic generator shut down")
				return
			}
			if err := sink.Write(ctx, evts); err != nil && !errors.Is(err, context.Canceled) {
				// TODO(wgd): Use error channel to handle write errors?
				logrus.WithFields(logrus.Fields{"err": err, "evts": evts}).Fatal("error writing changes")
			}
		}
	}(ctx)

	// Build up a buffer of some changes in the database
	logrus.Debug("waiting for initial table fill")
	time.Sleep(time.Duration((*fFillTime) * float64(time.Second)))

	// Delete and recreate the replication slot to guarantee there is no "cheating"
	// by simply skipping the backfill scan and getting complete table data from the
	// replication log.
	if err := recreateReplicationSlot(ctx); err != nil {
		return fmt.Errorf("error recreating replication slot: %w", err)
	}

	var captureOutput bytes.Buffer
	capture, err := newCapture(*fDockerImage, *fCaptureConfig, tableName, &captureOutput)
	if err != nil {
		return fmt.Errorf("error starting capture: %w", err)
	}
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		if err := capture.Run(ctx, func(rec *airbyte.Record) error {
			// Ignore validation errors here, we'll check the accumulated
			// error list at close.
			_ = outputValidator.Validate(rec)
			return nil
		}); err != nil {
			logrus.WithField("err", err).Warn("capture error")
		}
		logrus.Info("capture complete")
	}(ctx)

	// Wait for the capture to catch up and more changes to occur
	logrus.Debug("waiting a while for capture to run")
	time.Sleep(time.Duration((*fHoldTime) * float64(time.Second)))

	// Close the traffic generator and let everything finish up
	logrus.Info("closing traffic generator")
	trafficGenerator.Close()
	logrus.Info("waiting for capture to complete")
	wg.Wait()

	if err := outputValidator.Close(); err != nil {
		var errs = outputValidator.Errors()
		for _, err := range errs {
			logrus.WithField("err", err).Error("validation error")
		}
		logrus.WithField("errors", len(errs)).Error("validation errors")

		var filename = fmt.Sprintf("/tmp/%s.txt", tableName)
		if err := os.WriteFile(filename, captureOutput.Bytes(), 0644); err != nil {
			logrus.WithField("path", filename).WithField("err", err).Warn("error writing history file")
		} else {
			logrus.WithField("path", filename).Info("wrote full history to file")
		}
		return err
	}
	return nil
}

func recreateReplicationSlot(ctx context.Context) error {
	// Okay, so to do this we need to connect to PostgreSQL and
	var replConnConfig, err = pgconn.ParseConfig(*fPostgresURI)
	if err != nil {
		return fmt.Errorf("error parsing connection config: %w", err)
	}
	replConnConfig.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	replConn.Exec(ctx, fmt.Sprintf(`DROP_REPLICATION_SLOT %s;`, "flow_slot")).Close() // Ignore failures because it probably doesn't exist
	if err := replConn.Exec(ctx, fmt.Sprintf(`CREATE_REPLICATION_SLOT %s LOGICAL pgoutput;`, "flow_slot")).Close(); err != nil {
		return fmt.Errorf("error creating replication slot: %w", err)
	}
	return nil
}
