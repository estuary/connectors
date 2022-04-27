package main

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type transactor struct {
	// Fields that the transactor is initialized with
	config          *config
	bigqueryClient  *bigquery.Client
	storageClient   *storage.Client
	materialization *pf.MaterializationSpec
	schemas         []bigquery.Schema

	// Mutable fields that evolves throughout a transactor lifecyle.
	checkpoint *BigQueryCheckPoint
	bindings   []*Binding
}

func RunTransactor(ctx context.Context, cfg *config, stream pm.Driver_TransactionsServer, open *pm.TransactionRequest_Open) error {
	bigqueryClient, err := cfg.BigQueryClient(ctx)
	if err != nil {
		return err
	}

	storageClient, err := cfg.StorageClient(ctx)
	if err != nil {
		return err
	}

	t := &transactor{
		config:          cfg,
		bigqueryClient:  bigqueryClient,
		storageClient:   storageClient,
		materialization: open.Materialization,
		checkpoint:      NewBigQueryCheckPoint(),
		bindings:        make([]*Binding, len(open.Materialization.Bindings)),
	}

	if open.DriverCheckpointJson != nil {
		if err := json.Unmarshal(open.DriverCheckpointJson, &t.checkpoint); err != nil {
			return fmt.Errorf("parsing driver config: %w", err)
		}
	}

	for i, sb := range open.Materialization.Bindings {
		binding, err := NewBinding(ctx, cfg, storageClient.Bucket(cfg.Bucket), sb)
		if err != nil {
			return fmt.Errorf("binding generation: %w", err)
		}

		t.bindings[i] = binding

		// Error here doesn't mean it's fatal, it just means the checkpoint didn't have
		// a checkpoint binding, which is a normal scenario if the last time the driver
		// stored a checkpoint, the binding didn't exist.
		if driverBinding, err := t.checkpoint.Binding(i); err == nil {
			binding.Reset(ctx, driverBinding.FilePath)
		}
	}

	if err := stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return pm.RunTransactions(stream, t, log.WithField("materialization", "bigquery"))
}

func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, priorAcknowledgedCh <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	select {
	case <-priorAcknowledgedCh:
	}

	for it.Next() {
		binding := t.bindings[it.Binding]

		job, err := binding.Job(it.Context(), t.bigqueryClient, binding.Queries.LoadSQL)

		if err != nil {
			return fmt.Errorf("running BigQuery job: %w", err)
		}

		bqit, err := job.Read(it.Context())
		if err != nil {
			return fmt.Errorf("load job read: %w", err)
		}

		// Load documents.
		for {
			// Fetch and decode from database.
			var fd flowDocument

			if err = bqit.Next(&fd); err == iterator.Done {
				break
			} else if err != nil {
				return fmt.Errorf("load row read: %w", err)
			}

			// Load the document by sending it back into Flow.
			if err = loaded(it.Binding, fd.Body); err != nil {
				return fmt.Errorf("load row loaded: %w", err)
			}
		}

	}
	return nil
}

func (t *transactor) Prepare(ctx context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	t.checkpoint = NewBigQueryCheckPoint()

	for _, binding := range t.bindings {
		name, err := randomString()
		if err != nil {
			return pf.DriverCheckpoint{}, err
		}

		binding.Reset(ctx, fmt.Sprintf("%s/%s", t.config.BucketPath, name))

		t.checkpoint.Bindings = append(t.checkpoint.Bindings, &DriverCheckPointBinding{
			FilePath: binding.FilePath(),
			Query:    binding.Queries.WriteSQL,
		})
	}

	jsn, err := json.Marshal(t.checkpoint)
	if err != nil {
		return pf.DriverCheckpoint{}, fmt.Errorf("creating checkpoint json: %w", err)
	}

	return pf.DriverCheckpoint{
		DriverCheckpointJson: jsn,
	}, nil
}

func (t *transactor) Store(it *pm.StoreIterator) error {
	for it.Next() {
		binding := t.bindings[it.Binding]
		binding.Store(binding.GenerateDocument(it.Key, it.Values, it.RawJSON))
	}

	return nil
}

func (t *transactor) Commit(ctx context.Context) error {
	for _, binding := range t.bindings {
		err := binding.Commit(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *transactor) Acknowledge(ctx context.Context) error {
	for i, cpBinding := range t.checkpoint.Bindings {
		binding := t.bindings[i]

		job, err := binding.Job(ctx, t.bigqueryClient, cpBinding.Query)

		if err != nil {
			return err
		}

		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}

		if status.Err() != nil {
			return fmt.Errorf("Store: job completed with error: %v", status.Err())
		}
	}

	return nil
}

func (t *transactor) Destroy() {
	t.bigqueryClient.Close()
	t.storageClient.Close()
}
