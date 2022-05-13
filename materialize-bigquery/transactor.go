package main

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	cloudStorage "cloud.google.com/go/storage"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
	log "github.com/sirupsen/logrus"
)

type transactor struct {
	// Fields that the transactor is initialized with
	config                 *config
	bigqueryClient         *bigquery.Client
	storageClient          *cloudStorage.Client
	materialization        *pf.MaterializationSpec
	materializationVersion string

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
		config:                 cfg,
		bigqueryClient:         bigqueryClient,
		storageClient:          storageClient,
		materialization:        open.Materialization,
		materializationVersion: open.Version,
		checkpoint:             NewBigQueryCheckPoint(),
		bindings:               make([]*Binding, len(open.Materialization.Bindings)),
	}

	if open.DriverCheckpointJson != nil {
		if err := json.Unmarshal(open.DriverCheckpointJson, &t.checkpoint); err != nil {
			return fmt.Errorf("parsing driver config: %w", err)
		}
	}

	for i, sb := range open.Materialization.Bindings {
		binding, err := NewBinding(ctx, storageClient.Bucket(cfg.Bucket), sb, BindingVersion(t.materializationVersion, i))
		if err != nil {
			return fmt.Errorf("binding generation: %w", err)
		}

		t.bindings[i] = binding
	}

	if err := stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return pm.RunTransactions(stream, t, log.WithField("materialization", "bigquery"))
}

func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, priorAcknowledgedCh <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	storages := map[string]*ExternalStorage{}
	externalData := map[string]bigquery.ExternalData{}
	group, ctx := errgroup.WithContext(it.Context())

	for it.Next() {
		binding := t.bindings[it.Binding]
		storage, ok := storages[binding.Table.Name()]

		if !ok {
			path := fmt.Sprintf("%s/%s", t.config.BucketPath, randomString())
			storage = NewExternalStorage(it.Context(), binding, path)
			// Need to relax the schema because it's a lookup on a different type of payload that
			// will only have a subset of the fields present.
			storage.ExternalDataConfig.Schema = storage.ExternalDataConfig.Schema.Relax()

			storages[binding.Name()] = storage
			externalData[binding.externalTableAlias] = storage.ExternalDataConfig
		}

		for idx, key := range it.Key {
			doc := make(map[string]interface{})

			field := binding.Table.Fields[idx]
			value, err := field.Render(key)
			if err != nil {
				return fmt.Errorf("generating SQL value: %w", err)
			}

			doc[field.Name()] = value
			storage.Store(doc)
		}
	}

	// This blocks until the previous pipeline is acknowledged
	<-priorAcknowledgedCh

	for i, b := range t.bindings {
		// We're copying the value in local scope because they will be used
		// in a Go routine and the variables i & b are
		// defined outside the loop, which would mean the values of i and b
		// would be undefined when those routines starts working
		var idx = i
		var binding = b

		storage, ok := storages[binding.Name()]

		// Only processing bindings that have a writer associated to them
		if !ok {
			continue
		}

		// Sending {bindings} number of queries to BigQuery. Since each binding
		// is isolated to a file and a table, the number of bytes to read is theoratically the same
		// if the SQL queries were merged into one. By running those queries separately, the connector
		// can start sending flow documents back to flow before all bindings are returned.
		group.Go(func() error {
			err := storage.Commit(ctx)
			if err != nil {
				return fmt.Errorf("commit writer for binding: %s. %w", binding.Name(), err)
			}

			defer func() {
				storage.Destroy(ctx)
			}()

			query := t.bigqueryClient.Query(binding.LoadSQL)
			query.DefaultDatasetID = t.config.Dataset
			query.Location = t.config.Region
			query.TableDefinitions = externalData

			job, err := query.Run(ctx)
			if err != nil {
				return fmt.Errorf("running BigQuery job: %w", err)
			}

			bqit, err := job.Read(it.Context())
			if err != nil {
				return fmt.Errorf("load job read: %w", err)
			}

			for {
				var fd flowDocument

				if err = bqit.Next(&fd); err == iterator.Done {
					break
				} else if err != nil {
					return fmt.Errorf("load row read: %w", err)
				}

				// Load the document by sending it back into Flow.
				if err = loaded(idx, fd.Body); err != nil {
					return fmt.Errorf("load row loaded: %w", err)
				}
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("one of the load call in the group failed: %w", err)
	}

	return nil
}

func (t *transactor) Prepare(ctx context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	t.checkpoint = NewBigQueryCheckPoint()

	for _, binding := range t.bindings {
		data, _ := json.Marshal(binding.Spec)

		cpBinding := DriverCheckPointBinding{
			FilePath:        fmt.Sprintf("%s/%s", t.config.BucketPath, randomString()),
			BindingSpecJson: json.RawMessage(data),
			Version:         binding.Version,
		}

		t.checkpoint.Bindings = append(t.checkpoint.Bindings, cpBinding)

		binding.SetExternalStorage(
			ctx,
			NewExternalStorage(ctx, binding, cpBinding.FilePath),
		)
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
	for _, cpBinding := range t.checkpoint.Bindings {
		var binding *Binding
		var err error

		for _, bd := range t.bindings {
			if bd.Version == cpBinding.Version {
				binding = bd
				break
			}
		}

		if binding == nil {
			var bindingSpec *pf.MaterializationSpec_Binding

			err := json.Unmarshal(cpBinding.BindingSpecJson, &bindingSpec)
			if err != nil {
				return fmt.Errorf("unmarshalling binding spec from driver's checkpoint: %w", err)
			}

			if binding, err = NewBinding(ctx, t.storageClient.Bucket(t.config.Bucket), bindingSpec, cpBinding.Version); err != nil {
				return fmt.Errorf("initializing binding: %w", err)
			}

			binding.SetExternalStorage(
				ctx,
				NewExternalStorage(ctx, binding, cpBinding.FilePath),
			)
		}

		query := t.bigqueryClient.Query(binding.InsertOrMergeSQL)
		query.DefaultDatasetID = t.config.Dataset
		query.Location = t.config.Region
		query.TableDefinitions = map[string]bigquery.ExternalData{binding.externalTableAlias: binding.ExternalStorage.ExternalDataConfig}
		job, err := query.Run(ctx)

		if err != nil {
			return err
		}

		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}

		if status.Err() != nil {
			return fmt.Errorf("store: job completed with error: %v", status.Err())
		}

		if err = binding.DestroyExternalStorage(ctx); err != nil {
			return fmt.Errorf("writer: failed to destroy: %w", err)
		}
	}

	return nil
}

func (t *transactor) Destroy() {
	t.bigqueryClient.Close()
	t.storageClient.Close()
}

var backtickWrapper = sqlDriver.BackticksWrapper()
