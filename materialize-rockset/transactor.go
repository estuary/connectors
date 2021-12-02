package materialize_rockset

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Rockset has several API endpoints for manipulating documents. We need to
// determine the action for each document and include it in the requests we make
// to that endpoint.
type operation = int

const (
	opNotFound       = -1
	opDocumentUpsert = iota
	opDocumentDelete
)

// A binding represents the relationship between a single Flow Collection and a single Rockset Collection.
type binding struct {
	spec *pf.MaterializationSpec_Binding
	// User-facing configuration settings for this binding.
	res *resource
	// Serialized documents yet to be upserted into the Rockset Collection.
	pendingUpserts *DocumentBuffer
	// Serialized documents yet to be deleted from the Rockset Collection.
	pendingDeletions *DocumentBuffer
}

func NewBinding(spec *pf.MaterializationSpec_Binding, res *resource) *binding {
	return &binding{
		spec:             spec,
		res:              res,
		pendingUpserts:   NewDocumentBuffer(),
		pendingDeletions: NewDocumentBuffer(),
	}
}

func (b *binding) rocksetWorkspace() string {
	return b.res.Workspace
}

func (b *binding) rocksetCollection() string {
	return b.res.Collection
}

// Calculates how many concurrent goroutines will be used to make API calls while remaining under the resources's maxBatchSize.
func (b *binding) concurrencyFactor(numDocuments int) int {
	if numDocuments < b.res.MaxBatchSize {
		return 1
	} else {
		return numDocuments / b.res.MaxBatchSize
	}
}

type transactor struct {
	// TODO: ctx will be removed when the protocol updates land.
	ctx      context.Context
	config   *config
	client   *client
	bindings []*binding
}

// pm.Transactor
func (t *transactor) Load(it *pm.LoadIterator, priorCommittedCh <-chan struct{}, priorAcknowledgedCh <-chan struct{}, loaded func(binding int, doc json.RawMessage) error) error {
	panic("Rockset is not transactional - Load should never be called")
}

// pm.Transactor
func (t *transactor) Prepare(ctx context.Context, msg pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	// Nothing to prepare
	return pf.DriverCheckpoint{}, nil
}

// pm.Transactor
func (t *transactor) Store(it *pm.StoreIterator) error {
	for it.Next() {
		select {
		case <-t.ctx.Done():
			return fmt.Errorf("transactor context cancelled")
		default:
			// Keep going!
		}

		var b *binding = t.bindings[it.Binding]

		var op operation = opNotFound
		if t.config.ChangeIndicator == "" {
			op = opDocumentUpsert
		} else {
			changeType := findStringField(b, &it.Key, &it.Values, t.config.ChangeIndicator)
			switch changeType {
			case "Insert":
				op = opDocumentUpsert
			case "Update":
				// Sending an "AddDocument" request for a document which already
				// exists will result in the previous document being completely
				// overwritten. Since the CdcEvents contain the full document, this
				// is the exact desired behavior.
				op = opDocumentUpsert
			case "Delete":
				op = opDocumentDelete
			default:
				return fmt.Errorf("unrecognized change indicator field value: %s=`%s`", t.config.ChangeIndicator, changeType)
			}
		}

		if op == opDocumentUpsert {
			if err := t.storeUpsertOperations(b, &it.Key, &it.Values); err != nil {
				return err
			}

		} else if op == opDocumentDelete {
			if err := t.storeDeletionOperation(b, &it.Key); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unrecognized operation: %d", op)
		}
	}

	return nil
}

// pm.Transactor
func (t *transactor) Commit(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	for _, binding := range t.bindings {
		b := binding
		group.Go(func() error {
			return commitCollection(ctx, t, b)
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (t *transactor) Acknowledge(context.Context) error {
	// Nothing to do during acknowledgement
	return nil
}

// pm.Transactor
func (t *transactor) Destroy() {
	// Nothing to clean up
}

func findStringField(b *binding, keys *tuple.Tuple, values *tuple.Tuple, targetFieldName string) string {
	// Look for the field in the document's keys.
	for i, value := range *keys {
		var propName = b.spec.FieldSelection.Keys[i]
		if v, ok := value.(string); ok && propName == targetFieldName {
			return v
		}
	}

	// Look for the field in the document's other values.
	for i, value := range *values {
		var propName = b.spec.FieldSelection.Values[i]
		if v, ok := value.(string); ok && propName == targetFieldName {
			return v
		}
	}

	// Not found
	return ""
}

func (t *transactor) storeUpsertOperations(b *binding, keys *tuple.Tuple, values *tuple.Tuple) error {
	var document = make(map[string]interface{})

	// Add the `_id` field to the document. This is required by Rockset.
	document["_id"] = base64.RawStdEncoding.EncodeToString(keys.Pack())

	// Add the keys to the document.
	for i, value := range *keys {
		var propName = b.spec.FieldSelection.Keys[i]
		document[propName] = value
	}

	// Add the non-keys to the document.
	for i, value := range *values {
		var propName = b.spec.FieldSelection.Values[i]

		if raw, ok := value.([]byte); ok {
			document[propName] = json.RawMessage(raw)
		} else {
			document[propName] = value
		}
	}

	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to serialize the addition document: %w", err)
	}

	b.pendingUpserts.Push(jsonDoc)

	return nil
}

func (t *transactor) storeDeletionOperation(b *binding, key *tuple.Tuple) error {
	var document = make(map[string]interface{})
	document["_id"] = base64.RawStdEncoding.EncodeToString(key.Pack())

	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to serialize the deletion document: %w", err)
	}

	b.pendingDeletions.Push(jsonDoc)

	return nil
}

func commitCollection(ctx context.Context, t *transactor, b *binding) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("transactor context cancelled")
	default:
		// Keep going!
	}

	totalUpserts := b.pendingUpserts.Len()
	totalDeletions := b.pendingDeletions.Len()

	defer logElapsedTime(time.Now(), fmt.Sprintf("commit completed: %d documents added %d documents deleted", totalUpserts, totalDeletions))

	numWorkers := workerPoolSize(t.config.MaxConcurrentRequests, totalUpserts+totalDeletions, b.res.MaxBatchSize)
	workQueue, errors := newWorkerPool(ctx, numWorkers, func(ctx context.Context, j job, worker int) error {
		switch j.op {
		case opDocumentUpsert:
			return t.client.AddDocuments(ctx, b.rocksetWorkspace(), b.rocksetCollection(), j.data)
		case opDocumentDelete:
			return t.client.DeleteDocuments(ctx, b.rocksetWorkspace(), b.rocksetCollection(), j.data)
		default:
			return fmt.Errorf("unrecognized operation type: %v", j.op)
		}
	})

	go func() {
		defer close(workQueue)

		for _, batch := range b.pendingUpserts.SplitN(numWorkers) {
			if len(batch) > 0 {
				workQueue <- job{op: opDocumentUpsert, data: batch}
			}
		}
		for _, batch := range b.pendingDeletions.SplitN(numWorkers) {
			if len(batch) > 0 {
				workQueue <- job{op: opDocumentDelete, data: batch}
			}
		}
	}()

	err := errors.Wait()
	b.pendingUpserts.Clear()
	b.pendingDeletions.Clear()

	if err != nil {
		return fmt.Errorf("committing documents to rockset: %w", err)
	}

	return nil
}

type job struct {
	op   operation
	data []json.RawMessage
}

func workerPoolSize(maxConcurrency int, totalOperations int, maxBatchSize int) int {
	return clamp(1, maxConcurrency, totalOperations/maxBatchSize)
}

func newWorkerPool(ctx context.Context, poolSize int, doWork func(context.Context, job, int) error) (chan<- job, *errgroup.Group) {
	jobs := make(chan job, poolSize)
	group, ctx := errgroup.WithContext(ctx)

	for i := 0; i < poolSize; i++ {
		i := i
		group.Go(func() error {
			for job := range jobs {
				if err := doWork(ctx, job, i); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return jobs, group
}

func logElapsedTime(start time.Time, msg string) {
	elapsed := time.Since(start)
	log.Infof("%s,%f", msg, elapsed.Seconds())
}

func clamp(min int, max int, n int) int {
	if max <= min {
		panic("max must be larger than min")
	}

	if n > max {
		return max
	} else if n < min {
		return min
	} else {
		return n
	}
}
