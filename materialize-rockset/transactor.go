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

		if err := t.storeUpsertOperations(b, &it.Key, &it.Values); err != nil {
			return err
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

func commitCollection(ctx context.Context, t *transactor, b *binding) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("transactor context cancelled")
	default:
		// Keep going!
	}

	totalUpserts := b.pendingUpserts.Len()

	defer logElapsedTime(time.Now(), fmt.Sprintf("commit completed: %d documents added", totalUpserts))

	numWorkers := workerPoolSize(t.config.MaxConcurrentRequests, totalUpserts, b.res.MaxBatchSize)
	workQueue, errors := newWorkerPool(ctx, numWorkers, func(ctx context.Context, data []json.RawMessage, worker int) error {
		return t.client.AddDocuments(ctx, b.rocksetWorkspace(), b.rocksetCollection(), data)
	})

	go func() {
		defer close(workQueue)

		for _, batch := range b.pendingUpserts.SplitN(numWorkers) {
			if len(batch) > 0 {
				workQueue <- batch
			}
		}
	}()

	err := errors.Wait()
	b.pendingUpserts.Clear()

	if err != nil {
		return fmt.Errorf("committing documents to rockset: %w", err)
	}

	return nil
}

func workerPoolSize(maxConcurrency int, totalOperations int, maxBatchSize int) int {
	return clamp(1, maxConcurrency, totalOperations/maxBatchSize)
}

func newWorkerPool(ctx context.Context, poolSize int, doWork func(context.Context, []json.RawMessage, int) error) (chan<- []json.RawMessage, *errgroup.Group) {
	jobs := make(chan []json.RawMessage, poolSize)
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
