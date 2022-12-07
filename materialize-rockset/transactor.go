package materialize_rockset

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	rockset "github.com/rockset/rockset-go-client"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// A binding represents the relationship between a single Flow Collection and a single Rockset Collection.
type binding struct {
	spec *pf.MaterializationSpec_Binding
	// User-facing configuration settings for this binding.
	res       *resource
	addDocsCh chan<- map[string]interface{}
}

func NewBinding(spec *pf.MaterializationSpec_Binding, res *resource) *binding {
	return &binding{
		spec: spec,
		res:  res,
	}
}

func (b *binding) rocksetWorkspace() string {
	return b.res.Workspace
}

func (b *binding) rocksetCollection() string {
	return b.res.Collection
}

type transactor struct {
	config   *config
	client   *rockset.RockClient
	bindings []*binding
	errGroup *errgroup.Group
}

// awaitAllRocksetCollectionsReady will block until all the Rockset collections named in the bindings
// are in `READY` status and have completed any pending bulk ingestions. Specifically, this waits until the
// number of objects in the bucket (as reported by rockset) and the number of successfully imported objects
// is the same.
func (t *transactor) awaitAllRocksetCollectionsReady(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, b := range t.bindings {
		// Assignment here is required because we capture this in the goroutine function
		var binding = b
		group.Go(func() error {
			var integration = ""
			if binding.res.InitializeFromS3 != nil {
				integration = binding.res.InitializeFromS3.Integration
			}
			var err = awaitCollectionReady(
				ctx,
				t.client,
				binding.res.Workspace,
				binding.res.Collection,
				integration,
			)
			if err != nil {
				return fmt.Errorf("awaiting readiness of rockset collection '%s': %w", binding.res.Collection, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// pm.Transactor
func (t *transactor) Load(it *pm.LoadIterator, priorCommittedCh <-chan struct{}, priorAcknowledgedCh <-chan struct{}, loaded func(binding int, doc json.RawMessage) error) error {
	panic("Rockset is not transactional - Load should never be called")
}

// pm.Transactor
func (t *transactor) Prepare(ctx context.Context, msg pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	// There's nothing in particular to be done here, but what we're _not_ doing is notable.  We return an empty driver
	// checkpoint here, which may clear out a previous driver checkpoint from the materialize-s3-parquet connector, if
	// the user had used that to backfill data.
	return pf.DriverCheckpoint{}, nil
}

// max number of documents to send with each request
const storeBatchSize = 256

// pm.Transactor
func (t *transactor) Store(it *pm.StoreIterator) error {
	var errGroup, ctx = errgroup.WithContext(it.Context())
	// Store the error group so we can await it during commit
	t.errGroup = errGroup

	for it.Next() {
		var b *binding = t.bindings[it.Binding]
		// Lazily initialize the goroutine that sends the documents to rockset.
		if b.addDocsCh == nil {
			var addDocsCh = make(chan map[string]interface{}, storeBatchSize*2)
			b.addDocsCh = addDocsCh
			errGroup.Go(func() error {
				return t.sendAllDocuments(ctx, b, addDocsCh)
			})
			logrus.WithFields(logrus.Fields{
				"rocksetCollection": b.rocksetCollection(),
				"rocksetWorkspace":  b.rocksetWorkspace(),
			}).Debug("Started AddDocuments background worker")
		}

		var doc = buildDocument(b, it.Key, it.Values)
		select {
		case b.addDocsCh <- doc:
			continue
		case <-ctx.Done():
			err := ctx.Err()

			if errors.Is(err, context.Canceled) {
				err = errGroup.Wait()
			}

			return err
		}
	}
	return nil
}

// pm.Transactor
func (t *transactor) Commit(ctx context.Context) error {
	for _, binding := range t.bindings {
		if binding.addDocsCh != nil {
			close(binding.addDocsCh)
			binding.addDocsCh = nil
			logrus.WithFields(logrus.Fields{
				"rocksetCollection": binding.rocksetCollection(),
				"rocksetWorkspace":  binding.rocksetWorkspace(),
			}).Debug("Closed AddDocuments channel")
		}
	}
	if err := t.errGroup.Wait(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	logrus.Debug("Commit successful")
	return nil
}

// pm.Transactor
func (t *transactor) Acknowledge(ctx context.Context) error {
	// ack is a no-op since we ensure writes are durable in Commit
	return nil
}

// pm.Transactor
func (t *transactor) Destroy() {
	// Nothing to clean up
}

func buildDocument(b *binding, keys, values tuple.Tuple) map[string]interface{} {
	var document = make(map[string]interface{})

	// Add the `_id` field to the document. This is required by Rockset.
	document["_id"] = base64.RawStdEncoding.EncodeToString(keys.Pack())

	// Add the keys to the document.
	for i, value := range keys {
		var propName = b.spec.FieldSelection.Keys[i]
		document[propName] = value
	}

	// Add the non-keys to the document.
	for i, value := range values {
		var propName = b.spec.FieldSelection.Values[i]

		if raw, ok := value.([]byte); ok {
			document[propName] = json.RawMessage(raw)
		} else {
			document[propName] = value
		}
	}
	return document
}

func (t *transactor) sendAllDocuments(ctx context.Context, b *binding, addDocsCh <-chan map[string]interface{}) error {
	var docs = make([]interface{}, 0, storeBatchSize)

	var docCount = 0
	for addDocsCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case doc, ok := <-addDocsCh:
			if ok {
				docCount++
				docs = append(docs, doc)
			} else {
				logrus.WithFields(logrus.Fields{
					"rocksetCollection": b.rocksetCollection(),
				}).Debug("store channel closed")
				// Set channel to nil so that we don't try to read from it again
				addDocsCh = nil
			}
		}
		if len(docs) == storeBatchSize {
			if err := t.sendReq(ctx, b, docs); err != nil {
				return err
			}
			docs = docs[:0]
		}
	}
	if len(docs) > 0 {
		if err := t.sendReq(ctx, b, docs); err != nil {
			return err
		}
	}
	logrus.WithFields(logrus.Fields{
		"rocksetCollection": b.rocksetCollection(),
		"nDocuments":        docCount,
	}).Debug("successfully persisted documents to Rockset")
	return nil
}

func (t *transactor) sendReq(ctx context.Context, b *binding, docs []interface{}) error {
	docStatuses, err := t.client.AddDocuments(ctx, b.rocksetWorkspace(), b.rocksetCollection(), docs)
	if err != nil {
		return err
	}
	// Rockset's API doesn't fail the whole request due to an error with a single document,
	// so we need to iterate over each of the returned statuses and check them individually.
	// We'll log _all_ the errors, since it's unclear whether they'll all be the same or if the
	// order is significant.
	for _, docStatus := range docStatuses {
		if docStatus.Error != nil {
			// The error model has quite a few fields that seem worth logging. The naming
			// here is an attempt to clarify the provenance of the error info.
			var e = docStatus.Error
			// I'd hope that e.Message is never nil, but a generic error message is better
			// than _no_ error message just in case it is.
			var errMsg = "Rockset API error"
			if e.Message != nil && *e.Message != "" {
				errMsg = *e.Message // ugh
			}
			logrus.WithFields(logrus.Fields{
				"error":             errMsg,
				"rocksetErrorType":  e.Type,
				"rocksetTraceId":    e.TraceId,
				"rocksetErrorId":    e.ErrorId,
				"documentStatus":    docStatus.Status,
				"rocksetCollection": docStatus.Collection,
				"rocksetDocumentId": docStatus.Id,
			}).Error("Document was rejected by Rockset API")

			if err == nil {
				err = fmt.Errorf("a document was rejected by the Rockset API for Collection: '%s'", b.rocksetCollection())
			}
		}
	}
	return err
}
