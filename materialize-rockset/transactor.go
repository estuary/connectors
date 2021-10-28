package materialize_rockset

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type operation = int

const (
	opNotFound    = -1
	opDocumentAdd = iota
	opDocumentDelete
)

type binding struct {
	spec       *pf.MaterializationSpec_Binding
	operations map[operation][]json.RawMessage
}

func (b *binding) rocksetWorkspace() string {
	return b.spec.ResourcePath[0]
}

func (b *binding) rocksetCollection() string {
	return b.spec.ResourcePath[1]
}

type transactor struct {
	ctx      context.Context
	config   *config
	client   *client
	bindings []*binding
}

// pm.Transactor
func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	// Nothing to load
	return nil
}

// pm.Transactor
func (t *transactor) Prepare(req *pm.TransactionRequest_Prepare) (*pm.TransactionResponse_Prepared, error) {
	// Nothing to prepare
	return &pm.TransactionResponse_Prepared{}, nil
}

// pm.Transactor
func (t *transactor) Store(it *pm.StoreIterator) error {
	// TODO: Use operations as a stack, rather than discarding the entire buffer and reinitializing here.
	for _, b := range t.bindings {
		b.operations = make(map[int][]json.RawMessage)
	}

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
			op = opDocumentAdd
		} else {
			// TODO: use ChangeIndicator pointer to find the operation type

			// Sending an "AddDocument" request for a document which already
			// exists will result in the previous document being completely
			// overwritten. Since the CdcEvents contain the full document, this
			// is the exact desired behavior.
			op = opDocumentAdd
		}

		if op == opDocumentAdd {
			if err := t.storeAdditionOperation(b, &it.Key, &it.Values); err != nil {
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
func (t *transactor) Commit() error {
	var wait sync.WaitGroup

	for _, b := range t.bindings {
		wait.Add(1)

		go func(b *binding) {
			defer wait.Done()
			commitCollection(t, b)
		}(b)
	}

	wait.Wait()

	return nil
}

// pm.Transactor
func (t *transactor) Destroy() {
	// Nothing to clean up
}

func (t *transactor) storeAdditionOperation(b *binding, key *tuple.Tuple, values *tuple.Tuple) error {
	var document = make(map[string]interface{})

	// Add the `_id` field to the document. This is required by Rockset.
	document["_id"] = base64.RawStdEncoding.EncodeToString(key.Pack())

	// Add the keys to the document.
	for i, value := range *key {
		var propName = b.spec.FieldSelection.Keys[i]
		document[propName] = value
	}

	// Add the non-keys to the document.
	for i, value := range *values {
		var propName = b.spec.FieldSelection.Values[i]
		inference := b.spec.Collection.GetProjection(propName).Inference

		if containsString(inference.Types, "object") {
			if inference.IsSingleType() {
				// TODO: casting the tuple to a string isn't the right way of doing this.
				// Currently this results in the following error:
				// json: error calling MarshalJSON for type json.RawMessage: invalid character 's' looking for beginning of value
				document[propName] = json.RawMessage(value.(string))
			} else {
				// TODO: enforce this at constraint gathering time?
				return fmt.Errorf("cannot store document with fields which may or may not be an object: %s may be %v", propName, &inference.Types)
			}
		} else {
			document[propName] = value
		}
	}

	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to serialize the addition document: %w", err)
	}

	b.operations[opDocumentAdd] = append(b.operations[opDocumentAdd], jsonDoc)

	return nil
}

func (t *transactor) storeDeletionOperation(b *binding, key *tuple.Tuple) error {
	var document = make(map[string]interface{})
	document["_id"] = base64.RawStdEncoding.EncodeToString(key.Pack())

	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to serialize the deletion document: %w", err)
	}

	b.operations[opDocumentDelete] = append(b.operations[opDocumentDelete], jsonDoc)

	return nil
}

func commitCollection(t *transactor, b *binding) error {
	select {
	case <-t.ctx.Done():
		return fmt.Errorf("transactor context cancelled")
	default:
		// Keep going!
	}

	var documents []json.RawMessage

	defer logElapsedTime(time.Now(), fmt.Sprintf("commit completed: %d documents added, %d documents deleted", len(b.operations[opDocumentAdd]), len(b.operations[opDocumentDelete])))

	documents = b.operations[opDocumentAdd]
	if len(documents) > 0 {
		if err := t.client.AddDocuments(t.ctx, b.rocksetWorkspace(), b.rocksetCollection(), documents); err != nil {
			return err
		}
	}

	documents = b.operations[opDocumentDelete]
	if len(documents) > 0 {
		if err := t.client.DeleteDocuments(t.ctx, b.rocksetWorkspace(), b.rocksetCollection(), documents); err != nil {
			return err
		}
	}

	return nil
}

func logElapsedTime(start time.Time, msg string) {
	elapsed := time.Since(start)
	log.Infof("%s,%f", msg, elapsed.Seconds())
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}
