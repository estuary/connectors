package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
)

// Per the EventBridge PutEvents API limits.
const (
	// putEventsMaxBatch is the AWS-side hard cap on entries per PutEvents
	// request; an 11th entry returns 413. See API reference Entries field:
	// https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html
	putEventsMaxBatch = 10
	// putEventsMaxEntrySize and putEventsMaxRequestSize encode two
	// documented limits that the AWS docs do not perfectly agree on:
	//
	//   - EventBridge User Guide says the 1 MB limit applies to the
	//     request as a whole, with a single entry permitted to use the
	//     full 1 MB if alone:
	//     https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevents.html#eb-putevent-size
	//   - The Go SDK's PutEvents doc and older API reference language
	//     state the per-entry limit is 256 KB:
	//     https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/eventbridge#Client.PutEvents
	//
	// We honor both, conservatively: each entry must fit in 256 KB and
	// the sum of entries in a single PutEvents call must fit in 1 MB.
	//
	// Per the AWS-defined size calculation, each entry's size is
	// len(Source) + len(DetailType) + len(Detail) (UTF-8 bytes); plus 14
	// if Time is set; plus UTF-8 byte length of each Resources entry.
	// This connector sets neither Time nor Resources, and EventBusName
	// is not counted.
	putEventsMaxEntrySize   = 256 * 1024
	putEventsMaxRequestSize = 1024 * 1024
	// storeConcurrency caps in-flight PutEvents calls per Store transaction.
	// 8 * 10-entry batches stays well under default per-region PutEvents rate
	// limits while keeping the pipeline filled across typical call latency;
	// raise for high-throughput buses with raised quotas, lower if you see
	// ThrottlingException retries dominating.
	storeConcurrency = 8
)

type bindingState struct {
	path       []string
	source     string
	detailType string
}

type transactor struct {
	client       *eventbridge.Client
	eventBusName string
	bindings     []bindingState
	be           *m.BindingEvents
}

var _ m.Transactor = (*transactor)(nil)

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }
func (t *transactor) Flush(context.Context, map[int]time.Time) error {
	return nil
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}
func (t *transactor) Destroy() {}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

// EventBridge is delta-update only.
func (t *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for materialize-eventbridge")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	errGroup, ctx := errgroup.WithContext(it.Context())
	errGroup.SetLimit(storeConcurrency)

	round := it.Round
	// published counts the entries EventBridge accepted per binding, for the
	// transaction health report. A batch mixes bindings, so each entry's
	// binding rides alongside it.
	published := make([]atomic.Int64, len(t.bindings))

	var (
		batch         []types.PutEventsRequestEntry
		batchBindings []int
		batchBytes    int
	)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		entries, bindings := batch, batchBindings
		batch, batchBindings = nil, nil
		batchBytes = 0
		errGroup.Go(func() error {
			if err := t.putEvents(ctx, entries); err != nil {
				return err
			}
			for _, b := range bindings {
				published[b].Add(1)
			}
			return nil
		})
	}

	for it.Next(false) {
		b := t.bindings[it.Binding]

		entrySize := len(b.source) + len(b.detailType) + len(it.RawJSON)
		if entrySize > putEventsMaxEntrySize {
			return nil, fmt.Errorf(
				"document for binding %d is %d bytes (source+detail-type+detail), exceeding the EventBridge PutEvents per-entry limit of %d bytes",
				it.Binding, entrySize, putEventsMaxEntrySize,
			)
		}

		// Flush before append if either the batch is full or adding this
		// entry would push the request total over the 1 MB limit.
		if len(batch) >= putEventsMaxBatch || batchBytes+entrySize > putEventsMaxRequestSize {
			flush()
		}

		batch = append(batch, types.PutEventsRequestEntry{
			EventBusName: aws.String(t.eventBusName),
			Source:       aws.String(b.source),
			DetailType:   aws.String(b.detailType),
			Detail:       aws.String(string(it.RawJSON)),
		})
		batchBindings = append(batchBindings, it.Binding)
		batchBytes += entrySize
	}
	flush()

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	for i, b := range t.bindings {
		if n := published[i].Load(); n > 0 {
			t.be.ReportRowStats(round, b.path, m.TotalRowStats(n))
		}
	}
	return nil, nil
}

// putEvents publishes a batch, re-sending the entries that failed individually
// with a transient error. PutEvents returns 200 OK even when some entries fail,
// so each result must be inspected.
func (t *transactor) putEvents(ctx context.Context, entries []types.PutEventsRequestEntry) error {
	return putEventsRetry.Do(ctx, "PutEvents", func(ctx context.Context) error {
		out, err := t.client.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: entries})
		if err != nil {
			return fmt.Errorf("PutEvents: %w", err)
		} else if out.FailedEntryCount == 0 {
			return nil
		}

		var failed []types.PutEventsRequestEntry
		var first *types.PutEventsResultEntry
		for i, r := range out.Entries {
			if r.ErrorCode == nil && r.ErrorMessage == nil {
				continue
			}
			code := aws.ToString(r.ErrorCode)
			if !retryableCode(code) {
				return fmt.Errorf("entry %d: %s: %s", i, code, aws.ToString(r.ErrorMessage))
			}
			if first == nil {
				first = &out.Entries[i]
			}
			failed = append(failed, entries[i])
		}
		if len(failed) == 0 {
			// Defensive: FailedEntryCount > 0 but no entries flagged.
			return nil
		}
		entries = failed
		return &failedEntriesError{
			count:   len(failed),
			code:    aws.ToString(first.ErrorCode),
			message: aws.ToString(first.ErrorMessage),
		}
	})
}
