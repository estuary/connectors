package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
)

// Per the EventBridge PutEvents API limits.
//
// Retry layering: connection-level errors and 5xx responses bubble out of
// t.client.PutEvents and are handled by the AWS SDK's built-in retryer
// (default standard mode: 3 attempts, exponential backoff with jitter, max
// ~20s). The putEventsRetry* constants below govern a *separate* loop for
// PutEvents 200 responses with FailedEntryCount > 0 (per-entry partial
// failures, e.g. ThrottlingException on a subset of entries) — the SDK
// retryer does not see those because the HTTP call succeeded. 4 attempts
// with 200ms base gives 200/400/800ms backoff = ~1.4s cumulative wait
// across 4 RPCs. A sustained per-entry throttle that exhausts this budget
// surfaces an error and the Flow runtime retries the whole transaction
// (at-least-once delivery is contractual, so re-publishing duplicates is
// safe). Do not raise this without a concrete throttling incident — bigger
// numbers just delay the bounce; they do not improve correctness.
const (
	putEventsMaxBatch      = 10
	putEventsMaxEntrySize  = 256 * 1024
	putEventsRetryAttempts = 4
	putEventsRetryBaseWait = 200 * time.Millisecond
	// storeConcurrency caps in-flight PutEvents calls per Store transaction.
	// 8 * 10-entry batches stays well under default per-region PutEvents rate
	// limits while keeping the pipeline filled across typical call latency;
	// raise for high-throughput buses with raised quotas, lower if you see
	// ThrottlingException retries dominating.
	storeConcurrency = 8
)

type bindingState struct {
	source     string
	detailType string
}

type transactor struct {
	client       *eventbridge.Client
	eventBusName string
	bindings     []bindingState
}

var _ m.Transactor = (*transactor)(nil)

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }
func (t *transactor) Destroy()                                                    {}

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

	var batch []types.PutEventsRequestEntry
	flush := func() {
		if len(batch) == 0 {
			return
		}
		entries := batch
		batch = nil
		errGroup.Go(func() error {
			return t.putEvents(ctx, entries)
		})
	}

	for it.Next(false) {
		b := t.bindings[it.Binding]

		if len(it.RawJSON) > putEventsMaxEntrySize {
			return nil, fmt.Errorf(
				"document for binding %d is %d bytes, exceeding the EventBridge per-entry limit of %d bytes",
				it.Binding, len(it.RawJSON), putEventsMaxEntrySize,
			)
		}

		batch = append(batch, types.PutEventsRequestEntry{
			EventBusName: aws.String(t.eventBusName),
			Source:       aws.String(b.source),
			DetailType:   aws.String(b.detailType),
			Detail:       aws.String(string(it.RawJSON)),
		})

		if len(batch) >= putEventsMaxBatch {
			flush()
		}
	}
	flush()

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

// putEvents publishes a batch and retries any individually-failed entries.
// PutEvents returns 200 OK even when some entries fail (partial-failure
// semantics), so we must inspect each result.
func (t *transactor) putEvents(ctx context.Context, entries []types.PutEventsRequestEntry) error {
	for attempt := range putEventsRetryAttempts {
		if attempt > 0 {
			wait := putEventsRetryBaseWait << (attempt - 1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		out, err := t.client.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: entries})
		if err != nil {
			return fmt.Errorf("PutEvents: %w", err)
		}
		if out.FailedEntryCount == 0 {
			return nil
		}

		var retry []types.PutEventsRequestEntry
		// Track the first non-retryable failure so the returned error
		// describes a genuinely permanent entry, not whatever happened to
		// be last in iteration order. At-least-once semantics make
		// fail-fast on permanent codes preferable to silently abandoning
		// the still-retryable entries: the runtime retries the whole
		// transaction, so surfacing the permanent failure loudly is the
		// safer signal.
		var permIdx = -1
		var permCode, permMsg string
		for i, r := range out.Entries {
			if r.ErrorCode == nil && r.ErrorMessage == nil {
				continue
			}
			code := aws.ToString(r.ErrorCode)
			if !retryableCode(code) {
				if permIdx == -1 {
					permIdx, permCode, permMsg = i, code, aws.ToString(r.ErrorMessage)
				}
				continue
			}
			retry = append(retry, entries[i])
		}
		if permIdx != -1 {
			return fmt.Errorf("entry %d: %s: %s", permIdx, permCode, permMsg)
		}
		if len(retry) == 0 {
			// Defensive: FailedEntryCount > 0 but no entries flagged. Treat as success.
			return nil
		}
		entries = retry
	}
	return fmt.Errorf("PutEvents: %d entries still failing after %d attempts",
		len(entries), putEventsRetryAttempts)
}

// retryableCode returns true for EventBridge per-entry error codes that
// indicate transient failures. These codes are surfaced on
// PutEventsResultEntry.ErrorCode and are stable across SDK versions.
func retryableCode(code string) bool {
	switch code {
	case "ThrottlingException", "Throttling",
		"InternalFailure", "ServiceUnavailable":
		return true
	}
	return false
}
