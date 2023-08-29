package sql

import (
	"context"
	"fmt"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

const (
	// Default update delay for materializations opting in to CommitWithDelay to use if no other
	// delay has been set.
	defaultUpdateDelay = 30 * time.Minute

	// storeThreshold is a somewhat crude indication that a transaction was likely part of a backfill
	// vs. a smaller incremental streaming transaction. If a materialization is configured with a commit
	// delay, it should only apply that delay to transactions that occur after it has fully backfilled
	// from the collection. The idea is that if a transaction stored a lot of documents it's probably
	// part of a backfill.
	storeThreshold = 1_000_000
)

// CommitWithDelay wraps a commitFn in an async operation that may add additional delay prior to
// returning. When used as the returned OpFuture from a materialization utilizing async commits,
// this can spread out transaction processing and result in fewer, larger transactions which may be
// desirable to reduce warehouse compute costs or comply with rate limits. The delay is bypassed if
// the actual commit operation takes longer than the configured delay, or if they number of stored
// documents is large (see storeThreshold above).
//
// Delaying the return from this function delays acknowledgement back to the runtime that the commit
// has finished. The commit will still apply to the endpoint, but holding back the runtime
// acknowledgement will delay the start of the next transaction while allowing the runtime to
// continue combining over documents for the next transaction.
//
// It is always possible for a connector to restart between committing to the endpoint and sending
// the runtime acknowledgement of that commit. The chance of this happening is greater when
// intentionally adding a delay between these events. When the endpoint is authoritative and
// persists checkpoints transactionally with updating the state of the view (as is typical with a
// SQL materialization), what will happen in this case is that when the connector restarts it will
// read the previously persisted checkpoint and acknowledge it to the runtime then. In this way the
// materialization will resume from where it left off with respect to the endpoint state.
func CommitWithDelay(ctx context.Context, round int, delay time.Duration, stored int, commitFn func(context.Context) error) pf.OpFuture {
	return pf.RunAsyncOperation(func() error {
		started := time.Now()

		if err := commitFn(ctx); err != nil {
			return err
		}

		if round == 1 {
			// Always skip the delay on the first round of transactions, which is often an
			// artificially small transaction of the immediately-ready documents.
			log.Debug("will not delay commit acknowledgement of the first transaction")
			return nil
		}

		remainingDelay := delay - time.Since(started)

		logEntry := log.WithFields(log.Fields{
			"stored":          stored,
			"storedThreshold": storeThreshold,
			"remainingDelay":  remainingDelay.String(),
			"configuredDelay": delay.String(),
		})

		if stored > storeThreshold || remainingDelay <= 0 {
			logEntry.Debug("will acknowledge commit without further delay")
			return nil
		}

		logEntry.Debug("delaying before acknowledging commit")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(remainingDelay):
			return nil
		}
	})
}

// ParseDelay parses the delay Go duration string, returning and error if it is not valid, the
// parsed value if it is not an empty string, and the defaultUpdateDelay otherwise.
func ParseDelay(delay string) (time.Duration, error) {
	if delay != "" {
		parsed, err := time.ParseDuration(delay)
		if err != nil {
			return 0, fmt.Errorf("could not parse updateDelay '%s': must be a valid Go duration string", delay)
		}
		return parsed, nil
	}
	return defaultUpdateDelay, nil
}
