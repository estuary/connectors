package connector

import (
	"errors"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/estuary/connectors/go/retry"
)

// idempotentRetry rides out a ClickHouse Cloud replica recycle, or an
// idle-scaled service waking back up, either of which can take several
// minutes to resolve. After its bounds are exhausted the last error is
// returned and the shard fails as it would without retries, so a persistently
// unhealthy cluster still surfaces promptly.
//
// It retries ambiguous failures, so every statement run under it must be
// idempotent; each call site says why. Because the underlying clickhouse.Conn
// is a pool that discards broken connections, each retry dials fresh -- on a
// load-balanced endpoint this can land on a healthy replica.
var idempotentRetry = retry.Policy{
	Classify:       classify,
	RetryAmbiguous: true,
	MaxAttempts:    16,
	MaxElapsed:     10 * time.Minute,
	InitialBackoff: time.Second,
	MaxBackoff:     30 * time.Second,
	Jitter:         0.5,
}

// classify sorts the failures seen when a ClickHouse Cloud replica is
// recycled mid-query -- connection reset, EOF on the native protocol, an i/o
// timeout -- by retry.Network. A server exception raised because the replica
// lost its Keeper session clears once the replica reconnects, but the
// statement may already have been applied, so it is ambiguous. Other server
// exceptions (e.g. code 241 OOM kills, DDL errors) are fatal: they need the
// query itself to change, not a retry.
func classify(err error) retry.Outcome {
	if exc, ok := errors.AsType[*clickhouseproto.Exception](err); ok {
		if chproto.Error(exc.Code) == chproto.ErrKeeperException {
			return retry.Ambiguous
		}
		return retry.Fatal
	}
	return retry.Network(err)
}
