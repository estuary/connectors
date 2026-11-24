package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the history of awaiting queries is fetched.
// The server flushes its query log every 7.5 seconds by default.
const profileInterval = 15 * time.Second

// historyCluster is the cluster of a ClickHouse Cloud service's replicas.
const historyCluster = "default"

// historyWindow bounds the log searched for awaiting queries, all of which
// finished within the profiler's wait limit.
var historyWindow = boilerplate.QueryProfileMaxWait + time.Minute

// queryHistory reads the server's query log: a query's duration, the rows and
// bytes it read, wrote and returned, its memory, and the profile events that
// explain a slow one.
type queryHistory struct {
	conn chdriver.Conn
	// cluster is the cluster whose replicas' logs are all read, since a query
	// is only in the log of the replica that ran it. It's emptied if the
	// cluster can't be read, and the log of whichever replica a connection
	// lands on is read instead.
	cluster string
}

func newQueryProfiler(conn chdriver.Conn, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	var tables = make([]string, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
		tables[i] = b.target.Path[0]
	}
	return boilerplate.NewQueryProfiler(&queryHistory{conn: conn, cluster: historyCluster}, profileInterval, reportQueryID, paths,
		newTableContext(conn, tables))
}

// reportQueryID gives a query run with the returned context an ID of the
// connector's choosing, which the server logs it under.
func reportQueryID(ctx context.Context, setID func(string)) (context.Context, func()) {
	var id = uuid.NewString()
	setID(id)
	return clickhouse.Context(ctx, clickhouse.WithQueryID(id)), nil
}

// execTraced runs a statement of the binding, profiled as the binding's.
func (t *transactor) execTraced(ctx context.Context, conn chdriver.Conn, b *binding, query string, args ...any) error {
	var queryCtx, trace = t.profiler.Trace(ctx, b.target.Binding, boilerplate.StatementKind(query))
	var err = conn.Exec(queryCtx, query, args...)
	trace.Finish(err)
	return err
}

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	if h.cluster != "" {
		var out, err = h.fetch(ctx, fmt.Sprintf("clusterAllReplicas('%s', system.query_log)", h.cluster), ids)
		if !isException(err, chproto.ErrClusterDoesntExist, chproto.ErrBadGet, chproto.ErrAccessDenied) {
			return out, err
		}
		log.WithError(err).Debug("reading the query log of every replica failed; reading that of one")
		h.cluster = ""
	}

	var out, err = h.fetch(ctx, "system.query_log", ids)
	if isException(err, chproto.ErrAccessDenied, chproto.ErrUnknownTable) {
		return nil, fmt.Errorf("%w: %w", boilerplate.ErrQueryHistoryDenied, err)
	}
	return out, err
}

func (h *queryHistory) fetch(ctx context.Context, queryLog string, ids []string) (map[string]log.Fields, error) {
	var args = make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	var window = int(historyWindow.Seconds())
	rows, err := h.conn.Query(ctx, fmt.Sprintf(`
SELECT query_id, toString(type), query_duration_ms, read_rows, read_bytes, written_rows, written_bytes,
	result_rows, result_bytes, memory_usage, exception_code, ProfileEvents
FROM %s
WHERE event_date >= toDate(now() - INTERVAL %d SECOND) AND event_time >= now() - INTERVAL %d SECOND
	AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
	AND query_id IN (%s)
SETTINGS skip_unavailable_shards = 1`,
		queryLog, window, window, strings.Repeat("?, ", len(ids)-1)+"?"), args...)
	if err != nil {
		return nil, fmt.Errorf("querying query log: %w", err)
	}
	defer rows.Close()

	var out = make(map[string]log.Fields)
	for rows.Next() {
		var id, status string
		var duration, readRows, readBytes, writtenRows, writtenBytes, resultRows, resultBytes, memory uint64
		var exceptionCode int32
		var events map[string]uint64
		if err := rows.Scan(&id, &status, &duration, &readRows, &readBytes, &writtenRows, &writtenBytes,
			&resultRows, &resultBytes, &memory, &exceptionCode, &events); err != nil {
			return nil, fmt.Errorf("scanning query log: %w", err)
		}
		var fields = log.Fields{
			"status":        status,
			"totalMs":       duration,
			"rowsRead":      readRows,
			"bytesRead":     readBytes,
			"rowsWritten":   writtenRows,
			"bytesWritten":  writtenBytes,
			"rowsProduced":  resultRows,
			"bytesProduced": resultBytes,
			"memoryBytes":   memory,
		}
		if exceptionCode != 0 {
			fields["exceptionCode"] = exceptionCode
		}
		for event, field := range historyEvents {
			var value = events[event]
			if strings.HasSuffix(event, "Microseconds") {
				value /= 1000
			}
			if value > 0 {
				fields[field] = value
			}
		}
		out[id] = fields
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading query log: %w", err)
	}
	return out, nil
}

// historyEvents are the profile events logged of a query that counted them,
// by the field each is logged as. Microseconds are logged as milliseconds.
var historyEvents = map[string]string{
	"ConcurrentQueryWaitMicroseconds": "queuedMs",
	"OSCPUVirtualTimeMicroseconds":    "cpuMs",
	"OSCPUWaitMicroseconds":           "cpuWaitMs",
	"OSIOWaitMicroseconds":            "ioWaitMs",
	// The parts and index marks a query read, out of those of its tables,
	// tell how well the partition and sorting keys pruned.
	"SelectedParts":                       "partsScanned",
	"SelectedPartsTotal":                  "partsTotal",
	"SelectedMarks":                       "marksScanned",
	"SelectedMarksTotal":                  "marksTotal",
	"DiskReadElapsedMicroseconds":         "diskReadMs",
	"DiskWriteElapsedMicroseconds":        "diskWriteMs",
	"ReadBufferFromS3Microseconds":        "s3ReadMs",
	"ReadBufferFromS3Bytes":               "s3ReadBytes",
	"WriteBufferFromS3Microseconds":       "s3WriteMs",
	"WriteBufferFromS3Bytes":              "s3WriteBytes",
	"CachedReadBufferReadFromCacheBytes":  "cacheReadBytes",
	"CachedReadBufferReadFromSourceBytes": "cacheMissBytes",
	"NetworkSendElapsedMicroseconds":      "networkSendMs",
	"NetworkReceiveElapsedMicroseconds":   "networkReceiveMs",
	"ZooKeeperWaitMicroseconds":           "keeperWaitMs",
	"RWLockReadersWaitMilliseconds":       "readLockWaitMs",
	"RWLockWritersWaitMilliseconds":       "writeLockWaitMs",
	// Inserts into a table with too many parts are delayed.
	"DelayedInserts":                           "delayedInserts",
	"DelayedInsertsMilliseconds":               "insertDelayMs",
	"ExternalProcessingUncompressedBytesTotal": "spillBytes",
}

func isException(err error, codes ...chproto.Error) bool {
	if exc, ok := errors.AsType[*clickhouseproto.Exception](err); ok {
		for _, code := range codes {
			if chproto.Error(exc.Code) == code {
				return true
			}
		}
	}
	return false
}

// newTableContext reads a table's engine, keys, size and parts, and the
// server's version and size. Where the server requires grants to read system
// tables, the user may only have those of system.tables.
func newTableContext(conn chdriver.Conn, tables []string) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		var out = make(log.Fields)
		var errs []error

		table, err := describeTable(ctx, conn, tables[binding])
		if len(table) > 0 {
			out["table"] = table
		}
		errs = append(errs, err)

		warehouse, err := describeServer(ctx, conn)
		if len(warehouse) > 0 {
			out["warehouse"] = warehouse
		}
		errs = append(errs, err)

		return out, errors.Join(errs...)
	}
}

func describeTable(ctx context.Context, conn chdriver.Conn, table string) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	var engine, partitionKey, sortingKey string
	var rows, bytes *uint64
	if err := conn.QueryRow(ctx, `
SELECT engine, partition_key, sorting_key, total_rows, total_bytes
FROM system.tables WHERE database = currentDatabase() AND name = ?`, table,
	).Scan(&engine, &partitionKey, &sortingKey, &rows, &bytes); err != nil {
		errs = append(errs, fmt.Errorf("system.tables: %w", err))
	} else {
		out["engine"] = engine
		out["sortingKey"] = sortingKey
		if partitionKey != "" {
			out["partitionKey"] = partitionKey
		}
		if rows != nil {
			out["rows"] = *rows
		}
		if bytes != nil {
			out["bytes"] = *bytes
		}
	}

	// A query of a ReplacingMergeTree merges the parts of each partition it
	// reads, so many parts in one make it slow.
	var partitions, parts, maxPartitionParts, uncompressed uint64
	if err := conn.QueryRow(ctx, `
SELECT count(), sum(parts), max(parts), sum(uncompressed) FROM (
	SELECT count() AS parts, sum(data_uncompressed_bytes) AS uncompressed
	FROM system.parts WHERE database = currentDatabase() AND table = ? AND active
	GROUP BY partition_id
)`, table,
	).Scan(&partitions, &parts, &maxPartitionParts, &uncompressed); err != nil {
		errs = append(errs, fmt.Errorf("system.parts: %w", err))
	} else {
		out["partitions"] = partitions
		out["parts"] = parts
		out["maxPartitionParts"] = maxPartitionParts
		out["uncompressedBytes"] = uncompressed
	}

	var mutations uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = ? AND NOT is_done", table,
	).Scan(&mutations); err != nil {
		errs = append(errs, fmt.Errorf("system.mutations: %w", err))
	} else if mutations > 0 {
		out["pendingMutations"] = mutations
	}

	return out, errors.Join(errs...)
}

// describeServer reads the server's version, the memory it may use, which
// tells the size of a ClickHouse Cloud service, and its cluster's replicas.
func describeServer(ctx context.Context, conn chdriver.Conn) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		errs = append(errs, fmt.Errorf("version: %w", err))
	} else {
		out["version"] = version
	}

	var cgroupMemory, osMemory float64
	if err := conn.QueryRow(ctx, `
SELECT maxIf(value, metric = 'CGroupMemoryTotal'), maxIf(value, metric = 'OSMemoryTotal')
FROM system.asynchronous_metrics WHERE metric IN ('CGroupMemoryTotal', 'OSMemoryTotal')`,
	).Scan(&cgroupMemory, &osMemory); err != nil {
		errs = append(errs, fmt.Errorf("system.asynchronous_metrics: %w", err))
	} else if cgroupMemory > 0 {
		out["memoryBytes"] = uint64(cgroupMemory)
	} else if osMemory > 0 {
		out["memoryBytes"] = uint64(osMemory)
	}

	var replicas uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.clusters WHERE cluster = ?", historyCluster).Scan(&replicas); err != nil {
		errs = append(errs, fmt.Errorf("system.clusters: %w", err))
	} else if replicas > 0 {
		out["replicas"] = replicas
	}

	return out, errors.Join(errs...)
}
