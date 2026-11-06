package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake/v2"
)

// profileInterval is how often the history of awaiting queries is fetched.
// Reading it runs on the warehouse, so it's read soon after the queries
// finish, while the warehouse is still running for them.
const profileInterval = 5 * time.Second

// queryHistory reads the warehouse's query history: time spent queued,
// blocked on locks, compiling and executing, and the bytes and partitions
// scanned.
type queryHistory struct {
	db *stdsql.DB
}

func newQueryProfiler(db *stdsql.DB, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
	}
	return boilerplate.NewQueryProfiler(&queryHistory{db: db}, profileInterval, reportQueryID, paths)
}

// reportQueryID has the driver send the ID of a query run with the returned
// context to a channel, which is read once the query finishes.
func reportQueryID(ctx context.Context, setID func(string)) (context.Context, func()) {
	var ids = make(chan string, 1)
	return sf.WithQueryIDChan(ctx, ids), func() {
		select {
		case id := <-ids:
			setID(id)
		default:
		}
	}
}

// historyWindow bounds the history searched for awaiting queries, all of which
// finished within the profiler's wait limit.
var historyWindow = boilerplate.QueryProfileMaxWait + time.Minute

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	var placeholders = strings.Repeat("?, ", len(ids)-1) + "?"
	var args = make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	// QUERY_HISTORY lists the user's most recent queries, so the limit is
	// raised to its maximum to find them all among the user's other queries.
	rows, err := h.db.QueryContext(ctx, fmt.Sprintf(`
SELECT QUERY_ID, EXECUTION_STATUS, WAREHOUSE_SIZE, TOTAL_ELAPSED_TIME, COMPILATION_TIME, EXECUTION_TIME,
	QUEUED_PROVISIONING_TIME + QUEUED_REPAIR_TIME + QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME,
	BYTES_SCANNED, ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
	END_TIME_RANGE_START => DATEADD('second', -%d, CURRENT_TIMESTAMP()), RESULT_LIMIT => 10000))
WHERE QUERY_ID IN (%s) AND EXECUTION_STATUS IN ('SUCCESS', 'FAILED_WITH_ERROR', 'FAILED_WITH_INCIDENT')`,
		int(historyWindow.Seconds()), placeholders), args...)
	if err != nil {
		return nil, fmt.Errorf("querying history: %w", err)
	}
	defer rows.Close()

	var out = make(map[string]log.Fields)
	var final []string
	for rows.Next() {
		var id, status string
		var size stdsql.NullString
		var total, compilation, execution, queued, blocked, bytesScanned, rowsProduced stdsql.NullInt64
		if err := rows.Scan(&id, &status, &size, &total, &compilation, &execution, &queued, &blocked, &bytesScanned, &rowsProduced); err != nil {
			return nil, fmt.Errorf("scanning history: %w", err)
		}
		var fields = log.Fields{
			"status":        status,
			"warehouseSize": size.String,
			"totalMs":       total.Int64,
			"compilationMs": compilation.Int64,
			"executionMs":   execution.Int64,
			"bytesScanned":  bytesScanned.Int64,
			"rowsProduced":  rowsProduced.Int64,
		}
		if queued.Int64 > 0 {
			fields["queuedMs"] = queued.Int64
		}
		if blocked.Int64 > 0 {
			fields["blockedMs"] = blocked.Int64
		}
		out[id] = fields
		final = append(final, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading history: %w", err)
	} else if len(final) == 0 {
		return out, nil
	}

	if err := h.addOperatorStats(ctx, final, out); err != nil {
		return nil, err
	}
	return out, nil
}

// addOperatorStats adds the partitions the queries scanned of the tables they
// read, and the bytes they spilled, summed over their operators.
func (h *queryHistory) addOperatorStats(ctx context.Context, ids []string, out map[string]log.Fields) error {
	var selects = make([]string, len(ids))
	var args = make([]any, 0, 2*len(ids))
	for i, id := range ids {
		selects[i] = `SELECT ?,
	SUM(OPERATOR_STATISTICS:pruning:partitions_scanned::INT), SUM(OPERATOR_STATISTICS:pruning:partitions_total::INT),
	SUM(OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage::INT), SUM(OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage::INT)
FROM TABLE(GET_QUERY_OPERATOR_STATS(?))`
		args = append(args, id, id)
	}

	rows, err := h.db.QueryContext(ctx, strings.Join(selects, "\nUNION ALL\n"), args...)
	if err != nil {
		return fmt.Errorf("querying operator stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var scanned, total, spilledLocal, spilledRemote stdsql.NullInt64
		if err := rows.Scan(&id, &scanned, &total, &spilledLocal, &spilledRemote); err != nil {
			return fmt.Errorf("scanning operator stats: %w", err)
		}
		for name, v := range map[string]stdsql.NullInt64{
			"partitionsScanned": scanned,
			"partitionsTotal":   total,
			"spillLocalBytes":   spilledLocal,
			"spillRemoteBytes":  spilledRemote,
		} {
			if v.Valid {
				out[id][name] = v.Int64
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading operator stats: %w", err)
	}
	return nil
}
