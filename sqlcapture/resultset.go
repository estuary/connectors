package sqlcapture

import (
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
)

// A resultSet represents the buffered entries read from zero or more tables
// being backfilled. The nil resultSet is valid and represents an absence of
// any buffered data (this is used during startup streaming).
type resultSet struct {
	streams map[string]*backfillChunk
}

type backfillChunk struct {
	rows       map[string]ChangeEvent // A map from the encoded primary key of a row to the 'Insert' event for that row
	keyColumns []string               // The names of the primary key columns used for this table, in order
	scanned    []byte                 // The encoded primary key of the greatest row in the chunk (or nil when complete=true)
	complete   bool                   // When true, indicates that this chunk *completes* the table, and thus has no precise endpoint
}

func newResultSet() *resultSet {
	return &resultSet{streams: make(map[string]*backfillChunk)}
}

// Buffer appends new "Insert" events to the buffered range for the specified stream. An
// empty list of events indicates that the stream is completed, and thus the *range* of the
// buffer now extends to infinity.
func (r *resultSet) Buffer(streamID string, keyColumns []string, events []ChangeEvent) error {
	var chunk, ok = r.streams[streamID]
	if !ok {
		chunk = &backfillChunk{keyColumns: keyColumns, rows: make(map[string]ChangeEvent)}
		r.streams[streamID] = chunk
	}

	// Buffering an empty set of rows means we've reached the end of this table
	if len(events) == 0 {
		logrus.WithField("stream", streamID).Debug("buffered final (empty) chunk")
		chunk.scanned = nil
		chunk.complete = true
		return nil
	}

	// Otherwise add the new row to the `rows` map and update `scanned`.
	for _, event := range events {
		var bs, err = encodeRowKey(chunk.keyColumns, event.After)
		if err != nil {
			return fmt.Errorf("error encoding row key: %w", err)
		}
		if chunk.scanned != nil && compareTuples(chunk.scanned, bs) >= 0 {
			// It's important for correctness that the ordering of serialized primary keys matches
			// the ordering that PostgreSQL uses. Since we're already serializing every result row's
			// key this is a good place to opportunistically check that invariant.
			return fmt.Errorf("primary key ordering failure: prev=%q, next=%q", chunk.scanned, bs)
		}
		chunk.rows[string(bs)] = event
		chunk.scanned = bs
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.WithFields(logrus.Fields{
				"stream":   streamID,
				"op":       event.Operation,
				"rowKey":   base64.StdEncoding.EncodeToString(bs),
				"chunkEnd": base64.StdEncoding.EncodeToString(chunk.scanned),
			}).Debug("buffered scan result")
		}
	}
	return nil
}

// Streams returns a list of all streams tracked by the resultSet.
func (r *resultSet) Streams() []string {
	if r == nil {
		return nil
	}
	var streams []string
	for stream := range r.streams {
		streams = append(streams, stream)
	}
	sort.Strings(streams) // Sorted for test stability
	return streams
}

// Complete returns true if the stream is "completed" by this resultSet.
func (r *resultSet) Complete(streamID string) bool {
	if r == nil {
		return false
	}
	return r.streams[streamID].complete
}

// Scanned returns the highest row key buffered in the resultSet.
func (r *resultSet) Scanned(streamID string) []byte {
	if r == nil {
		return nil
	}
	return r.streams[streamID].scanned
}

// Patch modifies the buffered results to include the effect of the provided
// changeEvent. This may simply mean ignoring the event, if it occured on a
// table which is not in the resultSet or for a row which is not yet included.
func (r *resultSet) Patch(streamID string, event ChangeEvent) error {
	// If a particular table is not represented in the backfill result-set then
	// patching its changes is a no-op.
	if r == nil {
		return nil
	}
	var chunk, ok = r.streams[streamID]
	if !ok {
		return nil
	}

	var bs, err = encodeRowKey(chunk.keyColumns, event.KeyFields())
	if err != nil {
		return fmt.Errorf("error encoding patch key: %w", err)
	}
	var rowKey = string(bs)

	// Ignore mutations occurring after the end of the current resultset, unless this
	// is the final resultset which will complete the backfill.
	if !chunk.complete && compareTuples([]byte(rowKey), chunk.scanned) > 0 {
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.WithFields(logrus.Fields{
				"stream":   streamID,
				"op":       event.Operation,
				"rowKey":   base64.StdEncoding.EncodeToString([]byte(rowKey)),
				"chunkEnd": base64.StdEncoding.EncodeToString(chunk.scanned),
			}).Debug("filtering change")
		}
		return nil
	}

	// Apply the new change event to the buffered result set. Note that it's
	// entirely possible to see "invalid" changes (such as insertion of a row
	// that already exists, and updates/deletions of nonexistent rows), because
	// the whole point of buffering and patching the result-set is to resolve
	// all such inconsistencies by the time the next watermark is reached.
	switch event.Operation {
	case InsertOp:
		chunk.rows[rowKey] = event
	case UpdateOp:
		chunk.rows[rowKey] = ChangeEvent{
			Operation: InsertOp,
			Source:    event.Source,
			Before:    nil,
			After:     event.After,
		}
	case DeleteOp:
		delete(chunk.rows, rowKey)
	default:
		return fmt.Errorf("patched invalid change type %q", event.Operation)
	}
	return nil
}

// Changes returns the buffered contents of the resultSet for the specified stream,
// including any Patch()ed mutations to that buffer.
func (r *resultSet) Changes(streamID string) []ChangeEvent {
	if r == nil {
		return nil
	}

	// Sort row keys in order so that rows within a chunk will be
	// emitted deterministically. This only really matters for testing,
	// and it's in a relatively hot path here, so this might be low-
	// hanging fruit for optimization if profiling suggests it's needed.
	var keys []string
	for key := range r.streams[streamID].rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Make a list of events in the sorted order
	var events []ChangeEvent
	for _, key := range keys {
		events = append(events, r.streams[streamID].rows[key])
	}
	return events
}
