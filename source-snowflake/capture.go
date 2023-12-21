package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	initialCloneSequenceNumber = 0
	metadataProperty           = "_meta"
)

// Emit a checkpoint after every N change documents, so that partial
// progress can be made across connector restarts. This is a tuning
// constant which is only a variable so it can be lowered for tests.
var partialProgressCheckpointSpacing = 10000

type snowflakeSourceMetadata struct {
	sqlcapture.SourceCommon

	SequenceNumber int `json:"seq" jsonschema:"description=The sequence number of the staging table from which this document was read"`
	TableOffset    int `json:"off" jsonschema:"description=The offset within that staging table at which this document occurred"`
}

func (snowflakeDriver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	log.Debug("connector started")

	var checkpoint = new(captureState)
	if len(open.StateJson) > 0 {
		if err := pf.UnmarshalStrict(open.StateJson, checkpoint); err != nil {
			return fmt.Errorf("unable to parse state checkpoint: %w", err)
		}
	}
	if checkpoint.Streams == nil {
		checkpoint.Streams = make(map[snowflakeObject]*streamState)
	}

	var ctx = stream.Context()
	var cfg = new(config)
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, cfg); err != nil {
		return fmt.Errorf("error parsing config json: %w", err)
	}
	cfg.SetDefaults()

	var db, err = connectSnowflake(ctx, cfg)
	if err != nil {
		return fmt.Errorf("error connecting to snowflake: %w", err)
	}
	defer db.Close()

	var bindings = make(map[snowflakeObject]*captureBinding)
	var prerequisiteErrs = setupPrerequisites(ctx, cfg, db)
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}
		res.SetDefaults()

		var orderColumns []string
		for _, keyElem := range binding.Collection.Key {
			keyElem = strings.TrimPrefix(keyElem, "/") // Trim the leading slash

			// The collection key can only be converted into a list of ordering columns
			// if the key pointers all reference top-level properties which aren't `_meta`.
			if strings.Contains(keyElem, "/") || keyElem == metadataProperty {
				orderColumns = nil
				break
			}

			orderColumns = append(orderColumns, unescapeTildes(keyElem))
		}

		var table = snowflakeObject{res.Schema, res.Table}
		log.WithField("table", table.String()).Info("configured binding")
		if err := setupTablePrerequisites(ctx, cfg, db, table); err != nil {
			prerequisiteErrs = append(prerequisiteErrs, err)
			continue
		}
		bindings[table] = &captureBinding{
			Index:        idx,
			Table:        table,
			OrderColumns: orderColumns,
		}
	}
	if len(prerequisiteErrs) > 0 {
		return cerrors.NewUserError(nil, (&prerequisitesError{prerequisiteErrs}).Error())
	}

	var c = &capture{
		Name:     string(open.Capture.Name),
		Config:   cfg,
		DB:       db,
		State:    checkpoint,
		Bindings: bindings,
		Output:   stream,
	}
	return c.Run(ctx)
}

type capture struct {
	Name     string
	Config   *config
	DB       *sql.DB
	State    *captureState
	Bindings map[snowflakeObject]*captureBinding
	Output   *boilerplate.PullOutput
}

type captureBinding struct {
	Index        int
	Table        snowflakeObject
	OrderColumns []string
}

type captureState struct {
	Streams map[snowflakeObject]*streamState `json:"streams"`
}

func (s captureState) Validate() error {
	return nil
}

type streamState struct {
	SequenceNumber int `json:"seq"` // The sequence number of the current/next staging table to capture.
	TableOffset    int `json:"off"` // The offset within the current staging table.
}

func (c *capture) Run(ctx context.Context) error {
	log.WithField("name", c.Name).Info("started capture")

	// Notify Flow that we're ready and don't want to receive acknowledgements.
	if err := c.Output.Ready(false); err != nil {
		return err
	}

	log.Debug("processing added/removed bindings")
	var removedStreams, err = c.updateState(ctx)
	if err != nil {
		return fmt.Errorf("error processing added/removed bindings: %w", err)
	}

	if err := c.pruneDeletedStreams(ctx, removedStreams); err != nil {
		return fmt.Errorf("error pruning deleted streams: %w", err)
	}

	highestStaged, err := c.pruneStagingTables(ctx, removedStreams)
	if err != nil {
		return fmt.Errorf("error pruning staging tables: %w", err)
	}

	// Sort the list of streams so that we can capture each of them in a deterministic order.
	var tables []snowflakeObject
	for table := range c.State.Streams {
		tables = append(tables, table)
	}
	slices.SortFunc(tables, func(a, b snowflakeObject) int {
		if a.Schema != b.Schema {
			return strings.Compare(a.Schema, b.Schema)
		}
		return strings.Compare(a.Name, b.Name)
	})

	for _, table := range tables {
		var streamState = c.State.Streams[table]
		var binding = c.Bindings[table]
		if binding == nil {
			return fmt.Errorf("internal error: unknown binding for table %q", table.String())
		}

		// For each stream, we decide what our target sequence number is. In the overwhelmingly
		// more common case this will just be the sequence number from the checkpoint state,
		// but it is possible to end up with some "leftover" staging tables and in these cases
		// we want to consume those tables *plus one more* which we will create shortly.
		var targetSeq = streamState.SequenceNumber
		if staged, ok := highestStaged[table]; ok {
			targetSeq = staged + 1
		}

		// Capture staging tables until the stream's sequence number exceeds the target.
		// The captureStagingTable() operation will be responsible for updating the stream
		// state when done, since it's already handling checkpoints and partial progress.
		for streamState.SequenceNumber <= targetSeq {
			if err := c.captureStagingTable(ctx, table, binding.OrderColumns); err != nil {
				return fmt.Errorf("error capturing staging table %d: %w", streamState.SequenceNumber, err)
			}
		}
	}
	return nil
}

func (c *capture) updateState(ctx context.Context) ([]snowflakeObject, error) {
	var updatedStreams = make(map[snowflakeObject]*streamState)
	var removedStreams []snowflakeObject

	// Prune deleted streams from the state.
	for table := range c.State.Streams {
		if _, ok := c.Bindings[table]; !ok {
			log.WithField("table", table.String()).Info("binding removed")
			delete(c.State.Streams, table)
			updatedStreams[table] = nil
			removedStreams = append(removedStreams, table)
		}
	}

	// Identify newly added streams, clone initial table contents, and initialize stream state.
	for _, binding := range c.Bindings {
		if _, ok := c.State.Streams[binding.Table]; !ok {
			log.WithField("table", binding.Table.String()).Info("binding added")

			streamName, err := createChangeStream(ctx, c.Config, c.DB, c.Name, binding.Table)
			if err != nil {
				return nil, err
			}
			log.WithField("stream", streamName.String()).Debug("created change stream")

			stagingName, err := createInitialCloneTable(ctx, c.Config, c.DB, c.Name, binding.Table, initialCloneSequenceNumber)
			if err != nil {
				return nil, err
			}
			log.WithField("staged", stagingName.String()).Debug("cloned into staging table")

			var state = &streamState{SequenceNumber: initialCloneSequenceNumber}
			c.State.Streams[binding.Table] = state
			updatedStreams[binding.Table] = state
		}
	}

	// Emit a new checkpoint patch reflecting these deletions and initializations.
	if len(updatedStreams) > 0 {
		if updateCheckpoint, err := json.Marshal(&captureState{Streams: updatedStreams}); err != nil {
			return nil, fmt.Errorf("error marshalling checkpoint: %w", err)
		} else if err := c.Output.Checkpoint(updateCheckpoint, true); err != nil {
			return nil, fmt.Errorf("error emitting checkpoint: %w", err)
		}
	}
	return removedStreams, nil
}

func (c *capture) pruneDeletedStreams(ctx context.Context, removedStreams []snowflakeObject) error {
	log.WithField("flowSchema", c.Config.Advanced.FlowSchema).Info("pruning deleted streams")

	// Build a mapping from unique ID strings to deletion status. The function from table name
	// to ID is one-way, so we have to build the inverse mapping from known table names here.
	var idToRemovedStatus = make(map[string]bool)
	for _, table := range removedStreams {
		idToRemovedStatus[table.UniqueID(c.Name)] = true
	}

	// Enumerate streams in the appropriate schema
	var xdb = sqlx.NewDb(c.DB, "snowflake").Unsafe()
	var changeStreams []*struct {
		Schema string `db:"schema_name"`
		Name   string `db:"name"`
	}
	var changeStreamsQuery = fmt.Sprintf("SHOW STREAMS IN SCHEMA %s STARTS WITH 'flow_stream_';", c.Config.Advanced.FlowSchema)
	if err := xdb.Select(&changeStreams, changeStreamsQuery); err != nil {
		return fmt.Errorf("error listing change streams: %w", err)
	}

	// Iterate over the list of streams and make a list of ones needing deletion.
	var needsDeletion []string
	for _, changeStream := range changeStreams {
		// Split the name on underscores and extract the unique ID portion, and
		// queue for deletion any change streams whose ID corresponds to a newly
		// deleted binding.
		var bits = strings.SplitN(changeStream.Name, "_", 3)
		if len(bits) != 3 {
			continue
		}
		var uniqueID = bits[2]

		if idToRemovedStatus[uniqueID] {
			log.WithField("name", changeStream.Name).Info("will prune change stream of removed binding")
			needsDeletion = append(needsDeletion, changeStream.Name)
			continue
		}
	}

	// Actually perform deletion of no-longer-needed streams.
	for _, deleteStream := range needsDeletion {
		log.WithField("stream", deleteStream).Info("pruning change stream")
		var dropStreamQuery = fmt.Sprintf("DROP STREAM IF EXISTS %s;", snowflakeObject{c.Config.Advanced.FlowSchema, deleteStream}.QuotedName())
		if _, err := c.DB.ExecContext(ctx, dropStreamQuery); err != nil {
			return fmt.Errorf("error dropping change stream %q: %w", deleteStream, err)
		}
	}

	return nil
}

// pruneStagingTables enumerates staging tables in the Snowflake database and deletes any
// which are no longer needed.
//
// It returns a map from table names to the *greatest* staging table sequence number still
// live, so that the connector can reliably consume all old staging tables as well as one
// new staging table during each invocation.
//
// Since newly-added bindings have their initial table snapshot cloned before the stream
// state is initialized, that snapshot can be viewed as just another staging table here.
func (c *capture) pruneStagingTables(ctx context.Context, removedStreams []snowflakeObject) (map[snowflakeObject]int, error) {
	log.WithField("flowSchema", c.Config.Advanced.FlowSchema).Info("pruning staging tables")

	// Build mappings from unique ID strings to the corresponding table name
	// (for active bindings) and removed status (for bindings removed in this
	// connector invocation). The function from table name to ID is one-way, so
	// we have to build the inverse mapping from known table names here.
	var idToSourceTable = make(map[string]snowflakeObject)
	for table := range c.State.Streams {
		idToSourceTable[table.UniqueID(c.Name)] = table
	}
	var idToRemovedStatus = make(map[string]bool)
	for _, table := range removedStreams {
		idToRemovedStatus[table.UniqueID(c.Name)] = true
	}

	// Enumerate staging tables in the appropriate schema
	var xdb = sqlx.NewDb(c.DB, "snowflake").Unsafe()
	var stagingTables []*snowflakeDiscoveryTable
	var stagingTablesQuery = fmt.Sprintf("SHOW TABLES IN SCHEMA %s STARTS WITH 'flow_staging_';", c.Config.Advanced.FlowSchema)
	if err := xdb.Select(&stagingTables, stagingTablesQuery); err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}

	// Iterate over the list of staging tables and make a list of ones needing deletion.
	var needsDeletion []string
	var highestStaged = make(map[snowflakeObject]int)
	for _, stagingTable := range stagingTables {
		// Split the name on underscores and extract the sequence number and unique ID portions.
		var bits = strings.SplitN(stagingTable.Name, "_", 4)
		if len(bits) != 4 {
			continue
		}
		var sequenceNumber, err = strconv.ParseInt(bits[2], 10, 64)
		if err != nil {
			continue
		}
		var uniqueID = bits[3]

		log.WithField("uid", uniqueID).WithField("seq", sequenceNumber).Debug("found staging table")

		// A staging table should be dropped if the source table to which it corresponds
		// has just been deleted, or if it corresponds to an active binding and its sequence
		// number is less than the current acknowledged sequence number of that stream.
		if idToRemovedStatus[uniqueID] {
			log.WithField("name", stagingTable.Name).Info("will prune staging table of removed binding")
			needsDeletion = append(needsDeletion, stagingTable.Name)
			continue
		}
		var sourceTable, sourceTableKnown = idToSourceTable[uniqueID]
		if !sourceTableKnown {
			log.WithField("name", stagingTable.Name).Debug("will not prune staging table of unknown binding")
			continue
		}
		var streamState = c.State.Streams[sourceTable]
		if int(sequenceNumber) < streamState.SequenceNumber {
			log.WithField("name", stagingTable.Name).Debug("will prune staging table which has been fully captured")
			needsDeletion = append(needsDeletion, stagingTable.Name)
			continue
		}

		// Since we're already enumerating staging tables and parsing their names, we also
		// want to keep track of the highest staging table index observed for later. Using
		// greater-or-equal comparison here ensures that zero works consistently.
		if int(sequenceNumber) >= highestStaged[sourceTable] {
			highestStaged[sourceTable] = int(sequenceNumber)
		}
	}

	// Actually perform deletion of no-longer-needed staging tables.
	for _, deleteTable := range needsDeletion {
		log.WithField("table", deleteTable).Info("pruning staging table")
		var dropTableQuery = fmt.Sprintf("DROP TABLE IF EXISTS %s;", snowflakeObject{c.Config.Advanced.FlowSchema, deleteTable}.QuotedName())
		if _, err := c.DB.ExecContext(ctx, dropTableQuery); err != nil {
			return nil, fmt.Errorf("error dropping staging table %q: %w", deleteTable, err)
		}
	}
	return highestStaged, nil
}

func (c *capture) captureStagingTable(ctx context.Context, table snowflakeObject, orderColumns []string) error {
	var streamState = c.State.Streams[table]
	var stagingName = stagingTableName(c.Config, c.Name, table, streamState.SequenceNumber)
	var logEntry = log.WithFields(log.Fields{
		"table": table.String(),
		"seq":   streamState.SequenceNumber,
	})

	// Ensure that the staging table exists by creating it if necessary. Since
	// the creation is done IF NOT EXISTS this is a no-op if it already does.
	if _, err := createStagingTable(ctx, c.Config, c.DB, c.Name, table, streamState.SequenceNumber); err != nil {
		return err
	}

	// Generate a query to read the staging table contents.
	var quotedOrderColumns []string
	for _, orderColumn := range orderColumns {
		quotedOrderColumns = append(quotedOrderColumns, quoteSnowflakeIdentifier(orderColumn))
	}
	var queryBuf = new(strings.Builder)
	fmt.Fprintf(queryBuf, "SELECT * FROM %s", stagingName.QuotedName())
	if len(quotedOrderColumns) > 0 {
		fmt.Fprintf(queryBuf, " ORDER BY %s", strings.Join(quotedOrderColumns, ", "))
	}
	fmt.Fprintf(queryBuf, " LIMIT NULL OFFSET %d", streamState.TableOffset)
	var readStagingTableQuery = queryBuf.String()

	// Read contents of staging table and output to Flow.
	logEntry.WithField("query", readStagingTableQuery).Info("querying staging table")
	rows, err := c.DB.QueryContext(sf.WithStreamDownloader(ctx), readStagingTableQuery)
	if err != nil {
		return fmt.Errorf("error querying staging table %q: %w", stagingName.String(), err)
	}
	defer rows.Close()

	// Set up arrays for result row scanning
	cnames, err := rows.Columns()
	if err != nil {
		return err
	}
	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}
	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Snowflake represents a row update as a pair of an insert and a delete with
	// the same `METADATA$ROW_ID` value and `METADATA$ISUPDATE` set to true. This
	// buffer is used to store partial halves of rows.
	//
	// The paired rows changes should normally be located one after the other so
	// this buffer should only hold one row-value at any given time.
	var updatesBuffer = make(map[string]map[string]any)

	// We emit "partial progress" checkpoints periodically when processing large
	// result sets, but only when certain conditions are met.
	var documentsSinceLastCheckpoint int
	var previousDocumentKeyHash string

	for rows.Next() {
		if err := rows.Scan(vptrs...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}
		var fields = make(map[string]any)
		for idx, name := range cnames {
			var val, err = translateFieldValue(vals[idx], ctypes[idx])
			if err != nil {
				return fmt.Errorf("error translating field %q: %w", name, err)
			}
			fields[name] = val
		}
		logEntry.WithField("fields", fields).Trace("got change")

		// Extract 'METADATA$ACTION', 'METADATA$ROW_ID' and 'METADATA$ISUPDATE' from change data.
		// If the fields are missing, this staging table can be presumed to be the initial clone
		// and we should fill in suitable placeholders for each.
		action, ok := fields["METADATA$ACTION"].(string)
		if !ok {
			action = "INSERT"
		}
		rowID, ok := fields["METADATA$ROW_ID"].(string)
		if !ok {
			rowID = fmt.Sprintf("BACKFILL:%012d", streamState.TableOffset)
		}
		isUpdate, ok := fields["METADATA$ISUPDATE"].(bool)
		if !ok {
			isUpdate = false
		}
		delete(fields, "METADATA$ACTION")
		delete(fields, "METADATA$ROW_ID")
		delete(fields, "METADATA$ISUPDATE")

		// Translate action type and match up paired insert/delete portions of updates
		var operation sqlcapture.ChangeOp
		var before, after map[string]any
		if !isUpdate {
			switch action {
			case "INSERT":
				operation = sqlcapture.InsertOp
				after = fields
			case "DELETE":
				operation = sqlcapture.DeleteOp
				before = fields
			default:
				return fmt.Errorf("unhandled action type %q", action)
			}
		} else if otherHalf, ok := updatesBuffer[rowID]; ok {
			switch action {
			case "INSERT":
				operation = sqlcapture.UpdateOp
				before, after = otherHalf, fields
			case "DELETE":
				operation = sqlcapture.UpdateOp
				before, after = fields, otherHalf
			default:
				return fmt.Errorf("unhandled action type %q", action)
			}
			delete(updatesBuffer, rowID)
		} else {
			updatesBuffer[rowID] = fields
			continue
		}

		// Transform the before/after states into a proper change document with fields in the appropriate places.
		var meta = struct {
			Operation sqlcapture.ChangeOp      `json:"op"`
			Source    *snowflakeSourceMetadata `json:"source"`
			Before    map[string]any           `json:"before,omitempty"`
		}{
			Operation: operation,
			Source: &snowflakeSourceMetadata{
				SourceCommon: sqlcapture.SourceCommon{
					Millis:   0, // Not known.
					Schema:   table.Schema,
					Snapshot: streamState.SequenceNumber == initialCloneSequenceNumber,
					Table:    table.Name,
				},
				SequenceNumber: streamState.SequenceNumber,
				TableOffset:    streamState.TableOffset,
			},
			Before: nil,
		}
		var document map[string]any
		switch operation {
		case sqlcapture.InsertOp:
			document = after
		case sqlcapture.UpdateOp:
			meta.Before, document = before, after
		case sqlcapture.DeleteOp:
			document = before
		}
		if document == nil {
			logEntry.WithField("op", operation).Warn("change event data map is nil")
			document = make(map[string]any)
		}
		document[metadataProperty] = &meta

		// Emit partial-progress checkpoints, once enough documents have been emitted since
		// the previous checkpoint, and (if relevant) the row keys differ.
		if len(orderColumns) == 0 {
			// When we don't have a usable list of ordering columns, just unconditionally emit
			// a partial-progress checkpoint between every K documents.
			if documentsSinceLastCheckpoint >= partialProgressCheckpointSpacing {
				if err := c.emitCheckpoint(ctx, table, streamState); err != nil {
					return err
				}
				documentsSinceLastCheckpoint = 0
			}
		} else {
			// The document key hash is only relevant when we've exceeded the minimum spacing,
			// however we'll need to compute the hash value for the K-1th row before we start
			// comparing hashes at row K.
			var currentDocumentKeyHash = ""
			if documentsSinceLastCheckpoint >= partialProgressCheckpointSpacing-1 {
				var keysDocument = make(map[string]any)
				for _, orderColumn := range orderColumns {
					var val, ok = document[orderColumn]
					if !ok {
						return fmt.Errorf("ordering column %q not present in document", orderColumn)
					}
					keysDocument[orderColumn] = val
				}
				var hasher = sha256.New()
				var encoder = json.NewEncoder(hasher)
				if err := encoder.Encode(keysDocument); err != nil {
					return fmt.Errorf("internal error: failed to serialize keys document: %w", err)
				}
				currentDocumentKeyHash = string(hasher.Sum(nil))
			}

			// Emit a checkpoint in between rows with different collection keys (as determined
			// by comparing the computed key hashes) once K or more documents have been processed
			// since the previous checkpoint.
			//
			// The keys being different is important. Since we're requesting that Snowflake
			// order the result rows by that same set of columns but it's *not guaranteed* that
			// rows will be unique in those columns, we could see results with inconsistent row
			// ordering within the set of rows with a particular value, but *groups* of rows with
			// particular ordering-column values must occur in a consistent sequence, so by checking
			// and only emitting checkpoints between rows with different values for the `ORDER BY`
			// columns we guarantee that after a restart we can always pick up exactly where we left
			// off.
			//
			// And since the ordering key is just the columns corresponding to the Flow collection
			// key, it doesn't matter how many documents have the same key because Flow will reduce
			// them all together in memory as soon as they arrive.
			//
			// In the common case this is a bunch of unnecessary complexity and each row will have a
			// unique value for the ordering columns so we'll emit a checkpoint as soon as we reach
			// the minimum spacing. But doing it this way means that things should work correctly
			// even on atypical datasets.
			if documentsSinceLastCheckpoint >= partialProgressCheckpointSpacing && currentDocumentKeyHash != previousDocumentKeyHash {
				if err := c.emitCheckpoint(ctx, table, streamState); err != nil {
					return err
				}
				documentsSinceLastCheckpoint = 0
			}
			previousDocumentKeyHash = currentDocumentKeyHash
		}

		if err := c.emitDocument(ctx, table, document); err != nil {
			return err
		}

		streamState.TableOffset++
		documentsSinceLastCheckpoint++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error querying staging table %q: %w", stagingName.String(), err)
	}

	logEntry.WithField("off", streamState.TableOffset).Info("done querying staging table")
	streamState.TableOffset = 0
	streamState.SequenceNumber++
	if err := c.emitCheckpoint(ctx, table, streamState); err != nil {
		return err
	}
	return nil
}

func translateFieldValue(x any, ctype *sql.ColumnType) (any, error) {
	var dbType = ctype.DatabaseTypeName()
	switch x := x.(type) {
	case string:
		switch dbType {
		case "FIXED":
			_, scale, ok := ctype.DecimalSize()
			if !ok {
				return nil, fmt.Errorf("internal error: decimal size unknown")
			}
			if scale == 0 {
				return strconv.ParseInt(x, 10, 64)
			}
			return strconv.ParseFloat(x, 64)
		case "BOOLEAN":
			if x == "0" {
				return false, nil
			} else if x == "1" {
				return true, nil
			} else {
				return nil, fmt.Errorf("unexpected boolean value %#v", x)
			}
		case "REAL":
			return strconv.ParseFloat(x, 64)
		case "VARIANT", "OBJECT", "ARRAY":
			if json.Valid([]byte(x)) {
				return json.RawMessage(x), nil
			}
			return x, nil
		}
	}
	return x, nil
}

func (c *capture) emitDocument(ctx context.Context, table snowflakeObject, document any) error {
	var binding = c.Bindings[table]
	if binding == nil {
		return fmt.Errorf("internal error: unknown binding for table %q", table.String())
	}
	var bs, err = json.Marshal(document)
	if err != nil {
		log.WithFields(log.Fields{
			"doc":   fmt.Sprintf("%#v", document),
			"table": table.String(),
			"err":   err,
		}).Error("document serialization error")
		return fmt.Errorf("error serializing document from table %q: %w", table.String(), err)
	}
	return c.Output.Documents(binding.Index, bs)
}

func (c *capture) emitCheckpoint(ctx context.Context, table snowflakeObject, state *streamState) error {
	var updateStreams = map[snowflakeObject]*streamState{table: state}
	if updateCheckpoint, err := json.Marshal(&captureState{Streams: updateStreams}); err != nil {
		return fmt.Errorf("error marshalling checkpoint: %w", err)
	} else if err := c.Output.Checkpoint(updateCheckpoint, true); err != nil {
		return fmt.Errorf("error emitting checkpoint: %w", err)
	}
	return nil
}
