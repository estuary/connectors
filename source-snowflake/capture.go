package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

const (
	initialCloneSequenceNumber = 0

	// Emit a checkpoint after every N change documents, so that partial
	// progress can be made across connector restarts.
	partialProgressCheckpointSpacing = 50000
)

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
	} else {
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

		var table = snowflakeObject{res.Schema, res.Table}
		log.WithField("table", table.String()).Info("configured binding")
		if err := setupTablePrerequisites(ctx, cfg, db, table); err != nil {
			prerequisiteErrs = append(prerequisiteErrs, err)
			continue
		}
		bindings[table] = &captureBinding{Index: idx, Table: table}
	}
	if len(prerequisiteErrs) > 0 {
		return cerrors.NewUserError(nil, (&prerequisitesError{prerequisiteErrs}).Error())
	}

	var c = &capture{
		Config:   cfg,
		DB:       db,
		State:    checkpoint,
		Bindings: bindings,
		Output:   stream,
	}
	return c.Run(ctx)
}

type capture struct {
	Config   *config
	DB       *sql.DB
	State    *captureState
	Bindings map[snowflakeObject]*captureBinding
	Output   *boilerplate.PullOutput
}

type captureBinding struct {
	Index int
	Table snowflakeObject
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
	log.Info("processing added/removed bindings")
	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error processing added/removed bindings: %w", err)
	}

	if err := c.pruneDeletedStreams(ctx); err != nil {
		return fmt.Errorf("error pruning deleted streams: %w", err)
	}

	var highestStaged, err = c.pruneStagingTables(ctx)
	if err != nil {
		return fmt.Errorf("error pruning staging tables: %w", err)
	}

	for table, streamState := range c.State.Streams {
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
			if err := c.captureStagingTable(ctx, table); err != nil {
				return fmt.Errorf("error capturing staging table %d: %w", streamState.SequenceNumber, err)
			}
		}
	}
	return nil
}

func (c *capture) updateState(ctx context.Context) error {
	var updatedStreams = make(map[snowflakeObject]*streamState)

	// Prune deleted streams from the state.
	for table := range c.State.Streams {
		if _, ok := c.Bindings[table]; !ok {
			log.WithField("table", table.String()).Info("binding removed")
			delete(c.State.Streams, table)
			updatedStreams[table] = nil
		}
	}

	// Identify newly added streams, clone initial table contents, and initialize stream state.
	for _, binding := range c.Bindings {
		if _, ok := c.State.Streams[binding.Table]; !ok {
			log.WithField("table", binding.Table.String()).Info("binding added")
			var stagingName, err = createInitialCloneTable(ctx, c.Config, c.DB, binding.Table, initialCloneSequenceNumber)
			if err != nil {
				return err
			}
			log.WithField("staged", stagingName.String()).Debug("cloned into staging table")
			var state = &streamState{SequenceNumber: initialCloneSequenceNumber}
			c.State.Streams[binding.Table] = state
			updatedStreams[binding.Table] = state
		}
	}

	// Emit a new checkpoint patch reflecting these deletions and initializations.
	if updateCheckpoint, err := json.Marshal(&captureState{Streams: updatedStreams}); err != nil {
		return fmt.Errorf("error marshalling checkpoint: %w", err)
	} else if err := c.Output.Checkpoint(updateCheckpoint, true); err != nil {
		return fmt.Errorf("error emitting checkpoint: %w", err)
	}
	return nil
}

func (c *capture) pruneDeletedStreams(ctx context.Context) error {
	// TODO(wgd): Add pruning of deleted streams, after contemplating whether it
	// should be possible to have multiple capture tasks from the same Snowflake
	// database and whether it's acceptable to only prune them when the binding
	// is removed.
	//
	// Optionally, we might just not bother with this and leave the stream around
	// when a binding is disabled.

	// log.WithField("flowSchema", c.Config.Advanced.FlowSchema).Info("pruning deleted streams")
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
func (c *capture) pruneStagingTables(ctx context.Context) (map[snowflakeObject]int, error) {
	log.WithField("flowSchema", c.Config.Advanced.FlowSchema).Info("pruning staging tables")

	// Build a mapping from unique ID strings to the corresponding table name
	var idToSourceTable = make(map[string]snowflakeObject)
	for table := range c.State.Streams {
		idToSourceTable[table.UniqueID()] = table
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

		// A staging table should be dropped if there is no known source table to which it
		// corresponds (because the binding must have been deleted) or if its sequence number
		// is less than the current sequence number for that stream.
		var sourceTable, sourceTableKnown = idToSourceTable[uniqueID]
		if !sourceTableKnown {
			log.WithField("name", stagingTable.Name).Debug("will prune staging table from disabled binding")
			needsDeletion = append(needsDeletion, stagingTable.Name)
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

func (c *capture) captureStagingTable(ctx context.Context, table snowflakeObject) error {
	var streamState = c.State.Streams[table]
	var stagingName = stagingTableName(c.Config, table, streamState.SequenceNumber)
	var logEntry = log.WithFields(log.Fields{
		"table": table.String(),
		"seq":   streamState.SequenceNumber,
	})

	// Ensure that the staging table exists by creating it if necessary. Since
	// the creation is done IF NOT EXISTS this is a no-op if it already does.
	if _, err := createStagingTable(ctx, c.Config, c.DB, table, streamState.SequenceNumber); err != nil {
		return err
	}

	// Read contents of staging table and output to Flow.
	logEntry.WithField("off", streamState.TableOffset).Info("querying staging table")
	var readStagingTableQuery = fmt.Sprintf(`SELECT * FROM %s LIMIT NULL OFFSET %d;`, stagingName.QuotedName(), streamState.TableOffset)
	rows, err := c.DB.QueryContext(ctx, readStagingTableQuery)
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

	// Snowflake represents a row update as a pair of an insert and a delete with
	// the same `METADATA$ROW_ID` value and `METADATA$ISUPDATE` set to true. This
	// buffer is used to store partial halves of rows.
	//
	// The paired rows changes should normally be located one after the other so
	// this buffer should only hold one row-value at any given time.
	var updatesBuffer = make(map[string]map[string]any)

	for rows.Next() {
		if err := rows.Scan(vptrs...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}
		var fields = make(map[string]any)
		for idx, name := range cnames {
			fields[name] = vals[idx]
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
			document = make(map[string]interface{})
		}
		document["_meta"] = &meta

		if err := c.emitDocument(ctx, table, document); err != nil {
			return err
		}

		// TODO(wgd): Need to only emit partial-progress checkpoints when the current
		// document's collection key differs from the previous one.
		streamState.TableOffset++
		if streamState.TableOffset%partialProgressCheckpointSpacing == 0 {
			if err := c.emitCheckpoint(ctx, table, streamState); err != nil {
				return err
			}
		}

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

func (c *capture) emitDocument(ctx context.Context, table snowflakeObject, document any) error {
	var binding, ok = c.Bindings[table]
	if !ok {
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
