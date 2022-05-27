package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
	log "github.com/sirupsen/logrus"
)

// bigQueryOmitFromTransactionComment will be prepended to every statement such that the
// ExecuteStatements function can quickly detect it and ensure that is not run inside
// of a transaction.
const bigQueryOmitFromTransactionComment = "--**bigQueryOmitFromTransaction**\n"

// Endpoint is a BigQuery specific implementation of an estuary/protocols/materialize/sql.Endpoint.
type Endpoint struct {
	config *config
	// Bigquery Client
	bigQueryClient *bigquery.Client
	// Cloud Storage Client
	cloudStorageClient *storage.Client
	// Generator of SQL for this endpoint.
	generator sqlDriver.Generator
	// FlowTables
	flowTables sqlDriver.FlowTables
}

// Generator returns the Generator.
func (e *Endpoint) Generator() *sqlDriver.Generator {
	return &e.generator
}

// FlowTables returns the Flow Tables.
func (e *Endpoint) FlowTables() *sqlDriver.FlowTables {
	return &e.flowTables
}

// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func (e *Endpoint) LoadSpec(ctx context.Context, materialization pf.Materialization) (string, *pf.MaterializationSpec, error) {

	var spec = new(pf.MaterializationSpec)
	job, err := e.query(ctx, fmt.Sprintf(
		"SELECT version, spec FROM %s WHERE materialization=%s;",
		e.flowTables.Specs.Identifier,
		e.generator.Placeholder(0),
	), materialization.String())

	if err == errNotFound {
		log.WithFields(log.Fields{
			"table": e.flowTables.Specs.Identifier,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return "", nil, nil
	} else if err != nil {
		return "", nil, fmt.Errorf("query: %w", err)
	}

	var data struct {
		Version string `bigquery:"version"`
		SpecB64 string `bigquery:"spec"`
	}

	if err := e.fetchOne(ctx, job, &data); err == errNotFound {
		log.WithFields(log.Fields{
			"table": e.flowTables.Specs.Identifier,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return "", nil, nil
	} else if err != nil {
		return "", nil, fmt.Errorf("scan: %w", err)
	} else if specBytes, err := base64.StdEncoding.DecodeString(data.SpecB64); err != nil {
		return data.Version, nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return data.Version, nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return data.Version, nil, fmt.Errorf("validating spec: %w", err)
	}

	return data.Version, spec, nil
}

// ExecuteStatements takes a slice of SQL statements and executes them as a single transaction (if possible)
// and rolls back in case of failure. BigQuery cannot run all statements in transactions. Specifically
// Table creation/altering statements. These statements are tagged by the CreateTable function and are
// automatically excluded from transactions.
func (e *Endpoint) ExecuteStatements(ctx context.Context, statements []string) error {

	// Build a queue of statements than can be run in a transaction.
	var statementQueue []string
	var runStatementQueue = func() error {
		if len(statementQueue) == 0 {
			return nil
		}
		// Build the transaction.
		var script strings.Builder
		script.WriteString("BEGIN TRANSACTION;")
		for _, statement := range statementQueue {
			script.WriteString(statement)
		}
		script.WriteString("COMMIT TRANSACTION;")

		log.WithField("sql", script.String()).Debug("executing transaction")

		// Execute the transaction.
		_, err := e.query(ctx, script.String())
		if err != nil {
			return fmt.Errorf("query: %v", err)
		}

		// Truncate the queue.
		statementQueue = statementQueue[:0]

		return nil
	}

	// Loop through all statements requested to be run.
	for _, statement := range statements {
		// Does this statement contain the omit from transaction tag?
		if strings.HasPrefix(statement, bigQueryOmitFromTransactionComment) {

			// Process the statementQueue if anything is in there already.
			if err := runStatementQueue(); err != nil {
				return err
			}

			// Run the statement by itself.
			log.WithField("sql", statement).Debug("executing statement")
			_, err := e.query(ctx, statement)
			if err != nil {
				return fmt.Errorf("query: %v", err)
			}

		} else {
			// It's safe to include inside of a transaction, append it to the queue.
			statementQueue = append(statementQueue, statement)
		}
	}

	// Process the statementQueue if anything is remaining.
	return runStatementQueue()

}

// CreateTableStatement generates a CREATE TABLE statement for the given table. The returned
// statement must not contain any parameter placeholders.
func (e *Endpoint) CreateTableStatement(table *sqlDriver.Table) (string, error) {
	var builder strings.Builder

	// Write the comment identifying this as a non-transactionable statement since BigQuery
	// does not allow create table statements inside of transactions. As long as this
	// is the very first part of a statement, the whole statement will be excluded from
	// a transaction.
	builder.WriteString(bigQueryOmitFromTransactionComment)

	if len(table.Comment) > 0 {
		_, _ = e.generator.CommentRenderer.Write(&builder, table.Comment, "")
	}

	builder.WriteString("CREATE ")
	if table.Temporary {
		builder.WriteString("TEMPORARY ")
	}

	// Ignoring the table.IfNotExists because we need to
	// include it here otherwise, the statement would fail if the table exists
	// and since create gets called multiple time, if one table exists and another doesn't,
	// bigquery's table aren't fully configured and the connector fails when starting the transactor.
	builder.WriteString("TABLE IF NOT EXISTS")

	builder.WriteString(table.Identifier)
	builder.WriteString(" (\n\t")

	for i, column := range table.Columns {
		if i > 0 {
			builder.WriteString(",\n\t")
		}
		if len(column.Comment) > 0 {
			_, _ = e.generator.CommentRenderer.Write(&builder, column.Comment, "\t")
			// The comment will always end with a newline, but we'll need to add the indentation
			// for the next line. If there's no comment, then the indentation will already be there.
			builder.WriteRune('\t')
		}
		builder.WriteString(column.Identifier)
		builder.WriteRune(' ')

		var resolved, err = e.generator.TypeMappings.GetColumnType(&column)
		if err != nil {
			return "", err
		}
		builder.WriteString(resolved.SQLType)
	}
	// Close the create table paren.
	builder.WriteString(")")

	// Create clustering keys based on table primary keys.
	var pkIdentifiers []string
	for _, column := range table.Columns {
		if column.PrimaryKey {
			pkIdentifiers = append(pkIdentifiers, column.Identifier)
			// BigQuery only allows a maximum of 4 columns for clustering.
			if len(pkIdentifiers) >= 4 {
				break
			}
		}
	}
	if len(pkIdentifiers) > 0 {
		builder.WriteString("\nCLUSTER BY ")
		builder.WriteString(strings.Join(pkIdentifiers, ","))
	}

	builder.WriteRune(';')
	return builder.String(), nil
}

// NewFence installs and returns a new *Fence. On return, all older fences of
// this |shardFqn| have been fenced off from committing further transactions.
func (ep *Endpoint) NewFence(ctx context.Context, materialization pf.Materialization, keyBegin, keyEnd uint32) (sqlDriver.Fence, error) {

	job, err := ep.query(ctx, fmt.Sprintf(`
	-- Our desired fence
	DECLARE vMaterialization STRING DEFAULT %s;
	DECLARE vKeyBegin INT64 DEFAULT %s;
	DECLARE vKeyEnd INT64 DEFAULT %s;

	-- The current values
	DECLARE curFence INT64;
	DECLARE curKeyBegin INT64;
	DECLARE curKeyEnd INT64;
	DECLARE curCheckpoint STRING;

	BEGIN TRANSACTION;

	-- Increment the fence value of _any_ checkpoint which overlaps our key range.
	-- This will force them to fail any updates and shutdown.
	UPDATE `+ep.flowTables.Checkpoints.Identifier+`
		SET fence=fence+1
		WHERE materialization = vMaterialization
		AND key_end >= vKeyBegin
		AND key_begin <= vKeyEnd
	;

	-- Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	SET (curFence, curKeyBegin, curKeyEnd, curCheckpoint) = (
		SELECT AS STRUCT fence, key_begin, key_end, checkpoint
			FROM `+ep.flowTables.Checkpoints.Identifier+`
			WHERE materialization = vMaterialization
			AND key_begin <= vKeyBegin
			AND key_end >= vKeyEnd
			ORDER BY key_end - key_begin ASC
			LIMIT 1
	);

	-- Create a new fence if none exists.
	IF curFence IS NULL THEN
		SET curFence = 1;
		SET curKeyBegin = 1;
		SET curKeyEnd = 0;
		SET curCheckpoint = %s;
	END IF;

	-- If any of the key positions don't line up, create a new fence.
	-- Either it's new or we are starting a split shard.
	IF vKeyBegin <> curKeyBegin OR vKeyEnd <> curKeyEnd THEN
		INSERT INTO `+ep.flowTables.Checkpoints.Identifier+` (materialization, key_begin, key_end, checkpoint, fence)
		VALUES (vMaterialization, vKeyBegin, vKeyEnd, curCheckpoint, curFence);
	END IF;

	COMMIT TRANSACTION;

	-- Get the current value
	SELECT curFence AS fence, curCheckpoint AS checkpoint;
	`,
		ep.generator.Placeholder(0),
		ep.generator.Placeholder(1),
		ep.generator.Placeholder(2),
		ep.generator.Placeholder(3),
	),
		materialization,
		keyBegin,
		keyEnd,
		base64.StdEncoding.EncodeToString(pm.ExplicitZeroCheckpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("query fence: %w", err)
	}

	// Fetch and decode the fence information.
	var bqFence struct {
		Fence      int64  `bigquery:"fence"`
		Checkpoint string `bigquery:"checkpoint"`
	}

	if err = ep.fetchOne(ctx, job, &bqFence); err != nil {
		return nil, fmt.Errorf("read fence: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(bqFence.Checkpoint)
	if err != nil {
		return nil, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	return &fence{
		checkpoint:      checkpoint,
		fence:           bqFence.Fence,
		materialization: materialization,
		keyBegin:        keyBegin,
		keyEnd:          keyEnd,
	}, nil

}
