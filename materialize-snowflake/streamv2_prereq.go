package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// runtimePrereqDriver rejects the RPCs that carry a materialization spec when the spec
// asks for a write path the task's runtime cannot support, then delegates to the
// wrapped driver. Validate, which carries no spec of the task being published,
// warns instead.
//
// The runtime prerequisite check lives here rather than in a Materializer method
// because the runtime is a property of the task, and Apply and Open are the only
// RPCs that carry the task's spec.
type runtimePrereqDriver struct {
	*sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = runtimePrereqDriver{}

// NewRuntimePrereqDriver assembles the connector as it runs: the driver NewDriver builds,
// behind the runtime prerequisite check.
//
// The check is applied here rather than in cmd/connector so that it cannot be lost
// by a caller which builds the connector for itself, and NewDriver goes on
// returning the driver rather than this wrapper because the test helpers of
// materialize-sql take the concrete driver.
func NewRuntimePrereqDriver() boilerplate.Connector {
	return runtimePrereqDriver{NewDriver()}
}

// Validate warns while the operator is still making the change, which is the most
// a publication can do about the runtime: see missingRuntimeV2Warning for what the
// request does and does not carry. The warning is logged rather than returned
// because it cannot be told apart from a publish which is correct.
func (d runtimePrereqDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if warning := missingRuntimeV2Warning(req.ConfigJson, req.LastMaterialization); warning != "" {
		log.Warn(warning)
	}

	return d.Driver.Validate(ctx, req)
}

// Apply rejects ahead of the first session of a new specification, so a task
// whose runtime cannot support the configured write path fails before anything in
// Snowflake is created or altered for it.
func (d runtimePrereqDriver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := requireStreamingV2Runtime(req.Materialization); err != nil {
		return nil, err
	} else if err := requireStreamingV2RuntimeForState(req.Materialization, req.StateJson); err != nil {
		return nil, err
	} else if err := rejectOrphanedStreamV2Bindings(req.Materialization, req.StateJson); err != nil {
		return nil, err
	} else if err := adoptStreamV2StateKeys(ctx, req.Materialization); err != nil {
		return nil, err
	}

	return d.Driver.Apply(ctx, req)
}

// NewTransactor rejects at startup, covering the task whose runtime flag is
// turned off without its endpoint configuration being touched. Such a task must
// fail rather than fall back to a write path whose recovery assumptions no
// longer hold.
func (d runtimePrereqDriver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := requireStreamingV2Runtime(req.Materialization); err != nil {
		return nil, nil, nil, err
	} else if err := requireStreamingV2RuntimeForState(req.Materialization, req.StateJson); err != nil {
		return nil, nil, nil, err
	}

	return d.Driver.NewTransactor(ctx, req, be)
}

// missingRuntimeV2Warning reports what to tell an operator who is publishing the
// snowpipe_streaming_v2 write path onto a task which has not been running the v2
// runtime, and "" where there is nothing to tell them.
//
// Publication invokes Validate, whose request carries the endpoint configuration
// and the last published spec but no shard template of its own: the runtime the
// task is *about* to run under is not in it, and neither is anything else which
// implies it. So the only shard template a publish can be read against is the
// last one, which says what the task has been running rather than what it will
// run — enough to recognise a task being moved onto this write path, and not
// enough to reject one. A publish which adds the runtime flag and the feature
// flag together is indistinguishable here from one which forgets the runtime
// flag, and rejecting would break the first.
func missingRuntimeV2Warning(configJson json.RawMessage, last *pf.MaterializationSpec) string {
	var cfg config
	if err := json.Unmarshal(configJson, &cfg); err != nil {
		return ""
	}

	if !boilerplate.ParseFlags(cfg)[flagSnowpipeStreamingV2] {
		return ""
	} else if last == nil || boilerplate.IsMaterializationSpecRuntimeV2(last) {
		return ""
	}

	return fmt.Sprintf(
		"this task's last published specification did not run the v2 materialization runtime, which the %q feature flag requires: unless this publication also adds %q to the task's shards.flags, the task will be rejected at startup",
		flagSnowpipeStreamingV2, boilerplate.RuntimeV2FlagName,
	)
}

// rejectOrphanedStreamV2Bindings rejects a publication which moves a binding
// off the snowpipe_streaming_v2 write path, other than by the one exit this
// connector supports — see streamV2PathOrphaned for what an unsupported
// departure costs the binding, and streamV2Downgrade for the one it allows.
//
// The transactor rejects the same thing, and has to: the connector state a task
// runs on is its own, and a specification published before this check existed
// may already have made the move. But a task which is rejected is a task an
// operator has to notice, while a publication which is rejected is one they are already
// watching. Apply is the earliest RPC that can tell: it is the first to carry
// both the specification being published and the connector state the task has
// accumulated, which Validate does not.
//
// A state document this connector cannot read reports nothing rather than
// rejecting. The runtime owns that document's shape, and a binding whose write
// path cannot be established from it is one the transactor will establish for
// itself.
func rejectOrphanedStreamV2Bindings(spec *pf.MaterializationSpec, stateJson json.RawMessage) error {
	if spec == nil || len(stateJson) == 0 {
		return nil
	}

	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	} else if cfg.Credentials == nil {
		// Which the boilerplate's own validation reports, and better.
		return nil
	}

	var cp checkpoint
	if err := json.Unmarshal(stateJson, &cp); err != nil {
		return nil
	}

	var flagEnabled = boilerplate.ParseFlags(cfg)[flagSnowpipeStreamingV2]
	for _, binding := range spec.Bindings {
		var item = cp[binding.StateKey]
		if item == nil || streamsV2(&cfg, binding.DeltaUpdates, flagEnabled) {
			continue
		}
		var table = strings.Join(binding.ResourcePath, ".")
		if streamV2Downgrade(&cfg, binding.DeltaUpdates) {
			if warning := streamV2DowngradeWarning(table, item.StreamV2); warning != "" {
				log.Warn(warning)
			}
			continue
		}
		if err := streamV2PathOrphaned(table, item.StreamV2); err != nil {
			return err
		}
	}
	return nil
}

// adoptStreamV2StateKeys records this task and each binding's state key in the
// comment of every table that a streaming v2 binding appends to and that records
// no state key of its own.
//
// Only CreateTable stamps that comment, and a publication which moves a binding
// onto this write path creates no table: it adopts the table the binding has been
// materializing into all along. streamV2CheckStateKeyConflict reads a table with
// no state key as one that says nothing about which state key may append, so the
// backfill race that the comment guards runs unguarded on exactly those tables
// which were carried onto this write path in place.
//
// Apply is where this belongs. It runs once for the task, on the leader, before any
// shard opens a channel, so the comment is in place ahead of the first append
// rather than racing it. The write path itself issues no DDL.
//
// A comment which already records a state key is left exactly as it stands,
// whether it names this task or another one. That comment is the account the guard
// reads, and overwriting it would erase the very conflict the guard reports.
func adoptStreamV2StateKeys(ctx context.Context, spec *pf.MaterializationSpec) error {
	if spec == nil {
		return nil
	}

	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	} else if cfg.Credentials == nil {
		// Which the boilerplate's own validation reports, and better.
		return nil
	}

	var flags = boilerplate.ParseFlags(cfg)
	var streaming []*pf.MaterializationSpec_Binding
	for _, binding := range spec.Bindings {
		if streamsV2(&cfg, binding.DeltaUpdates, flags[flagSnowpipeStreamingV2]) {
			streaming = append(streaming, binding)
		}
	}
	if len(streaming) == 0 {
		return nil
	}

	dsn, err := cfg.toURI(true, spec.TaskName())
	if err != nil {
		return err
	}
	db, err := stdsql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("opening connection to record streaming v2 state keys: %w", err)
	}
	defer db.Close()

	var dialect = snowflakeDialect(cfg.Schema, cfg.TimestampType, flags)

	for _, binding := range streaming {
		if n := len(binding.ResourcePath); n != 1 && n != 2 {
			return fmt.Errorf("cannot record the streaming v2 state key of resource path %q: expected a table, or a schema and a table", strings.Join(binding.ResourcePath, "."))
		}

		// Through the locator, which folds an identifier the way the DDL that created
		// the table folded it: Snowflake stores an unquoted name in upper case, and a
		// resource names its table in whatever case its author wrote. A lookup by the
		// name as written matches nothing, and this function would then take its
		// "no such table" branch and skip a table it ought to adopt.
		var loc = dialect.TableLocator(binding.ResourcePath)
		var schema, table = loc.TableSchema, loc.TableName

		// Read directly rather than through streamV2QueryTableComment, which reports
		// a table that does not exist and a table with no comment as the same empty
		// string. They are not the same here: the first is a table CreateTable is
		// about to stamp, and the second is a table to adopt.
		var query = fmt.Sprintf(
			"SELECT COMMENT FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;",
			dialect.Identifier(cfg.Database),
		)
		var comment *string
		if err := db.QueryRowContext(ctx, query, schema, table).Scan(&comment); errors.Is(err, stdsql.ErrNoRows) {
			continue
		} else if err != nil {
			return fmt.Errorf("querying the comment of table %s.%s: %w", schema, table, err)
		}

		var existing string
		if comment != nil {
			existing = *comment
		}
		if _, ok := streamV2StateKeyFromTableComment(existing); ok {
			continue
		}

		var adopted = streamV2EmbedStateKeyInTableComment(existing, streamV2RecordedStateKey{
			materialization: spec.TaskName(),
			stateKey:        binding.StateKey,
		})
		var identifier = dialect.Identifier(schema, table)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"COMMENT ON TABLE %s IS %s;", identifier, dialect.Literal(adopted),
		)); err != nil {
			return fmt.Errorf("recording the state key of table %s: %w", identifier, err)
		}

		log.WithFields(log.Fields{
			"table":    identifier,
			"stateKey": binding.StateKey,
		}).Info("recorded the streaming v2 state key of a table this task adopted")
	}

	return nil
}

// requireStreamingV2Runtime reports whether the given task spec may run the
// snowpipe_streaming_v2 write path. That path appends rows to a channel as it
// stores them and resumes from Snowflake's committed offset token, which is only
// correct if an interrupted transaction is replayed identically — a guarantee
// the v2 runtime makes and the v1 runtime does not.
func requireStreamingV2Runtime(spec *pf.MaterializationSpec) error {
	// The runtime prerequisite check reads the spec before anything else validates
	// it, so an absent one is reported here rather than dereferenced.
	if spec == nil {
		return fmt.Errorf("request carries no materialization spec")
	}

	// The full configuration is validated by the boilerplate, which reports a
	// better error than this check could. Only the feature flags are read here.
	var cfg config
	if err := json.Unmarshal(spec.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	// A configuration asking for both write paths is reported as such even when
	// the runtime is also wrong, so that the operator is told about the mistake
	// they made rather than the one it implies.
	if err := cfg.validateStreamingFlags(); err != nil {
		return err
	}

	if !boilerplate.ParseFlags(cfg)[flagSnowpipeStreamingV2] {
		return nil
	} else if boilerplate.IsMaterializationSpecRuntimeV2(spec) {
		return nil
	}

	return fmt.Errorf(
		"the %q feature flag requires the v2 materialization runtime, which this task is not running: add %q to the task's shards.flags, or remove %q from the endpoint configuration's feature_flags",
		flagSnowpipeStreamingV2, boilerplate.RuntimeV2FlagName, flagSnowpipeStreamingV2,
	)
}

// requireStreamingV2RuntimeForState rejects a task that is not on the v2
// materialization runtime while its checkpoint still names snowpipe streaming v2
// channels for a binding the specification keeps. Dropping those channels replays
// the interrupted transaction, and only the v2 runtime replays it in order.
func requireStreamingV2RuntimeForState(spec *pf.MaterializationSpec, stateJson json.RawMessage) error {
	if spec == nil || len(stateJson) == 0 || boilerplate.IsMaterializationSpecRuntimeV2(spec) {
		return nil
	}

	var cp checkpoint
	if err := json.Unmarshal(stateJson, &cp); err != nil {
		return nil
	}

	for _, binding := range spec.Bindings {
		var item = cp[binding.StateKey]
		if item == nil {
			continue
		}
		var channels = streamV2ChannelNames(item.StreamV2)
		if len(channels) == 0 {
			continue
		}
		return fmt.Errorf(
			"the task's checkpoint still records the snowpipe_streaming_v2 channel(s) %s for %s, and this task is not running the v2 materialization runtime: dropping them requires that runtime, so add %q to the task's shards.flags, or restore %q to the endpoint configuration's feature_flags",
			strings.Join(channels, ", "), strings.Join(binding.ResourcePath, "."), boilerplate.RuntimeV2FlagName, flagSnowpipeStreamingV2,
		)
	}
	return nil
}
