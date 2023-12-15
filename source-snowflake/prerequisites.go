package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

func (snowflakeDriver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg = new(config)
	if err := pf.UnmarshalStrict(req.ConfigJson, cfg); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	cfg.SetDefaults()

	var db, err = connectSnowflake(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to snowflake: %w", err)
	}
	defer db.Close()

	if _, err := performSnowflakeDiscovery(ctx, cfg, db); err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}

	var errs = setupPrerequisites(ctx, cfg, db)
	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}
		res.SetDefaults()

		if err := setupTablePrerequisites(ctx, cfg, db, snowflakeObject{res.Schema, res.Table}); err != nil {
			errs = append(errs, err)
			continue
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Schema, res.Table},
		})
	}
	if len(errs) > 0 {
		e := &prerequisitesError{errs}
		return nil, cerrors.NewUserError(nil, e.Error())
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

type prerequisitesError struct {
	errs []error
}

func (e *prerequisitesError) Error() string {
	var b = new(strings.Builder)
	fmt.Fprintf(b, "the capture cannot run due to the following error(s):")
	for _, err := range e.errs {
		b.WriteString("\n - ")
		b.WriteString(err.Error())
	}
	return b.String()
}

func (e *prerequisitesError) Unwrap() []error {
	return e.errs
}

func setupPrerequisites(ctx context.Context, cfg *config, db *sql.DB) []error {
	var errs []error
	for _, prereq := range []func(ctx context.Context, cfg *config, db *sql.DB) error{
		prerequisiteFlowSchema,
	} {
		if err := prereq(ctx, cfg, db); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func prerequisiteFlowSchema(ctx context.Context, cfg *config, db *sql.DB) error {
	var createSchemaQuery = fmt.Sprintf(
		`CREATE TRANSIENT SCHEMA IF NOT EXISTS %s COMMENT = 'Schema containing Flow streams and staging tables';`,
		quoteSnowflakeIdentifier(cfg.Advanced.FlowSchema),
	)
	if _, err := db.ExecContext(ctx, createSchemaQuery); err != nil {
		return fmt.Errorf("error creating Flow schema: %w", err)
	}
	return nil
}

func setupTablePrerequisites(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject) error {
	for _, prereq := range []func(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject) error{
		prerequisiteTableReadable,
		prerequisiteTableChangeStream,
	} {
		if err := prereq(ctx, cfg, db, table); err != nil {
			return err
		}
	}
	return nil
}

func prerequisiteTableReadable(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject) error {
	var quotedTableName = table.QuotedName()
	var testReadQuery = fmt.Sprintf(`SELECT 1 FROM %s WHERE 1 = 0;`, quotedTableName)
	log.WithField("query", testReadQuery).Debug("verifying that table is readable")
	if _, err := db.ExecContext(ctx, testReadQuery); err != nil {
		return fmt.Errorf("unable to read from table %s: %w", quotedTableName, err)
	}
	return nil
}

func prerequisiteTableChangeStream(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject) error {
	var tableName = table.QuotedName()
	var changeStreamName = changeStreamName(cfg, table).QuotedName()
	var createStreamQuery = fmt.Sprintf(
		`CREATE STREAM IF NOT EXISTS %s ON TABLE %s;`,
		changeStreamName,
		tableName,
	)
	log.WithField("query", createStreamQuery).Info("ensuring change stream exists")
	if _, err := db.ExecContext(ctx, createStreamQuery); err != nil {
		return fmt.Errorf("error creating stream %s for table %s: %w", changeStreamName, tableName, err)
	}
	return nil
}
