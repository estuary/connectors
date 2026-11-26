package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	_ "github.com/sijms/go-ora/v2"
)

type oracleDriver struct {
	*sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = (*oracleDriver)(nil)

func NewDriver() *oracleDriver {
	return &oracleDriver{Driver: &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-oracle",
		StartTunnel:      func(context.Context, config) error { return nil },
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			var dialect = createOracleDialect(cfg.DefaultNamespace())
			var templates = renderTemplates(dialect)
			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.DefaultNamespace()}),
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       prepareNewTransactor(templates),
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
			}, nil
		},
		PreReqs: preReqs,
	}}
}

func openDB(cfg config) (*stdsql.DB, error) {
	uri, err := cfg.ToURI()
	if err != nil {
		return nil, err
	}
	return stdsql.Open("oracle", uri.String())
}

func preReqs(ctx context.Context, cfg config, _ map[string]bool) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}
	db, err := openDB(cfg)
	if err != nil {
		errs.Err(err)
		return errs
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		errs.Err(fmt.Errorf("connecting to Oracle: %w", err))
		return errs
	}
	var charset string
	if err := db.QueryRowContext(ctx, "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_CHARACTERSET'").Scan(&charset); err != nil {
		errs.Err(fmt.Errorf("reading the database character set: %w", err))
	} else if err := checkCharacterSet(charset); err != nil {
		errs.Err(err)
	}
	return errs
}

// checkCharacterSet requires a Unicode database character set. VARCHAR2 and
// CLOB columns use it, and Oracle replaces characters it can't represent, so
// distinct keys could otherwise collide.
func checkCharacterSet(charset string) error {
	if charset == "AL32UTF8" || charset == "UTF8" {
		return nil
	}
	return fmt.Errorf("the database character set is %s, but the connector requires AL32UTF8 or UTF8 so that it can store any text", charset)
}
