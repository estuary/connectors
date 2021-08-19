module github.com/estuary/connectors/source-postgres

go 1.16

require (
	github.com/estuary/connectors/go-types v0.0.0
	github.com/jackc/pgx/v4 v4.13.0
	github.com/pkg/errors v0.8.1
)

replace github.com/estuary/connectors/go-types => ../go-types
