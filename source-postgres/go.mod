module github.com/estuary/connectors/source-postgres

go 1.16

require (
	github.com/estuary/connectors/go-types v0.0.0
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pglogrepl v0.0.0-20210731151948-9f1effd582c4
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/jackc/pgtype v1.8.1
	github.com/jackc/pgx/v4 v4.13.0
	github.com/pkg/errors v0.8.1
)

replace github.com/estuary/connectors/go-types => ../go-types
