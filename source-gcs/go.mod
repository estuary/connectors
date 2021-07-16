module github.com/estuary/connectors/source-gcs

go 1.16

require (
	cloud.google.com/go/storage v1.16.0
	github.com/estuary/connectors/go-types v0.0.0
	google.golang.org/api v0.49.0
)

replace github.com/estuary/connectors/go-types => ../go-types
