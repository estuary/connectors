package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-s3-iceberg"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
