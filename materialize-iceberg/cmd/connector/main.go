package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-iceberg"
)

func main() {
	boilerplate.RunMain(new(connector.Driver))
}
