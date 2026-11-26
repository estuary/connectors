package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-cosmosdb-nosql"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
