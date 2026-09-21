package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-postgres"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
