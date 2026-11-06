package main

import (
	connector "github.com/estuary/connectors/materialize-bigtable"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
