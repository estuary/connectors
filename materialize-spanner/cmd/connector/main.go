package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-spanner"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
