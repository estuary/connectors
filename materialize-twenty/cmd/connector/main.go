package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-twenty"
)

func main() {
	boilerplate.RunMain(new(connector.Driver))
}
