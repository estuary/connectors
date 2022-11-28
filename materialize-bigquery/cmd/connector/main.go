package main

import (
	connector "github.com/estuary/connectors/materialize-bigquery"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

func main() {
	boilerplate.RunMain(connector.Driver())
}
