package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-clickhouse"
)

func main() {
	boilerplate.RunMain(connector.NewDriver())
}
