package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-snowflake"
)

func main() {
	boilerplate.RunMain(connector.NewConnector())
}
