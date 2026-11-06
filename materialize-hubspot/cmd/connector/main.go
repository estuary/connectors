package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	hubspot "github.com/estuary/connectors/materialize-hubspot"
)

func main() {
	boilerplate.RunMain(new(hubspot.Driver))
}
