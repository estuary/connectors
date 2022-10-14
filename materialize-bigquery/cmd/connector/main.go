package main

import (
	"os"

	connector "github.com/estuary/connectors/materialize-bigquery"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.InfoLevel)
	boilerplate.RunMain(connector.Driver())
}
