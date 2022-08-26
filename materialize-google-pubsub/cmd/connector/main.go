package main

import (
	"os"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-google-pubsub"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.InfoLevel)
	boilerplate.RunMain(connector.Driver())
}
