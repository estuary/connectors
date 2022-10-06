package main

import (
	"math/rand"
	"os"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	connector "github.com/estuary/connectors/materialize-rockset"
	log "github.com/sirupsen/logrus"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

	boilerplate.RunMain(connector.NewRocksetDriver(), false)
}
