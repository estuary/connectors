package main

import (
	"context"

	"github.com/estuary/connectors/materialize-tester/mattest"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type Config struct {
	mattest.Config
	Log mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
}

func main() {

	var config Config
	var parser = flags.NewParser(&config, flags.HelpFlag|flags.PassDoubleDash)
	mbp.MustParseArgs(parser)
	mbp.InitLog(config.Log)

	ctx := context.Background()
	if err := config.Config.Run(ctx); err != nil {
		log.Fatalf("failed: %v", err)
	}
}
