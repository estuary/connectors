package main

import (
	"flag"
	"os"
	"runtime/debug"
	"testing"

	log "github.com/sirupsen/logrus"
)

var (
	dbAddress = flag.String("db_address", "localhost:5432", "The database server address")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err != nil {
			log.WithField("level", logLevel).Fatal("invalid log level")
		}
		log.SetLevel(level)
	}

	// Set a 900MiB memory limit, same as we use in production.
	debug.SetMemoryLimit(900 * 1024 * 1024)

	os.Exit(m.Run())
}
