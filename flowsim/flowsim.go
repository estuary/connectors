package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/estuary/connectors/flowsim/matsim"
	"github.com/jessevdk/go-flags"
)

var (
	Version   string = "unknown"
	BuildDate string = "unknown"
)

func main() {

	var parser = flags.NewParser(nil, flags.Default)
	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	_ = addCmd(parser.Command, "version", "version",
		"Print version", &versionCmd{})

	var matsimCmd = addCmd(parser.Command, "matsim", "materialization simulator",
		"Run the materialization simulator", &struct{}{})
	matsim.AddCommand(ctx, matsimCmd, nil, nil)

	if _, err := parser.Parse(); err != nil {
		log.Fatal(err)
	}

}

type versionCmd struct{}

// Execute displays the version and exits.
func (versionCmd) Execute(_ []string) error {
	fmt.Printf("%s - %s\n", Version, BuildDate)
	return nil
}

func addCmd(to interface {
	AddCommand(string, string, string, interface{}) (*flags.Command, error)
}, a, b, c string, iface interface{}) *flags.Command {
	var cmd, err = to.AddCommand(a, b, c, iface)
	if err != nil {
		panic(err)
	}
	return cmd
}
