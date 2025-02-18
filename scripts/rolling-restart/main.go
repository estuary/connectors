// The rolling-restart script accepts a list of task names on the command line and restarts
// each of them in turn at some specified interval.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The rolling-restart script accepts a list of task names on the command
line and restarts each of them in turn at some specified interval.
`

var (
	logLevel = flag.String("log_level", "info", "The log level to print at.")
	interval = flag.String("interval", "60s", "The desired interval between task restarts.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s\n", scriptDescription)
		fmt.Fprintf(flag.CommandLine.Output(), "usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if lvl, err := log.ParseLevel(*logLevel); err != nil {
		log.WithFields(log.Fields{"level": *logLevel, "err": err}).Fatal("invalid log level")
	} else {
		log.SetLevel(lvl)
	}
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		PadLevelText:  true,
	})

	if err := rollingRestart(context.Background(), flag.Args()); err != nil {
		log.Fatalf("error performing rolling restart: %v", err)
	}
}

func rollingRestart(ctx context.Context, taskNames []string) error {
	// Parse the interval duration
	duration, err := time.ParseDuration(*interval)
	if err != nil {
		return fmt.Errorf("invalid interval %q: %w", *interval, err)
	}

	// Restart tasks in sorted order for consistency
	sort.Strings(taskNames)

	log.WithFields(log.Fields{
		"count":    len(taskNames),
		"interval": duration,
	}).Info("restarting impacted tasks...")

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for i, taskName := range taskNames {
		if err := restartTask(ctx, taskName); err != nil {
			return fmt.Errorf("failed to restart task %q: %w", taskName, err)
		}

		// Wait for next tick if this isn't the last task
		if i < len(taskNames)-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				// Continue to next restart
			}
		}
	}

	log.Info("all tasks restarted")
	return nil
}

func restartTask(ctx context.Context, taskName string) error {
	log.WithField("task", taskName).Info("restarting task")

	cmd := exec.CommandContext(ctx, "flowctl-go", "shards", "unassign", "-l",
		fmt.Sprintf("estuary.dev/task-name=%s", taskName))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error running restart command: %v", err)
	}
	return nil
}
