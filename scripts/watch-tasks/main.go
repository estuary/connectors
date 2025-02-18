// The watch-tasks script keeps an eye on the error/warning counts in
// the task statistics table and logs when any of them increase. It can
// be used in conjunction with list-tasks to monitor the health of a
// particular connector during a potentially risky release.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The watch-tasks script accepts a list of task names on the command
line and repeatedly polls Supabase for their statistics.

Usage example:
$ watch-tasks.sh $(list-tasks.sh --connector=source-mysql)
`

var (
	logLevel = flag.String("log_level", "info", "The log level to print at.")
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

	if err := watchTasks(context.Background(), flag.Args()); err != nil {
		log.WithField("err", err).Fatal("error watching tasks")
	}
}

func watchTasks(ctx context.Context, taskNames []string) error {
	var previousTaskStats, err = queryTaskStats(ctx, taskNames)
	if err != nil {
		return fmt.Errorf("error querying task stats: %w", err)
	}
	for ctx.Err() == nil {
		time.Sleep(60 * time.Second)
		log.WithField("count", len(taskNames)).Info("polling task stats for changes")
		var taskStats, err = queryTaskStats(ctx, taskNames)
		if err != nil {
			log.WithField("err", err).Error("error querying task stats")
			continue
		}
		compareStats(previousTaskStats, taskStats)
		previousTaskStats = taskStats
	}
	return ctx.Err()
}

const (
	failureCountThreshold = 10
	errorCountThreshold   = 10
	warningCountThreshold = 10
)

func compareStats(previous, current map[string]*taskStatistics) {
	for taskName, currentStats := range current {
		previousStats, ok := previous[taskName]
		if !ok {
			log.WithField("task", taskName).Info("task added")
			continue
		}
		if currentStats.FailuresCount > previousStats.FailuresCount && previousStats.FailuresCount < failureCountThreshold {
			log.WithFields(log.Fields{
				"task":          taskName,
				"failuresCount": currentStats.FailuresCount,
			}).Warn("failures count increased")
		} else if currentStats.ErrorsCount > previousStats.ErrorsCount && previousStats.ErrorsCount < errorCountThreshold {
			log.WithFields(log.Fields{
				"task":       taskName,
				"errorCount": currentStats.ErrorsCount,
			}).Warn("error count increased")
		} else if currentStats.WarningsCount > previousStats.WarningsCount && previousStats.WarningsCount < warningCountThreshold {
			log.WithFields(log.Fields{
				"task":         taskName,
				"warningCount": currentStats.WarningsCount,
			}).Warn("warning count increased")
		}
	}
}

type taskStatistics struct {
	CatalogName   string
	BytesMoved    int
	WarningsCount int
	ErrorsCount   int
	FailuresCount int
}

func queryTaskStats(ctx context.Context, taskNames []string) (map[string]*taskStatistics, error) {
	var taskStats = make(map[string]*taskStatistics)

	// Query tasks in batches of 10 to avoid hitting maximums.
	for idx := 0; idx < len(taskNames); idx += 10 {
		var end = idx + 10
		if end > len(taskNames) {
			end = len(taskNames)
		}

		command := []string{
			"flowctl", "raw", "get",
			"--table=catalog_stats",
			"--query", "select=catalog_name,bytes_written_by_me,bytes_read_by_me,warnings,errors,failures",
			"--query", "grain=eq.daily",
			"--query", fmt.Sprintf("ts=gte.%s", time.Now().Add(-24*time.Hour).Format(time.RFC3339)),
			"--query", fmt.Sprintf("catalog_name=in.(%s)", strings.Join(taskNames[idx:end], ",")),
		}

		log.WithField("command", command).Debug("executing stats query command")
		bs, err := exec.CommandContext(ctx, command[0], command[1:]...).Output()
		if err != nil {
			return nil, fmt.Errorf("error querying task stats: %w", err)
		}

		var tasks []struct {
			CatalogName      string `json:"catalog_name"`
			BytesWrittenByMe int    `json:"bytes_written_by_me"`
			BytesReadByMe    int    `json:"bytes_read_by_me"`
			Warnings         int    `json:"warnings"`
			Errors           int    `json:"errors"`
			Failures         int    `json:"failures"`
		}
		if err := json.Unmarshal(bs, &tasks); err != nil {
			return nil, fmt.Errorf("error parsing task stats: %w", err)
		}

		for _, t := range tasks {
			stats := &taskStatistics{
				CatalogName:   t.CatalogName,
				BytesMoved:    t.BytesWrittenByMe + t.BytesReadByMe,
				WarningsCount: t.Warnings,
				ErrorsCount:   t.Errors,
				FailuresCount: t.Failures,
			}
			log.WithField("stats", stats).Debug("got task stats")
			taskStats[t.CatalogName] = stats
		}
	}
	return taskStats, nil
}
