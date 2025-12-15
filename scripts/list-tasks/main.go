// The list-tasks script enumerates all tasks using any variant of a particular
// connector, and optionally either fetches the task specs or adds the tasks to
// the active flowctl draft.
//
// It consults 'https://raw.githubusercontent.com/estuary/connectors/refs/heads/main/<connector>/VARIANTS'
// to get an updated list of all variant names of a particular connector, and then lists all matching
// tasks using a 'flowctl raw get' query like:
//
//	flowctl raw get --table live_specs --query select=catalog_name,connector_image_name \
//	  --query 'spec_type=eq.capture' \
//	  --query 'connector_image_name=in.("ghcr.io/estuary/source-mysql")' \
//	  --query 'catalog_name=like.acmeCo/*'
//
// This is primarily meant for bulk editing workflows, which are described in more detail in
// 'docs/feature_flags.md'.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The list-tasks script enumerates all tasks using any variant of a
particular connector, and optionally pulls the task specs or adds
them to the active flowctl draft.

Refer to 'docs/feature_flags.md' for more information on the intended
use-case and workflow.
`

var (
	logLevel = flag.String("log_level", "info", "The log level to print at.")

	taskType     = flag.String("type", "", "The type of catalog spec to list (typically 'capture' or 'materialization'). If unspecified the task listing will not be filtered by type.")
	imageName    = flag.String("connector", "", "The connector image name to filter on. Can be a full URL like 'ghcr.io/estuary/source-mysql' or a short name like 'source-mysql', and in the latter case the name will be expanded into a full URL including all variants. If unspecified the task list will not be filtered by connector.")
	namePrefix   = flag.String("prefix", "", "The task name prefix to filter on. If unspecified the task listing will not be filtered by name.")
	missingFlags = flag.String("missing", "", "A comma-separated list of feature flags. If specified only tasks with one or more flags unset will be listed/pulled.")

	addToDraft = flag.Bool("draft", false, "When true, all listed tasks will be added to the active flowctl draft.")

	pullSpecs = flag.Bool("pull", false, "When true, all listed tasks will be fetched using 'flowctl catalog pull-specs'. Output files are in a flattened directory structure which plays more nicely with the bulk publishing script.")
	outputDir = flag.String("dir", "./specs", "The directory to write specs under when using the '--pull' flag.")
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

	if err := performListing(context.Background()); err != nil {
		log.WithField("err", err).Fatal("error listing tasks")
	}
}

func performListing(ctx context.Context) error {
	var imageNames []string
	if strings.Contains(*imageName, "/") {
		imageNames = append(imageNames, *imageName)
	} else if variants, err := listVariants(*imageName); err != nil {
		return fmt.Errorf("error listing variants of connector %q: %w", *imageName, err)
	} else {
		imageNames = variants
	}

	var tasks, err = listTasks(ctx, *taskType, imageNames, *namePrefix)
	if err != nil {
		return fmt.Errorf("error listing tasks: %w", err)
	}
	sort.Slice(tasks, func(i, j int) bool { return strings.Compare(tasks[i].CatalogName, tasks[j].CatalogName) < 0 })
	for _, task := range tasks {
		if *missingFlags != "" {
			// Check whether the task spec already has all of the specified settings, and if so skip this task.
			var flagNames = strings.Split(*missingFlags, ",")
			if hasAllFlags, err := hasSettingsForAllFlags(task.Spec, flagNames); err != nil {
				return fmt.Errorf("error checking flag settings for task %q: %w", task.CatalogName, err)
			} else if hasAllFlags {
				log.WithField("task", task.CatalogName).Info("task already has settings for all flags, skipping")
				continue
			}
		}
		if *addToDraft {
			if err := addTaskToDraft(ctx, task.CatalogName); err != nil {
				return fmt.Errorf("error adding task %q to flowctl draft: %w", task.CatalogName, err)
			}
			fmt.Printf("added %q to draft\n", task.CatalogName)
		}
		if *pullSpecs && task.CatalogName != "-----/aman-dummy/source-mongodb" {
			if err := pullTaskSpec(ctx, task.CatalogName); err != nil {
				return fmt.Errorf("error pulling spec for task %q: %w", task.CatalogName, err)
			}
			fmt.Printf("pulled spec for %q\n", task.CatalogName)
		}
		if !*addToDraft && !*pullSpecs {
			fmt.Println(task.CatalogName)
		}
	}
	return nil
}

type taskSpec struct {
	CatalogName    string          `json:"catalog_name"`
	ConnectorImage string          `json:"connector_image_name"`
	Spec           json.RawMessage `json:"spec"`
}

func listTasks(ctx context.Context, taskType string, imageNames []string, namePrefix string) ([]*taskSpec, error) {
	var command = []string{"flowctl", "raw", "get", "--table=live_specs", "--query", "select=catalog_name,connector_image_name,spec"}

	if taskType != "" {
		command = append(command, "--query", fmt.Sprintf("spec_type=eq.%s", taskType))
	}
	if *imageName != "" {
		command = append(command, "--query", fmt.Sprintf("connector_image_name=in.(%s)", strings.Join(imageNames, ",")))
	}
	if namePrefix != "" {
		command = append(command, "--query", fmt.Sprintf("catalog_name=like.%s*", namePrefix))
	}

	log.WithField("command", command).Info("executing task listing command")
	var bs, err = exec.CommandContext(ctx, command[0], command[1:]...).Output()
	if err != nil {
		return nil, fmt.Errorf("error querying live specs: %w", err)
	}
	var tasks []*taskSpec
	var failureInfo struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(bs, &tasks); err == nil {
		return tasks, nil
	} else if json.Unmarshal(bs, &failureInfo) == nil && failureInfo.Message != "" {
		return nil, fmt.Errorf("error querying live specs: %s", failureInfo.Message)
	} else {
		return nil, fmt.Errorf("error parsing live specs query results: %w", err)
	}
}

func listVariants(connectorName string) ([]string, error) {
	var variantsURL = fmt.Sprintf("https://raw.githubusercontent.com/estuary/connectors/refs/heads/main/%s/VARIANTS", connectorName)
	var variants = []string{fmt.Sprintf("ghcr.io/estuary/%s", connectorName)}

	resp, err := http.Get(variantsURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching variants list: %w", err)
	}
	defer resp.Body.Close()

	// Return just the primary name if VARIANTS file doesn't exist
	if resp.StatusCode == http.StatusNotFound {
		return variants, nil
	}

	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error fetching variants list: %w", err)
	}

	for _, v := range strings.Split(string(bs), "\n") {
		variants = append(variants, fmt.Sprintf("ghcr.io/estuary/%s", v))
	}
	return variants, nil
}

func addTaskToDraft(ctx context.Context, taskName string) error {
	return flowctl(ctx, "catalog", "draft", "--name", taskName)
}

func pullTaskSpec(ctx context.Context, taskName string) error {
	// Most of the work this function does is related to computing a target
	// directory name to write the spec file(s) into. This is so that later
	// on the bulk-publishing tool can assume every 'flow.yaml' file is
	// definitely a single isolated task that can be published independently.
	var taskHash = fmt.Sprintf("%x", sha256.Sum256([]byte(taskName)))
	var dirName = taskName
	dirName = dirName[:strings.LastIndex(dirName, "/")] // Strip the final /connector-name component of the task name
	dirName = strings.ReplaceAll(dirName, "/", "_")     // Replace slashes with underscores to get a single level directory hierarchy
	dirName = dirName + "_" + taskHash[:8]              // Add a unique hash to the directory name just so there definitely aren't any collisions
	var targetDir = path.Join(*outputDir, dirName)

	// Make sure the target directory and its parent(s) exist, then pull the task spec.
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("error creating target directory: %w", err)
	}
	return flowctl(ctx, "catalog", "pull-specs", "--name", taskName, "--overwrite", "--flat", "--target", targetDir+"/flow.yaml")
}

func flowctl(ctx context.Context, args ...string) error {
	log.WithField("command", args).Debug("executing flowctl command")
	var _, err = exec.CommandContext(ctx, "flowctl", args...).Output()
	return err
}

func hasSettingForFlag(flagsSetting string, flagName string) bool {
	flagName = strings.TrimPrefix(flagName, "no_")
	for _, flag := range strings.Split(flagsSetting, ",") {
		flag = strings.TrimSpace(flag)
		if flag == flagName || flag == "no_"+flagName {
			return true
		}
	}
	return false
}

func hasSettingsForAllFlags(spec json.RawMessage, flagNames []string) (hasAllFlags bool, err error) {
	// Extract the 'endpoint.connector.config.advanced.feature_flags' string property from the task spec,
	// and check if it contains a setting for all specified flag names.
	var taskFlags = extractStringProperty(spec, "endpoint", "connector", "config", "advanced", "feature_flags")
	for _, flagName := range flagNames {
		if !hasSettingForFlag(taskFlags, flagName) {
			return false, nil
		}
	}
	return true, nil
}

func extractStringProperty(obj json.RawMessage, pathComponents ...string) string {
	for _, component := range pathComponents {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(obj, &m); err != nil {
			return ""
		} else if next, ok := m[component]; !ok {
			return ""
		} else {
			obj = next
		}
	}
	var s string
	if err := json.Unmarshal(obj, &s); err != nil {
		return ""
	}
	return s
}
