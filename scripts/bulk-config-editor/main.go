// The bulk-config-editor script applies an edit operation to all endpoint configs in a
// local directory, using SOPS as necessary to manipulate encrypted configs.
//
// While the script is written to be fairly modular, it is designed to be used alongside
// the list-tasks script to bulk-edit all production tasks using a particular connector
// to have some new feature flag.
//
// The process for doing that is described in 'docs/feature_flags.md'
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pmezard/go-difflib/difflib"
	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The bulk-config-editor script applies an edit operation to all endpoint
configs in a local directory, using SOPS as necessary to manipulate
encrypted configs.

Refer to 'docs/feature_flags.md' for more information on the intended
use-case and workflow.
`

var (
	logLevel   = flag.String("log_level", "info", "The log level to print at.")
	configsDir = flag.String("dir", "./specs", "The directory beneath which to edit configs.")
	setFlag    = flag.String("set_flag", "", "A feature-flag setting to add to every task.")
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

	if err := performEdits(context.Background()); err != nil {
		log.WithField("err", err).Fatal("error")
	}
}

func performEdits(ctx context.Context) error {
	// List all task directories (dirs containing leaf flow.yaml files) and endpoint configs
	// (*.config.yaml files) under the current directory. This allows us to do a couple of
	// basic sanity checks to ensure that we'll be sucessful.
	//
	// The way you could end up with a task directory but no corresponding config file is if
	// that task's config is plaintext and also short enough to fall below the 512 byte threshold
	// for 'flowctl catalog pull-specs' to break it out into a separate file. An encrypted config
	// can never fall below this threshold because the SOPS stanza is around 700 bytes alone, and
	// in practice this almost never happens. If it does happen, you will need to manually edit
	// that task spec to break out the config into a separate file if you want to use this script
	// to modify it.
	var taskDirs, err = listTaskSpecDirs(*configsDir)
	if err != nil {
		return fmt.Errorf("error listing task specs: %w", err)
	}
	configFiles, err := listConfigFiles(*configsDir)
	if err != nil {
		return fmt.Errorf("error listing config files: %w", err)
	}
	if len(configFiles) == 0 {
		return fmt.Errorf("no files named '*.config.yaml' under the current directory")
	}
	if len(taskDirs) > len(configFiles) {
		// Find the directory which doesn't have a corresponding config file
		for _, taskDir := range taskDirs {
			var found bool
			for _, configFile := range configFiles {
				if strings.HasPrefix(configFile, taskDir) {
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("task dir %q has no corresponding config file\n", taskDir)
			}
		}
		return fmt.Errorf("found %d task directories but only %d endpoint configs", len(taskDirs), len(configFiles))
	}

	// Compute edits for each config file
	log.WithField("count", len(configFiles)).Info("editing config files")
	var errs []error
	for _, configFile := range configFiles {
		if err := editConfigFile(ctx, configFile); err != nil {
			errs = append(errs, fmt.Errorf("error editing config %q: %w", configFile, err))
		}
	}
	if len(errs) > 0 {
		fmt.Printf("failed to edit %d configs\n", len(errs))
		for _, err := range errs {
			fmt.Printf("%s\n", err.Error())
		}
	}
	var successCount = len(configFiles) - len(errs)
	fmt.Printf("edited %d/%d configs\n", successCount, len(configFiles))
	return nil
}

func editConfigFile(ctx context.Context, configFile string) error {
	// Read in and parse the config file
	var originalConfigBytes, err = os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	var originalConfig any
	if err := yaml.Unmarshal(originalConfigBytes, &originalConfig); err != nil {
		return fmt.Errorf("error parsing file: %w", err)
	}

	// Compute edits based on the parsed original config
	edits, err := computeEdits(originalConfig)
	if err != nil {
		return fmt.Errorf("error computing edits: %w", err)
	}

	// Apply edits, either using SOPS if the file is encrypted or by modifying
	// the parsed representation in-process if it's plaintext.
	var modifiedConfig any
	if _, ok := indexPath(originalConfig, "sops"); ok {
		log.WithField("name", configFile).Debug("editing SOPS encrypted config")
		if err := editEncryptedTaskConfig(ctx, configFile, edits); err != nil {
			return fmt.Errorf("error applying edits via SOPS: %w", err)
		}

		// Read in the modified config for diffing against the original
		modifiedConfigBytes, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("error reading modified config: %w", err)
		}
		if err := yaml.Unmarshal(modifiedConfigBytes, &modifiedConfig); err != nil {
			return fmt.Errorf("error parsing modified config: %w", err)
		}
	} else {
		// In order to not mess up the diffs we need the edits to the modified config to
		// not share any pointers with the original version of the config. We could do some
		// fiddly deep-copy logic, but the simplest way to achieve this is by re-parsing the
		// original config a second time and then modifying that.
		if err := yaml.Unmarshal(originalConfigBytes, &modifiedConfig); err != nil {
			return fmt.Errorf("error re-parsing config: %w", err)
		}

		log.WithField("name", configFile).Debug("editing plaintext config")
		modifiedConfig, err = editPlaintextTaskConfig(modifiedConfig, edits)
		if err != nil {
			return fmt.Errorf("error applying edits in memory: %w", err)
		}

		// Serialize and write out the modified config
		bs, err := yaml.Marshal(modifiedConfig)
		if err != nil {
			return fmt.Errorf("error marshalling modified config: %w", err)
		}
		if err := os.WriteFile(configFile, bs, 0664); err != nil {
			return fmt.Errorf("error writing modified config: %w", err)
		}

		// It takes about a second per config file edited with SOPS, and that's actually
		// pretty convenient for watching the edits go by and making sure they all seem
		// basically reasonable. So it messes with that if plaintext configs just zip on
		// past instantly.
		time.Sleep(500 * time.Millisecond)
	}

	log.WithField("name", configFile).Debug("edited config")
	if err := printYAMLDiff(originalConfig, modifiedConfig, configFile, true); err != nil {
		return fmt.Errorf("error diffing config: %w", err)
	}
	return nil
}

func listConfigFiles(dir string) ([]string, error) {
	var filePaths []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// List only files ending in '.config.yaml' and exclude any with names like 'source-foo.resource.1.config.yaml'
		// (which are written when Flow decides to break out a resource config into a separate file).
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".config.yaml") && !strings.Contains(info.Name(), ".resource.") {
			filePaths = append(filePaths, path)
		}
		return nil
	})
	return filePaths, err
}

func listTaskSpecDirs(dir string) ([]string, error) {
	var allFiles, err = listFilesNamed(dir, "flow.yaml")
	if err != nil {
		return nil, fmt.Errorf("error listing flow.yaml files: %w", err)
	}
	var leafDirs []string
	for _, file := range allFiles {
		if hasImports, hasOnlyImports, err := checkForImports(file); err != nil {
			return nil, fmt.Errorf("error checking for imports in %q: %w", file, err)
		} else if hasImports && !hasOnlyImports {
			return nil, fmt.Errorf("file %q imports other files and also contains other non-import data, which is not supported", file)
		} else if hasImports && hasOnlyImports {
			log.WithField("file", file).Warn("skipping import-only flow.yaml")
		} else {
			leafDirs = append(leafDirs, strings.TrimSuffix(file, "/flow.yaml"))
		}
	}
	return leafDirs, nil
}

// listFilesNamed returns a list of all files under the given directory (recursively) with the given name.
func listFilesNamed(dir, filename string) ([]string, error) {
	var files []string
	var err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == filename {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// checkForImports reads the file at the given path and checks whether it has a top-level property named 'import'.
func checkForImports(file string) (hasImports bool, hasOnlyImports bool, err error) {
	bs, err := os.ReadFile(file)
	if err != nil {
		return false, false, fmt.Errorf("error reading file: %w", err)
	}
	var doc any
	if err := yaml.Unmarshal(bs, &doc); err != nil {
		return false, false, fmt.Errorf("error unmarshalling YAML: %w", err)
	}
	if m, ok := doc.(map[string]any); ok {
		if _, hasImports := m["import"]; hasImports {
			return true, len(m) == 1, nil
		}
	}

	return false, false, nil
}

func printYAMLDiff(old, new any, description string, colorize bool) error {
	oldBytes, err := yaml.Marshal(old)
	if err != nil {
		return fmt.Errorf("error serializing old value: %w", err)
	}
	newBytes, err := yaml.Marshal(new)
	if err != nil {
		return fmt.Errorf("error serializing new value: %w", err)
	}

	configDiff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(oldBytes)),
		B:        difflib.SplitLines(string(newBytes)),
		FromFile: fmt.Sprintf("Original (%s)", description),
		ToFile:   fmt.Sprintf("Modified (%s)", description),
		Context:  1,
	})
	if err != nil {
		return fmt.Errorf("error diffing JSON objects: %w", err)
	}

	var diffLines = strings.Split(configDiff, "\n")
	if len(diffLines) == 1 && diffLines[0] == "" {
		diffLines = nil
	}
	for _, diffLine := range diffLines {
		var colorCode = ""
		if colorize {
			colorCode = "\033[0m"
			if strings.HasPrefix(diffLine, "-") {
				colorCode = "\033[1;31m"
			}
			if strings.HasPrefix(diffLine, "+") {
				colorCode = "\033[1;32m"
			}
		}
		fmt.Printf("%s%s\n", colorCode, diffLine)
	}
	if len(diffLines) == 0 {
		fmt.Printf("%s (no diff)\n", description)
	}
	return nil
}

// computeEdits applies the desired edit(s) to the provided task config. Currently there
// is only one edit type, but it should be reasonably clear how to add others.
func computeEdits(cfg any) ([]configEdit, error) {
	if *setFlag != "" {
		return addFeatureFlag(cfg, *setFlag)
	}
	return nil, fmt.Errorf("no edits specified on the command-line")
}

// addFeatureFlag constructs an edit which sets the /advanced/feature_flags property to have
// the specified flag. If there is already a value for /advanced/feature_flags the new flag is
// appended to the comma-separated list, unless the prior flags include a setting for this
// particular flag, in which case an error is returned instead so the task won't be changed.
func addFeatureFlag(cfg any, flagName string) ([]configEdit, error) {
	// Determine whether the task config already has a feature flags property, and if so
	// parse it as a list of flag settings which we can easily append to.
	var featureFlags []string
	if flagsProp, ok := indexPath(cfg, "advanced", "feature_flags"); ok {
		if flagsString, ok := flagsProp.(string); ok {
			log.WithField("flags", flagsProp).Debug("task already has feature flags")
			featureFlags = strings.Split(flagsString, ",")
		}
	}

	// If the preexisting feature flags already include a setting for this base flag name,
	// we don't want to add a redundant or contradictory one. In theory we might want to
	// be able to flip flags here, but it seems safer to just refuse to do anything when
	// this happens.
	var flagBase = strings.TrimPrefix(flagName, "no_")
	if slices.Contains(featureFlags, flagBase) || slices.Contains(featureFlags, "no_"+flagBase) {
		return nil, fmt.Errorf("task config already has a setting for feature flag %q, doing nothing", flagBase)
	}

	// Finally, add the specified flag to the list and emit the resulting config edit.
	featureFlags = append(featureFlags, flagName)
	return []configEdit{{
		Path:  []string{"advanced", "feature_flags"},
		Value: strings.Join(featureFlags, ","),
	}}, nil
}
