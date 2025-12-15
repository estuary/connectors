package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The bulk-publish script is used to publish all leaf 'flow.yaml' files underneath
a local directory to production, independently of each other so that a failure to
publish one spec doesn't block others.

This behavior works best when applied to the directory structure created by the
list-tasks script when using the '--pull' flag, but the script has logic to avoid
publishing any 'flow.yaml' files which import others so _in theory_ it should work
correctly on the specs produced by 'flowctl draft develop' as well.

Percentile-based publishing:
  Use --from-pct and --to-pct to publish a subset of specs. Files are ordered by
  a hash of the catalog name (not alphabetically) to distribute publications across
  tenants. For example:
    --from-pct=0 --to-pct=10    # Publish the first 10%
    --from-pct=10 --to-pct=50   # Publish from 10% to 50%
  The --mark flag ensures already-published specs are skipped on subsequent runs.

Refer to 'docs/feature_flags.md' for more information on the intended use-case and workflow.
`

var (
	logLevel      = flag.String("log_level", "info", "The log level to print at.")
	configsDir    = flag.String("dir", "./specs", "The directory beneath which to publish configs.")
	markPublished = flag.Bool("mark", false, "When true, renames files after publishing so that another run of this script will ignore them.")
	yesFlag       = flag.Bool("yes", false, "When true, automatically answer 'yes' to the continue prompt.")
	fromPct       = flag.Int("from-pct", 0, "Start publishing from this percentile (0-100). Files are ordered by hash of catalog name.")
	toPct         = flag.Int("to-pct", 100, "Stop publishing at this percentile (0-100). Files are ordered by hash of catalog name.")
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

	// Validate percentile flags
	if *fromPct < 0 || *fromPct > 100 {
		log.WithField("from-pct", *fromPct).Fatal("from-pct must be between 0 and 100")
	}
	if *toPct < 0 || *toPct > 100 {
		log.WithField("to-pct", *toPct).Fatal("to-pct must be between 0 and 100")
	}
	if *fromPct >= *toPct {
		log.WithFields(log.Fields{"from-pct": *fromPct, "to-pct": *toPct}).Fatal("from-pct must be less than to-pct")
	}

	if err := performPublishing(context.Background()); err != nil {
		log.WithError(err).Fatal("error")
	}
}

func performPublishing(ctx context.Context) error {
	// List all flow.yaml files underneath the configs directory, recursively.
	var files, err = listFilesNamed(*configsDir, "flow.yaml")
	if err != nil {
		return fmt.Errorf("error listing flow.yaml files: %w", err)
	}

	// Parse each YAML file and check if it imports any other files. Strictly
	// speaking this check isn't required if we assume that the local specs came
	// from running 'list-tasks.sh --pull', but it's a good safety check in case
	// the specs came from 'flowctl draft develop'.
	var leafFiles []string
	for _, file := range files {
		if hasImports, hasOnlyImports, err := checkForImports(file); err != nil {
			return fmt.Errorf("error checking flow.yaml file for imports: %w", err)
		} else if hasImports && !hasOnlyImports {
			return fmt.Errorf("file %q imports other files and also contains other non-import data, which is not supported", file)
		} else if hasImports && hasOnlyImports {
			log.WithField("file", file).Warn("skipping import-only flow.yaml")
		} else {
			leafFiles = append(leafFiles, file)
		}
	}

	if len(leafFiles) == 0 {
		return fmt.Errorf("no leaf flow.yaml files found")
	}

	// Sort files by hash of catalog name to distribute publications across tenants.
	// This ensures a deterministic but non-alphabetical ordering.
	sort.Slice(leafFiles, func(i, j int) bool {
		return catalogNameHash(leafFiles[i]) < catalogNameHash(leafFiles[j])
	})

	// Select files in the specified percentile range.
	totalFiles := len(leafFiles)
	startIdx := (totalFiles * *fromPct) / 100
	endIdx := (totalFiles * *toPct) / 100
	if endIdx > totalFiles {
		endIdx = totalFiles
	}
	selectedFiles := leafFiles[startIdx:endIdx]

	if len(selectedFiles) == 0 {
		log.WithFields(log.Fields{
			"total":    totalFiles,
			"from-pct": *fromPct,
			"to-pct":   *toPct,
		}).Warn("no files in the specified percentile range")
		return nil
	}

	log.WithFields(log.Fields{
		"total":    totalFiles,
		"selected": len(selectedFiles),
		"from-pct": *fromPct,
		"to-pct":   *toPct,
		"range":    fmt.Sprintf("[%d:%d]", startIdx, endIdx),
	}).Info("selected files for publishing")

	if !*yesFlag {
		fmt.Printf("Publish %d changes (indices %d-%d of %d total)? (y/N) ", len(selectedFiles), startIdx, endIdx-1, totalFiles)
		var response string
		fmt.Scanln(&response)
		if !strings.EqualFold(response, "y") {
			fmt.Println("Aborting.")
			return nil
		}
	}

	// For each flow.yaml file, try and publish it to production.
	var failedFiles []string
	for _, file := range selectedFiles {
		if err := publishFile(ctx, file); err != nil {
			log.WithField("file", file).WithError(err).Warn("error publishing file")
			failedFiles = append(failedFiles, file)
		} else if *markPublished {
			var newFilename = strings.ReplaceAll(file, "flow.yaml", "flow.published.yaml")
			if err := os.Rename(file, newFilename); err != nil {
				return fmt.Errorf("error renaming file %q to %q: %w", file, newFilename, err)
			}
		}
	}
	for _, file := range failedFiles {
		log.WithField("file", file).Error("failed to publish")
	}
	return nil
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

// publishFile publishes the file at the given path to production.
func publishFile(ctx context.Context, file string) error {
	log.WithField("file", file).Info("publishing file")

	if err := flowctl(ctx, "draft", "create"); err != nil {
		return fmt.Errorf("error creating draft: %w", err)
	} else if err := flowctl(ctx, "draft", "author", "--source", file); err != nil {
		return fmt.Errorf("error authoring draft: %w", err)
	} else if err := flowctl(ctx, "draft", "publish"); err != nil {
		return fmt.Errorf("error publishing draft: %w", err)
	}
	return nil
}

func flowctl(ctx context.Context, args ...string) error {
	log.WithField("command", args).Debug("executing flowctl command")
	var _, err = exec.CommandContext(ctx, "flowctl", args...).Output()
	return err
}

// catalogNameHash extracts the catalog name from a file path and returns its hash.
// For a path like "./specs/tenantTaskName/flow.yaml", the catalog name is "tenantTaskName".
func catalogNameHash(filePath string) uint64 {
	// Get the parent directory name (catalog name)
	catalogName := filepath.Base(filepath.Dir(filePath))

	// Hash the catalog name using FNV-1a
	h := fnv.New64a()
	h.Write([]byte(catalogName))
	return h.Sum64()
}
