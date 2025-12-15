// The bulk-image-updater script updates the connector image tag in all
// flow.yaml files under a local directory.
//
// This is useful for bulk-updating connector versions across many task
// specs pulled using the list-tasks script.
//
// The process for doing that is:
//   1. scripts/list-tasks.sh --connector=source-foo --pull --dir=./specs
//   2. scripts/bulk-image-updater.sh --tag=v3 --dir=./specs
//   3. scripts/bulk-publish.sh --mark --dir=./specs
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pmezard/go-difflib/difflib"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var scriptDescription = `
The bulk-image-updater script updates the connector image tag in all
flow.yaml files under a local directory.

This is useful for bulk-updating connector versions across many task
specs pulled using the list-tasks script.

Usage:
  1. scripts/list-tasks.sh --connector=source-foo --pull --dir=./specs
  2. scripts/bulk-image-updater.sh --tag=v3 --dir=./specs
  3. scripts/bulk-publish.sh --mark --dir=./specs
`

var (
	logLevel = flag.String("log_level", "info", "The log level to print at.")
	specsDir = flag.String("dir", "./specs", "The directory beneath which to update images.")
	newTag   = flag.String("tag", "", "The new image tag to set (e.g., 'v2', 'v3.1.0'). Required.")
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

	if *newTag == "" {
		log.Fatal("--tag is required")
	}

	if err := performEdits(); err != nil {
		log.WithField("err", err).Fatal("error")
	}
}

func performEdits() error {
	// List all leaf flow.yaml files under the directory
	flowFiles, err := listLeafFlowFiles(*specsDir)
	if err != nil {
		return fmt.Errorf("error listing flow.yaml files: %w", err)
	}
	if len(flowFiles) == 0 {
		return fmt.Errorf("no leaf flow.yaml files found under %q", *specsDir)
	}

	log.WithField("count", len(flowFiles)).Info("updating image tags in flow.yaml files")
	var errs []error
	var updatedCount int
	for _, file := range flowFiles {
		updated, err := editFlowYaml(file, *newTag)
		if err != nil {
			errs = append(errs, fmt.Errorf("error editing %q: %w", file, err))
		} else if updated {
			updatedCount++
		}
	}

	if len(errs) > 0 {
		fmt.Printf("failed to edit %d files\n", len(errs))
		for _, err := range errs {
			fmt.Printf("%s\n", err.Error())
		}
	}
	fmt.Printf("updated %d/%d flow.yaml files\n", updatedCount, len(flowFiles))
	return nil
}

// listLeafFlowFiles returns all flow.yaml files that are not import-only files.
func listLeafFlowFiles(dir string) ([]string, error) {
	allFiles, err := listFilesNamed(dir, "flow.yaml")
	if err != nil {
		return nil, fmt.Errorf("error listing flow.yaml files: %w", err)
	}
	var leafFiles []string
	for _, file := range allFiles {
		hasImports, hasOnlyImports, err := checkForImports(file)
		if err != nil {
			return nil, fmt.Errorf("error checking for imports in %q: %w", file, err)
		}
		if hasImports && !hasOnlyImports {
			return nil, fmt.Errorf("file %q imports other files and also contains other non-import data, which is not supported", file)
		}
		if hasImports && hasOnlyImports {
			log.WithField("file", file).Debug("skipping import-only flow.yaml")
		} else {
			leafFiles = append(leafFiles, file)
		}
	}
	return leafFiles, nil
}

// listFilesNamed returns a list of all files under the given directory (recursively) with the given name.
func listFilesNamed(dir, filename string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
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

// editFlowYaml updates all connector image tags in a flow.yaml file.
// Returns true if the file was modified.
func editFlowYaml(filePath string, tag string) (bool, error) {
	bs, err := os.ReadFile(filePath)
	if err != nil {
		return false, fmt.Errorf("error reading file: %w", err)
	}

	var originalDoc any
	if err := yaml.Unmarshal(bs, &originalDoc); err != nil {
		return false, fmt.Errorf("error parsing YAML: %w", err)
	}

	// Re-parse to get a separate copy for modification
	var modifiedDoc any
	if err := yaml.Unmarshal(bs, &modifiedDoc); err != nil {
		return false, fmt.Errorf("error re-parsing YAML: %w", err)
	}

	// Try both captures and materializations
	var modified bool
	for _, taskType := range []string{"captures", "materializations"} {
		tasks, ok := indexPath(modifiedDoc, taskType)
		if !ok {
			continue
		}
		tasksMap, ok := tasks.(map[string]any)
		if !ok {
			continue
		}
		for taskName, taskSpec := range tasksMap {
			imageVal, ok := indexPath(taskSpec, "endpoint", "connector", "image")
			if !ok {
				log.WithFields(log.Fields{"task": taskName, "file": filePath}).Debug("no image property found")
				continue
			}
			imageStr, ok := imageVal.(string)
			if !ok {
				log.WithFields(log.Fields{"task": taskName, "file": filePath}).Warn("image property is not a string")
				continue
			}

			newImage, err := updateImageTag(imageStr, tag)
			if err != nil {
				return false, fmt.Errorf("error updating image for task %q: %w", taskName, err)
			}
			if newImage == "" {
				// Already at target tag
				log.WithFields(log.Fields{"task": taskName, "image": imageStr}).Debug("already at target tag")
				continue
			}

			// Set the new image
			if _, err := setAtPath(taskSpec, []string{"endpoint", "connector", "image"}, newImage); err != nil {
				return false, fmt.Errorf("error setting image for task %q: %w", taskName, err)
			}
			modified = true
			log.WithFields(log.Fields{"task": taskName, "old": imageStr, "new": newImage}).Debug("updated image")
		}
	}

	if !modified {
		fmt.Printf("%s (no change)\n", filePath)
		return false, nil
	}

	// Write back
	newBs, err := yaml.Marshal(modifiedDoc)
	if err != nil {
		return false, fmt.Errorf("error marshalling YAML: %w", err)
	}
	if err := os.WriteFile(filePath, newBs, 0644); err != nil {
		return false, fmt.Errorf("error writing file: %w", err)
	}

	// Print diff
	if err := printYAMLDiff(originalDoc, modifiedDoc, filePath, true); err != nil {
		return false, fmt.Errorf("error printing diff: %w", err)
	}

	// Small delay so user can see changes go by
	time.Sleep(300 * time.Millisecond)
	return true, nil
}

// updateImageTag replaces the tag portion of a container image reference.
// Returns empty string if no change is needed (already at target tag).
func updateImageTag(currentImage, newTag string) (string, error) {
	if currentImage == "" {
		return "", fmt.Errorf("empty image value")
	}

	lastColon := strings.LastIndex(currentImage, ":")
	if lastColon == -1 {
		// No tag - append
		return currentImage + ":" + newTag, nil
	}

	imageWithoutTag := currentImage[:lastColon]
	currentTag := currentImage[lastColon+1:]

	// Check for digest (sha256:...)
	if strings.HasPrefix(currentTag, "sha256") {
		return "", fmt.Errorf("image uses digest instead of tag: %s", currentImage)
	}

	if currentTag == newTag {
		return "", nil // No change needed
	}

	return imageWithoutTag + ":" + newTag, nil
}

// indexPath navigates through nested maps to find a value at the given path.
func indexPath(x any, path ...string) (any, bool) {
	for _, p := range path {
		m, ok := x.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[p]
		if !ok {
			return nil, false
		}
		x = v
	}
	return x, true
}

// setAtPath sets a value at the given path in a nested map structure.
func setAtPath(x any, p []string, v any) (any, error) {
	if len(p) == 0 {
		return v, nil
	}

	if x == nil {
		x = make(map[string]any)
	}
	obj, ok := x.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unable to modify %v at pointer %s", x, strings.Join(p, "."))
	}
	modified, err := setAtPath(obj[p[0]], p[1:], v)
	if err != nil {
		return nil, err
	}
	obj[p[0]] = modified
	return obj, nil
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
		return fmt.Errorf("error diffing YAML: %w", err)
	}

	diffLines := strings.Split(configDiff, "\n")
	if len(diffLines) == 1 && diffLines[0] == "" {
		diffLines = nil
	}
	for _, diffLine := range diffLines {
		colorCode := ""
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
