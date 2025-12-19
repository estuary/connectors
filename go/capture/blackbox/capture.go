// Package blackbox implements functions for running capture connectors in a black-box test
package blackbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"gopkg.in/yaml.v3"
)

// Capture represents a capture's configuration, bindings, and persistent checkpoint state.
type Capture struct {
	runRoot         string          // Directory in which flowctl commands are executed
	Catalog         json.RawMessage // In-memory catalog JSON, written to disk before commands
	Checkpoint      json.RawMessage // Persistent checkpoint state between captures
	DiscoveryFilter *regexp.Regexp  // Filter for discovered bindings (nil = no filtering)
	Logger          func(...any)    // Log function (defaults to stderr, set to t.Log in tests)
}

func New(baseYAML string) (*Capture, error) {
	// Check that flowctl is available on the PATH
	if _, err := exec.LookPath("flowctl"); err != nil {
		return nil, fmt.Errorf("flowctl not found on PATH: %w", err)
	}

	// Find the run root directory, either from environment variable or via git
	var runRoot = os.Getenv("BLACKBOX_RUN_ROOT")
	if runRoot == "" {
		cmd := exec.Command("git", "rev-parse", "--show-toplevel")
		output, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("finding repository root (set BLACKBOX_RUN_ROOT to override): %w", err)
		}
		runRoot = string(output[:len(output)-1]) // trim trailing newline
	}

	// Read the base YAML file. Since YAML is a superset of JSON it should be possible to
	// use a `flow.json` here as well, but we translate the file to JSON so that it's a
	// tiny bit simpler to do programmatic manipulations of the catalog later on.
	yamlCatalog, err := os.ReadFile(baseYAML)
	if err != nil {
		return nil, fmt.Errorf("reading base YAML %q: %w", baseYAML, err)
	}
	var parsedCatalog any
	if err := yaml.Unmarshal(yamlCatalog, &parsedCatalog); err != nil {
		return nil, fmt.Errorf("parsing YAML %q: %w", baseYAML, err)
	}
	catalog, err := json.Marshal(parsedCatalog)
	if err != nil {
		return nil, fmt.Errorf("encoding as JSON: %w", err)
	}

	return &Capture{
		runRoot:    runRoot,
		Catalog:    catalog,
		Checkpoint: json.RawMessage(`{}`),
		Logger:     defaultLogger,
	}, nil
}

// flowctlCmd represents a running flowctl command with log streaming.
type flowctlCmd struct {
	cmd        *exec.Cmd
	Stdout     io.ReadCloser
	stderrDone <-chan struct{}
	lastError  string // Last error-level log message, populated by stderr goroutine
}

// Wait waits for stderr streaming to complete and then waits for the command to exit.
// The caller must fully consume Stdout before calling Wait to avoid deadlock.
func (fc *flowctlCmd) Wait() error {
	<-fc.stderrDone
	return fc.cmd.Wait()
}

// startFlowctl starts a flowctl command with stderr streaming to the logger.
// Returns a flowctlCmd whose Stdout must be consumed before calling Wait().
// When the context is cancelled, the command process will be killed.
func (c *Capture) startFlowctl(ctx context.Context, args ...string) (*flowctlCmd, error) {
	cmd := exec.CommandContext(ctx, "flowctl", append([]string{"--profile=testing"}, args...)...)
	cmd.Dir = c.runRoot
	cmd.Env = append(os.Environ(), "NO_COLOR=1", "LOG_FORMAT=json")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("creating stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("creating stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting flowctl: %w", err)
	}

	var done = make(chan struct{})
	var fc = &flowctlCmd{
		cmd:        cmd,
		Stdout:     stdout,
		stderrDone: done,
	}

	go func() {
		defer close(done)
		var scanner = bufio.NewScanner(stderr)
		for scanner.Scan() {
			var text = scanner.Text()
			if !strings.HasPrefix(text, "{") {
				continue // Skip lines which aren't JSON logs
			}

			// Parse JSON log line to extract level, message, and fields
			var logEntry struct {
				Level   string          `json:"level"`
				Message string          `json:"message"`
				Fields  json.RawMessage `json:"fields"`
			}
			if err := json.Unmarshal([]byte(text), &logEntry); err != nil {
				c.log(text) // Fall back to raw output if parsing fails
				continue
			}

			// Track the last error-level message
			if logEntry.Level == "error" {
				fc.lastError = logEntry.Message
			}

			// Format: "LEVEL  message  {fields}" or "LEVEL  message" if no fields
			var formatted string
			if len(logEntry.Fields) > 0 && string(logEntry.Fields) != "null" {
				formatted = fmt.Sprintf("%-5s %s  %s", strings.ToUpper(logEntry.Level), logEntry.Message, string(logEntry.Fields))
			} else {
				formatted = fmt.Sprintf("%-5s %s", strings.ToUpper(logEntry.Level), logEntry.Message)
			}
			c.log(formatted)
		}
	}()

	return fc, nil
}

// writeCatalog writes the in-memory catalog to a temporary directory and returns
// the path to the catalog file and the directory. The caller is responsible for
// removing the directory when done via os.RemoveAll(catalogDir).
func (c *Capture) writeCatalog() (catalogFile, catalogDir string, err error) {
	catalogDir, err = os.MkdirTemp("", "blackbox-capture-*")
	if err != nil {
		return "", "", fmt.Errorf("creating temp directory: %w", err)
	}
	catalogFile = catalogDir + "/flow.json"
	if err := os.WriteFile(catalogFile, c.Catalog, 0644); err != nil {
		os.RemoveAll(catalogDir)
		return "", "", fmt.Errorf("writing catalog: %w", err)
	}
	return catalogFile, catalogDir, nil
}

func (c *Capture) Spec() (json.RawMessage, error) {
	path, tempdir, err := c.writeCatalog()
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempdir)

	fc, err := c.startFlowctl(context.Background(), "raw", "spec", "--source", path)
	if err != nil {
		return nil, err
	}
	output, err := io.ReadAll(fc.Stdout)
	if err != nil {
		return nil, fmt.Errorf("reading stdout: %w", err)
	}
	if err := fc.Wait(); err != nil {
		return nil, fmt.Errorf("flowctl raw spec failed: %w", err)
	}
	return json.RawMessage(output), nil
}

// DiscoverRaw performs discovery with the `--emit-raw` flag for snapshotting the results.
// It does not modify the catalog bindings, use `Discover()` for that.
func (c *Capture) DiscoverRaw() ([]json.RawMessage, error) {
	path, tempdir, err := c.writeCatalog()
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempdir)

	fc, err := c.startFlowctl(context.Background(), "raw", "discover", "--source", path, "-o", "json", "--emit-raw")
	if err != nil {
		return nil, err
	}
	var bindings []json.RawMessage
	var scanner = bufio.NewScanner(fc.Stdout)
	for scanner.Scan() {
		var line = scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if c.DiscoveryFilter != nil {
			var recommendedName = gjson.GetBytes(line, "recommendedName").String()
			if !c.DiscoveryFilter.MatchString(recommendedName) {
				continue
			}
		}
		// Note: scanner.Bytes() returns a slice referencing the scanner's internal
		// buffer, which is overwritten on each Scan() call. We must copy the bytes.
		bindings = append(bindings, append(json.RawMessage(nil), line...))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning discover output: %w", err)
	}

	if err := fc.Wait(); err != nil {
		return nil, fmt.Errorf("flowctl raw discover failed: %w", err)
	}

	return bindings, nil
}

// Discover performs discovery and updates c.Catalog with the resulting bindings.
func (c *Capture) Discover() (json.RawMessage, error) {
	path, tempdir, err := c.writeCatalog()
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempdir)

	fc, err := c.startFlowctl(context.Background(), "raw", "discover", "--source", path)
	if err != nil {
		return nil, err
	}
	io.Copy(io.Discard, fc.Stdout) // Drain stdout to avoid deadlock
	if err := fc.Wait(); err != nil {
		return nil, fmt.Errorf("flowctl raw discover failed: %w", err)
	}

	// Bundle the discovered catalog into a single flattened JSON
	fc, err = c.startFlowctl(context.Background(), "raw", "bundle", "--source", path, "-o", "json")
	if err != nil {
		return nil, err
	}
	c.Catalog, err = io.ReadAll(fc.Stdout)
	if err != nil {
		return nil, fmt.Errorf("reading bundle output: %w", err)
	}
	if err := fc.Wait(); err != nil {
		return nil, fmt.Errorf("flowctl raw bundle failed: %w", err)
	}

	var catalog map[string]any
	if err := json.Unmarshal(c.Catalog, &catalog); err != nil {
		return nil, fmt.Errorf("parsing catalog: %w", err)
	}

	// Filter bindings and collections if a filter regexp is provided
	if captures, ok := catalog["captures"].(map[string]any); ok {
		for _, capture := range captures {
			if captureObj, ok := capture.(map[string]any); ok {
				if bindings, ok := captureObj["bindings"].([]any); ok {
					var filtered []map[string]any
					for _, binding := range bindings {
						if bindingObj, ok := binding.(map[string]any); ok {
							if target, ok := bindingObj["target"].(string); ok {
								if c.DiscoveryFilter == nil || c.DiscoveryFilter.MatchString(target) {
									filtered = append(filtered, bindingObj)
								}
							}
						}
					}
					// Sort filtered bindings by target name. Sometimes `flowctl raw discover`
					// will yield bindings in different orders, and that seems to influence the
					// ordering of `flowctl preview` documents within a single transaction, so
					// sorting by name here improves test stability.
					sort.Slice(filtered, func(i, j int) bool {
						return filtered[i]["target"].(string) < filtered[j]["target"].(string)
					})
					captureObj["bindings"] = filtered
				}
			}
		}
	}

	// Filter .collections for collection names that match the regexp, and remove
	// $id from schemas for test stability (it contains the temp directory path)
	if collections, ok := catalog["collections"].(map[string]any); ok {
		for name, collection := range collections {
			if c.DiscoveryFilter != nil && !c.DiscoveryFilter.MatchString(name) {
				delete(collections, name)
				continue
			}
			if collectionObj, ok := collection.(map[string]any); ok {
				if schema, ok := collectionObj["schema"].(map[string]any); ok {
					delete(schema, "$id")
				}
			}
		}
	}

	data, err := json.Marshal(catalog)
	if err != nil {
		return nil, fmt.Errorf("serializing catalog: %w", err)
	}
	c.Catalog = data

	return c.Catalog, nil
}

func (c *Capture) Run(sessions int) ([]byte, error) {
	return c.RunWithContext(context.Background(), sessions)
}

// RunWithContext runs a capture with the given context. When the context is cancelled,
// the capture process will be killed. This is useful for tests that need to run a
// capture indefinitely and then stop it programmatically.
func (c *Capture) RunWithContext(ctx context.Context, sessions int) ([]byte, error) {
	path, tempdir, err := c.writeCatalog()
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempdir)

	fc, err := c.startFlowctl(ctx, "preview",
		"--log-json",
		"--source", path,
		fmt.Sprintf("--sessions=%d", sessions),
		"--timeout=30s",
		"--output-state",
		"--initial-state", string(c.Checkpoint),
	)
	if err != nil {
		return nil, err
	}

	// Process output line-by-line, separating documents from state updates
	var documents []byte
	var statePrefix = []byte(`["connectorState",`)
	var scanner = bufio.NewScanner(fc.Stdout)
	scanner.Buffer(nil, 256*1024*1024) // Handle output documents up to 256MiB
	for scanner.Scan() {
		var line = scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if bytes.HasPrefix(line, statePrefix) {
			// Parse state update and update checkpoint
			var stateUpdate []json.RawMessage
			if err := json.Unmarshal(line, &stateUpdate); err != nil {
				return nil, fmt.Errorf("parsing state update: %w", err)
			}
			if len(stateUpdate) < 2 {
				continue
			}
			var state struct {
				Updated    json.RawMessage `json:"updated"`
				MergePatch bool            `json:"mergePatch"`
			}
			if err := json.Unmarshal(stateUpdate[1], &state); err != nil {
				return nil, fmt.Errorf("parsing state object: %w", err)
			}
			if len(state.Updated) > 0 {
				if state.MergePatch {
					c.Checkpoint, err = jsonpatch.MergePatch(c.Checkpoint, state.Updated)
					if err != nil {
						return nil, fmt.Errorf("applying state merge patch: %w", err)
					}
				} else {
					c.Checkpoint = state.Updated
				}
			}
		} else {
			// Accumulate document lines
			documents = append(documents, line...)
			documents = append(documents, '\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning output: %w", err)
	}
	if err := fc.Wait(); err != nil {
		if fc.lastError != "" {
			return nil, fmt.Errorf("flowctl preview failed:\n%s", fc.lastError)
		}
		return nil, fmt.Errorf("flowctl preview failed: %w", err)
	}

	return documents, nil
}

// EditConfig modifies a property of the endpoint config(s) of all captures in the catalog.
func (c *Capture) EditConfig(path string, val any) error {
	for _, captureName := range gjson.GetBytes(c.Catalog, "captures.@keys").Array() {
		// Figure out if it's a local command or Docker container (.connector) endpoint
		var fullPath = `captures.` + captureName.String() + `.endpoint`
		if gjson.GetBytes(c.Catalog, fullPath+`.local`).Exists() {
			fullPath += `.local.config.` + path
		} else {
			fullPath += `.connector.config.` + path
		}

		// Actually perform the edit
		var result, err = sjson.SetBytes(c.Catalog, fullPath, val)
		if err != nil {
			return err
		}
		c.Catalog = result
	}
	return nil
}

// EditCollection modifies a property of collections whose names contain the filter substring.
// The path is relative to the collection, e.g. "key" to set the collection key.
func (c *Capture) EditCollection(filter string, path string, val any) error {
	for _, name := range gjson.GetBytes(c.Catalog, "collections.@keys").Array() {
		if !strings.Contains(name.String(), filter) {
			continue
		}
		var fullPath = "collections." + name.String() + "." + path
		var result, err = sjson.SetBytes(c.Catalog, fullPath, val)
		if err != nil {
			return err
		}
		c.Catalog = result
	}
	return nil
}

// EditBinding modifies a property of the binding's resource config at the given index.
// The path is relative to the binding's resource, e.g. "mode" to set the backfill mode.
func (c *Capture) EditBinding(index int, path string, val any) error {
	var captureName = gjson.GetBytes(c.Catalog, "captures.@keys.0").String()
	var fullPath = fmt.Sprintf("captures.%s.bindings.%d.%s", captureName, index, path)
	var result, err = sjson.SetBytes(c.Catalog, fullPath, val)
	if err != nil {
		return err
	}
	c.Catalog = result
	return nil
}

// EditCheckpoint modifies a property of the checkpoint state.
// The path is specified in sjson dot notation, e.g. "bindingStateV1.test%2Ffoobar.scanned"
func (c *Capture) EditCheckpoint(path string, val any) error {
	result, err := sjson.SetBytes(c.Checkpoint, path, val)
	if err != nil {
		return err
	}
	c.Checkpoint = result
	return nil
}

// JSONSanitizer defines a pattern to match and replace in JSON output before pretty-printing.
type JSONSanitizer struct {
	Matcher     *regexp.Regexp
	Replacement string
}

// TranscriptCapture wraps a Capture and writes readable results to a transcript
// for convenient snapshot testing across multiple operations.
type TranscriptCapture struct {
	Capture              *Capture
	Transcript           *strings.Builder
	DocumentSanitizers   []JSONSanitizer // Applied to captured documents before pretty-printing
	CheckpointSanitizers []JSONSanitizer // Applied to checkpoint state output
}

// NewWithTranscript creates a new TranscriptCapture from a base YAML catalog file.
func NewWithTranscript(baseYAML string) (*TranscriptCapture, error) {
	capture, err := New(baseYAML)
	if err != nil {
		return nil, err
	}
	return &TranscriptCapture{
		Capture:    capture,
		Transcript: new(strings.Builder),
	}, nil
}

// applySanitizers applies a list of sanitizers to data and returns the result.
func applySanitizers(data []byte, sanitizers []JSONSanitizer) []byte {
	for _, s := range sanitizers {
		data = s.Matcher.ReplaceAll(data, []byte(s.Replacement))
	}
	return data
}

// prettyPrintJSON writes JSON data to the transcript, pretty-printed if possible.
func (tc *TranscriptCapture) prettyPrintJSON(data json.RawMessage) {
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		tc.Transcript.Write(data)
	} else {
		tc.Transcript.Write(pretty.Bytes())
	}
	tc.Transcript.WriteString("\n")
}

func (tc *TranscriptCapture) Spec(description string) json.RawMessage {
	fmt.Fprintf(tc.Transcript, "=== Spec: %s ===\n", description)
	result, err := tc.Capture.Spec()
	if err != nil {
		fmt.Fprintf(tc.Transcript, "error: %v\n\n", err)
		return nil
	}
	tc.prettyPrintJSON(result)
	return result
}

// Discover runs discovery mainly for its side-effects and writes only the names of
// discovered collections to the transcript.
func (tc *TranscriptCapture) Discover(description string) json.RawMessage {
	fmt.Fprintf(tc.Transcript, "=== Discover: %s ===\n", description)
	result, err := tc.Capture.Discover()
	if err != nil {
		fmt.Fprintf(tc.Transcript, "error: %v\n\n", err)
		return nil
	}
	var catalog struct {
		Collections map[string]any `json:"collections"`
	}
	if err := json.Unmarshal(result, &catalog); err != nil {
		fmt.Fprintf(tc.Transcript, "error parsing catalog: %v\n\n", err)
		return result
	}
	var names []string
	for name := range catalog.Collections {
		names = append(names, name)
	}
	sort.Strings(names) // Sort for test stability
	for _, name := range names {
		fmt.Fprintf(tc.Transcript, "%s\n", name)
	}
	return result
}

// DiscoverFull runs discovery and writes the raw discovered bindings to the transcript.
// It also performs discovery for its side-effects on the catalog.
func (tc *TranscriptCapture) DiscoverFull(description string) []json.RawMessage {
	fmt.Fprintf(tc.Transcript, "=== Discover: %s ===\n", description)
	bindings, err := tc.Capture.DiscoverRaw()
	if err != nil {
		fmt.Fprintf(tc.Transcript, "error: %v\n\n", err)
		return nil
	}
	if _, err := tc.Capture.Discover(); err != nil {
		fmt.Fprintf(tc.Transcript, "error: %v\n\n", err)
		return nil
	}
	// Sort bindings by recommendedName for stable output
	sort.Slice(bindings, func(i, j int) bool {
		return gjson.GetBytes(bindings[i], "recommendedName").String() <
			gjson.GetBytes(bindings[j], "recommendedName").String()
	})
	for _, binding := range bindings {
		tc.prettyPrintJSON(binding)
	}
	if len(bindings) == 0 {
		fmt.Fprintf(tc.Transcript, "(no bindings)\n")
	}
	return bindings
}

func (tc *TranscriptCapture) Run(description string, sessions int) []byte {
	fmt.Fprintf(tc.Transcript, "=== Run: %s ===\n", description)
	result, err := tc.Capture.Run(sessions)
	if err != nil {
		fmt.Fprintf(tc.Transcript, "error: %v\n\n", err)
		return nil
	}

	// Collect all document lines
	var lines [][]byte
	for line := range bytes.SplitSeq(result, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		lines = append(lines, line)
	}

	// Sort lines by table name (the prefix before the first comma).
	// Use stable sort to preserve document ordering within each table.
	sort.SliceStable(lines, func(i, j int) bool {
		iPre, _, _ := bytes.Cut(lines[i], []byte(","))
		jPre, _, _ := bytes.Cut(lines[j], []byte(","))
		return bytes.Compare(iPre, jPre) < 0
	})

	// Pretty-print each document line, applying document sanitizers first
	for _, line := range lines {
		tc.prettyPrintJSON(applySanitizers(line, tc.DocumentSanitizers))
	}

	// Print final checkpoint, applying checkpoint sanitizers first
	fmt.Fprintf(tc.Transcript, "--- Final Checkpoint ---\n")
	tc.prettyPrintJSON(applySanitizers(tc.Capture.Checkpoint, tc.CheckpointSanitizers))

	return result
}
