// The restart-tasks script restarts a list of tasks by publishing each with
// 'shards: {disable: true}', waiting until the control plane reports it as
// TASK_DISABLED, then publishing it re-enabled and waiting until it reports OK.
//
// Every task is driven independently to completion. A failure anywhere in the
// cycle (pull, publish, status poll, timeout, or the task landing in ERROR after
// re-enable) restarts the cycle for that task, until it succeeds or --max-attempts
// is exhausted. Successfully restarted tasks are recorded in <dir>/restarted.txt
// so that re-running the script skips them, and the exit code is nonzero unless
// every task was restarted.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The restart-tasks script restarts tasks by disabling and then re-enabling
their shards, verifying each step against 'flowctl catalog status' and
retrying any task whose restart does not complete.

Task names are taken from --tasks and/or one-per-line from --file.
`

var (
	logLevel    = flag.String("log_level", "info", "The log level to print at.")
	tasksFlag   = flag.String("tasks", "", "A comma-separated list of task names to restart.")
	tasksFile   = flag.String("file", "", "A file with one task name per line to restart. Blank lines and '#' comments are ignored.")
	workDir     = flag.String("dir", "./restart-specs", "The directory under which task specs are pulled and progress is recorded.")
	parallelism = flag.Int("parallel", 1, "How many tasks to restart concurrently.")
	phaseWait   = flag.Duration("wait", 10*time.Minute, "How long to wait for a task to reach the expected status after each publish before treating the attempt as failed.")
	pollEvery   = flag.Duration("poll", 10*time.Second, "How often to poll 'flowctl catalog status' while waiting.")
	maxAttempts = flag.Int("max-attempts", 0, "Give up on a task after this many failed attempts. 0 retries forever.")
	retryDelay  = flag.Duration("retry-delay", 30*time.Second, "Initial delay between attempts for a task, doubled on each failure up to 10 minutes.")
	yesFlag     = flag.Bool("yes", false, "When true, automatically answer 'yes' to the continue prompt.")
)

const progressFile = "restarted.txt"

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

	if err := run(context.Background()); err != nil {
		log.WithError(err).Fatal("error")
	}
}

func run(ctx context.Context) error {
	tasks, err := loadTaskList()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*workDir, 0755); err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}
	progress, err := newProgress(filepath.Join(*workDir, progressFile))
	if err != nil {
		return err
	}
	defer progress.Close()

	var pending []string
	for _, task := range tasks {
		if progress.Done(task) {
			log.WithField("task", task).Info("already restarted in a previous run, skipping")
		} else {
			pending = append(pending, task)
		}
	}
	if len(pending) == 0 {
		log.Info("nothing to do")
		return nil
	}

	log.WithFields(log.Fields{"total": len(tasks), "pending": len(pending)}).Info("tasks to restart")
	if !*yesFlag {
		fmt.Printf("Restart %d tasks? (y/N) ", len(pending))
		var response string
		fmt.Scanln(&response)
		if !strings.EqualFold(response, "y") {
			fmt.Println("Aborting.")
			return nil
		}
	}

	var (
		wg      sync.WaitGroup
		sem     = make(chan struct{}, max(*parallelism, 1))
		mu      sync.Mutex
		failed  []string
		results = map[string]error{}
	)
	for _, task := range pending {
		wg.Add(1)
		sem <- struct{}{}
		go func(task string) {
			defer wg.Done()
			defer func() { <-sem }()
			err := restartUntilSuccessful(ctx, task)
			mu.Lock()
			defer mu.Unlock()
			results[task] = err
			if err != nil {
				failed = append(failed, task)
			} else if err := progress.Record(task); err != nil {
				log.WithField("task", task).WithError(err).Error("failed to record progress")
			}
		}(task)
	}
	wg.Wait()

	log.Info("==== summary ====")
	for _, task := range pending {
		if err := results[task]; err != nil {
			log.WithField("task", task).WithError(err).Error("NOT restarted")
		} else {
			log.WithField("task", task).Info("restarted")
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d tasks were not restarted", len(failed), len(pending))
	}
	log.WithField("count", len(pending)).Info("all tasks restarted successfully")
	return nil
}

func restartUntilSuccessful(ctx context.Context, task string) error {
	var delay = *retryDelay
	for attempt := 1; *maxAttempts <= 0 || attempt <= *maxAttempts; attempt++ {
		var logger = log.WithFields(log.Fields{"task": task, "attempt": attempt})
		if err := restartOnce(ctx, logger, task); err == nil {
			logger.Info("restart complete")
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else {
			logger.WithError(err).Warn("restart attempt failed, will retry")
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(delay*2, 10*time.Minute)
	}
	return fmt.Errorf("gave up after %d attempts", *maxAttempts)
}

// restartOnce performs one full disable → verify → enable → verify cycle.
func restartOnce(ctx context.Context, logger *log.Entry, task string) error {
	specFile, err := pullSpec(ctx, task)
	if err != nil {
		return fmt.Errorf("pulling spec: %w", err)
	}

	logger.Info("disabling shards")
	if err := publishWithShardsDisabled(ctx, specFile, task, true); err != nil {
		return fmt.Errorf("publishing disabled spec: %w", err)
	}
	if err := waitForStatus(ctx, logger, task, "TASK_DISABLED"); err != nil {
		return fmt.Errorf("waiting for disable: %w", err)
	}

	// Re-pull so the enable publish carries the expectPubId of the current
	// live spec (normally our disable publication). If anyone else published
	// in between, that publish fails and the whole cycle is retried rather
	// than overwriting their change.
	logger.Info("re-enabling shards")
	if specFile, err = pullSpec(ctx, task); err != nil {
		return fmt.Errorf("re-pulling spec: %w", err)
	}
	if err := publishWithShardsDisabled(ctx, specFile, task, false); err != nil {
		return fmt.Errorf("publishing enabled spec: %w", err)
	}
	if err := waitForStatus(ctx, logger, task, "OK"); err != nil {
		return fmt.Errorf("waiting for task to become OK: %w", err)
	}
	return nil
}

func pullSpec(ctx context.Context, task string) (string, error) {
	var dir = filepath.Join(*workDir, strings.ReplaceAll(task, "/", "_"))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	var specFile = filepath.Join(dir, "flow.yaml")
	if _, err := flowctl(ctx, "catalog", "pull-specs", "--name", task, "--overwrite", "--flat", "--target", specFile); err != nil {
		return "", err
	}
	return specFile, nil
}

// publishWithShardsDisabled rewrites the task's spec so that shards.disable
// is set (or removed, when disabled is false) and publishes it. The pulled
// expectPubId is kept so the publish is rejected if the live spec changed
// since the pull.
func publishWithShardsDisabled(ctx context.Context, specFile, task string, disabled bool) error {
	bs, err := os.ReadFile(specFile)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(bs, &doc); err != nil {
		return fmt.Errorf("parsing %s: %w", specFile, err)
	}
	taskNode, err := findTaskNode(&doc, task)
	if err != nil {
		return err
	}
	if disabled {
		setBool(mapChild(taskNode, "shards", true), "disable", true)
	} else if shards := mapChild(taskNode, "shards", false); shards != nil {
		deleteKey(shards, "disable")
		if len(shards.Content) == 0 {
			deleteKey(taskNode, "shards")
		}
	}

	var buf bytes.Buffer
	var enc = yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return err
	}
	if err := os.WriteFile(specFile, buf.Bytes(), 0644); err != nil {
		return err
	}
	_, err = flowctl(ctx, "catalog", "publish", "--source", specFile, "--auto-approve")
	return err
}

func findTaskNode(doc *yaml.Node, task string) (*yaml.Node, error) {
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("spec is not a YAML mapping")
	}
	var root = doc.Content[0]
	for _, section := range []string{"captures", "materializations"} {
		if m := mapChild(root, section, false); m != nil {
			if t := mapChild(m, task, false); t != nil {
				return t, nil
			}
		}
	}
	return nil, fmt.Errorf("task %q not found under captures or materializations in pulled spec", task)
}

// mapChild returns the mapping value for key, creating an empty mapping when
// create is set and the key is absent.
func mapChild(m *yaml.Node, key string, create bool) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	if !create {
		return nil
	}
	var v = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	m.Content = append(m.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, v)
	return v
}

func setBool(m *yaml.Node, key string, val bool) {
	var v = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: fmt.Sprint(val)}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1] = v
			return
		}
	}
	m.Content = append(m.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, v)
}

func deleteKey(m *yaml.Node, key string) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content = append(m.Content[:i], m.Content[i+2:]...)
			return
		}
	}
}

// waitForStatus polls the task's status until its type matches want. An ERROR
// status while waiting for OK fails immediately rather than waiting out the
// timeout, since the task will not recover on its own.
func waitForStatus(ctx context.Context, logger *log.Entry, task, want string) error {
	var deadline = time.Now().Add(*phaseWait)
	var last string
	for {
		typ, summary, err := taskStatus(ctx, task)
		if err != nil {
			logger.WithError(err).Debug("status check failed, will retry")
		} else {
			if typ == want {
				return nil
			}
			if typ == "ERROR" && want == "OK" {
				return fmt.Errorf("task entered ERROR: %s", summary)
			}
			if typ != last {
				logger.WithFields(log.Fields{"status": typ, "summary": summary, "want": want}).Info("waiting")
				last = typ
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for status %s (last seen %q)", *phaseWait, want, last)
		}
		select {
		case <-time.After(*pollEvery):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func taskStatus(ctx context.Context, task string) (typ, summary string, err error) {
	out, err := flowctl(ctx, "catalog", "status", task, "--output", "json")
	if err != nil {
		return "", "", err
	}
	type entry struct {
		Status struct {
			Type    string `json:"type"`
			Summary string `json:"summary"`
		} `json:"status"`
	}
	var trimmed = bytes.TrimSpace(out)
	if bytes.HasPrefix(trimmed, []byte("[")) {
		var entries []entry
		if err := json.Unmarshal(trimmed, &entries); err != nil {
			return "", "", fmt.Errorf("parsing status: %w", err)
		} else if len(entries) == 0 {
			return "", "", errors.New("empty status response")
		}
		return entries[0].Status.Type, entries[0].Status.Summary, nil
	}
	var e entry
	if err := json.Unmarshal(trimmed, &e); err != nil {
		return "", "", fmt.Errorf("parsing status: %w", err)
	}
	return e.Status.Type, e.Status.Summary, nil
}

func flowctl(ctx context.Context, args ...string) ([]byte, error) {
	log.WithField("command", args).Debug("executing flowctl command")
	var cmd = exec.CommandContext(ctx, "flowctl", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("flowctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func loadTaskList() ([]string, error) {
	var seen = map[string]bool{}
	var tasks []string
	var add = func(name string) {
		name = strings.TrimSpace(name)
		if name != "" && !strings.HasPrefix(name, "#") && !seen[name] {
			seen[name] = true
			tasks = append(tasks, name)
		}
	}
	for _, t := range strings.Split(*tasksFlag, ",") {
		add(t)
	}
	if *tasksFile != "" {
		f, err := os.Open(*tasksFile)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		var sc = bufio.NewScanner(f)
		for sc.Scan() {
			add(sc.Text())
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	if len(tasks) == 0 {
		return nil, errors.New("no tasks specified: use --tasks and/or --file")
	}
	return tasks, nil
}

// progress is an append-only record of restarted tasks, used to skip them
// when the script is re-run.
type progress struct {
	mu   sync.Mutex
	done map[string]bool
	f    *os.File
}

func newProgress(path string) (*progress, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening progress file: %w", err)
	}
	var p = &progress{done: map[string]bool{}, f: f}
	var sc = bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			p.done[line] = true
		}
	}
	return p, sc.Err()
}

func (p *progress) Done(task string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[task]
}

func (p *progress) Record(task string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[task] = true
	_, err := fmt.Fprintln(p.f, task)
	return err
}

func (p *progress) Close() error { return p.f.Close() }
