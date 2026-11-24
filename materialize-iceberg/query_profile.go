package connector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	log "github.com/sirupsen/logrus"
)

const (
	// profileInterval is how often the profiles of finished jobs are logged,
	// once the compute backend has reported what it does of a job after it
	// ends.
	profileInterval = 15 * time.Second
	// profileMaxBindings is the most bindings logged of a merge job: those
	// the job took the longest to merge.
	profileMaxBindings = 10
)

// computeDescriber is implemented by compute backends that can describe what
// runs their jobs.
type computeDescriber interface {
	describe(ctx context.Context) (log.Fields, error)
}

// newQueryProfiler profiles the load and merge jobs of the bindings. A
// table's first profile also logs the table's metadata from the catalog, and
// the compute application that ran the job.
func newQueryProfiler(cat *catalog.Catalog, compute computeRunner, bindings []boilerplate.MappedBinding[config, resource, mapped]) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	for i, b := range bindings {
		paths[i] = b.ResourcePath
	}
	var history boilerplate.QueryHistory
	if h, ok := compute.(boilerplate.QueryHistory); ok {
		history = h
	}
	// The application is the same for every table, so only the first table
	// to be profiled logs it.
	var described bool
	return boilerplate.NewQueryProfiler(history, profileInterval, nil, paths, func(ctx context.Context, binding int) (log.Fields, error) {
		var out = make(log.Fields)
		var errs []error

		if tbl, err := cat.GetTable(ctx, paths[binding][0], paths[binding][1]); err != nil {
			errs = append(errs, fmt.Errorf("getting table: %w", err))
		} else {
			out["table"] = tableFields(tbl.Metadata)
		}

		if describer, ok := compute.(computeDescriber); ok && !described {
			described = true
			if application, err := describer.describe(ctx); err != nil {
				errs = append(errs, err)
			} else {
				out["application"] = application
			}
		}

		return out, errors.Join(errs...)
	})
}

// traceJob adds what the compute backend reports of a job to its profile.
func traceJob(trace *boilerplate.QueryTrace, res jobResult) {
	if res.id != "" {
		trace.SetID(res.id)
	}
	for k, v := range res.fields {
		trace.Set(k, v)
	}
	if res.stats.QueryMs > 0 {
		trace.Set("queryMs", res.stats.QueryMs)
	}
}

// mergedBinding is a binding whose pending merge a job ran.
type mergedBinding struct {
	idx  int
	path []string
	*pendingMerge
}

// profileMerges adds the bindings a merge job ran to its profile, with what
// the job measured of each.
func profileMerges(profile *boilerplate.QueryProfile, trace *boilerplate.QueryTrace, merged []mergedBinding, stats []python.BindingStats) {
	var byBinding = make(map[int]python.BindingStats, len(stats))
	for _, s := range stats {
		byBinding[s.Binding] = s
	}

	merged, omitted := slowestMerges(merged, byBinding)
	if omitted > 0 {
		trace.Set("bindingsOmitted", omitted)
	}
	for _, m := range merged {
		// Lists the binding even if nothing more is known of it.
		profile.AddKeys(m.idx, 0)
		if m.bounds != nil {
			profile.SetBounds(m.idx, m.bounds)
		}
		for k, v := range mergeFields(m, byBinding) {
			profile.Set(m.idx, k, v)
		}
	}
}

// slowestMerges are the bindings to log of a merge job and how many are left
// out: all of them, or the ones the job took the longest over if it measured
// more than a profile logs.
func slowestMerges(merged []mergedBinding, stats map[int]python.BindingStats) ([]mergedBinding, int) {
	if len(stats) == 0 || len(merged) <= profileMaxBindings {
		return merged, 0
	}
	var took = func(m mergedBinding) int64 {
		return stats[m.Binding].MergeMs + stats[m.Binding].TruncateMs
	}
	merged = slices.SortedStableFunc(slices.Values(merged), func(a, b mergedBinding) int {
		return cmp.Compare(took(b), took(a))
	})
	return merged[:profileMaxBindings], len(merged) - profileMaxBindings
}

// mergeFields are the rows staged for a binding's merge, and how long the job
// took to merge and truncate it and what the merge committed, if the job
// reported them.
func mergeFields(m mergedBinding, stats map[int]python.BindingStats) log.Fields {
	var out = make(log.Fields)
	if m.Rows > 0 {
		out["rows"] = m.Rows
	}
	s, ok := stats[m.Binding]
	if !ok {
		return out
	}
	if m.Query != "" {
		out["mergeMs"] = s.MergeMs
	}
	if m.TruncateQuery != "" {
		out["truncateMs"] = s.TruncateMs
	}
	if s.Retries > 0 {
		out["retries"] = s.Retries
	}
	if len(s.Commit) > 0 {
		out["commit"] = summaryFields(s.Commit)
	}
	return out
}

// summaryFields picks the operation and counts of a snapshot summary: the
// files, records and deletes its commit added and removed, and the table's
// totals after it.
func summaryFields(summary map[string]string) log.Fields {
	var out = make(log.Fields)
	for k, v := range summary {
		if k == "operation" {
			out[k] = v
		} else if n, err := strconv.ParseInt(v, 10, 64); err != nil {
			continue
		} else if k == "changed-partition-count" || slices.ContainsFunc([]string{"added-", "deleted-", "removed-", "total-"}, func(p string) bool { return strings.HasPrefix(k, p) }) {
			out[k] = n
		}
	}
	return out
}

// loggedPropertyPrefixes are the namespaces of Iceberg's own table
// properties. Other properties are the user's, and may say anything.
var loggedPropertyPrefixes = []string{"write.", "read.", "commit.", "history."}

// tableFields are what of a table's metadata explains a slow load or merge:
// its format version, partitioning and sort order, how rows are deleted and
// updated, and the files, deletes and records of its current snapshot.
func tableFields(md table.Metadata) log.Fields {
	var out = log.Fields{
		"formatVersion": md.Version(),
		"snapshots":     len(md.Snapshots()),
	}

	var schema = md.CurrentSchema()
	var column = func(sourceIDs []int) string {
		var names = make([]string, len(sourceIDs))
		for i, id := range sourceIDs {
			names[i], _ = schema.FindColumnName(id)
		}
		return strings.Join(names, ", ")
	}
	var spec = md.PartitionSpec()
	var partitions []string
	for _, f := range spec.Fields() {
		partitions = append(partitions, fmt.Sprintf("%s(%s)", f.Transform, column(f.SourceIDs)))
	}
	if len(partitions) > 0 {
		out["partitionSpec"] = partitions
	}
	var sortOrder []string
	for _, f := range md.SortOrder().Fields() {
		var field = column(f.SourceIDs)
		if _, identity := f.Transform.(iceberg.IdentityTransform); !identity {
			field = fmt.Sprintf("%s(%s)", f.Transform, field)
		}
		sortOrder = append(sortOrder, fmt.Sprintf("%s %s", field, f.Direction))
	}
	if len(sortOrder) > 0 {
		out["sortOrder"] = sortOrder
	}

	var properties = make(map[string]string)
	for k, v := range md.Properties() {
		if slices.ContainsFunc(loggedPropertyPrefixes, func(p string) bool { return strings.HasPrefix(k, p) }) {
			properties[k] = v
		}
	}
	if len(properties) > 0 {
		out["properties"] = properties
	}

	if snapshot := md.CurrentSnapshot(); snapshot != nil {
		out["snapshotAt"] = time.UnixMilli(snapshot.TimestampMs).UTC().Format(time.RFC3339)
		if s := snapshot.Summary; s != nil {
			var summary = summaryFields(s.Properties)
			summary["operation"] = string(s.Operation)
			out["snapshot"] = summary
		}
	}
	return out
}

// describe reads the application's release, capacity limits, and workers
// kept initialized, which the execution role may not be allowed to.
func (e *emrClient) describe(ctx context.Context) (log.Fields, error) {
	out, err := e.c.GetApplication(ctx, &emr.GetApplicationInput{ApplicationId: aws.String(e.cfg.ApplicationId)})
	if err != nil {
		return nil, fmt.Errorf("getting EMR application: %w", err)
	}
	return applicationFields(out.Application), nil
}

func applicationFields(app *emrTypes.Application) log.Fields {
	var out = log.Fields{
		"releaseLabel": aws.ToString(app.ReleaseLabel),
		"architecture": app.Architecture,
	}
	if m := app.MaximumCapacity; m != nil {
		out["maximumCapacity"] = resourceFields(m.Cpu, m.Memory, m.Disk)
	}
	var initial = make(log.Fields)
	for worker, capacity := range app.InitialCapacity {
		var fields = log.Fields{"workers": aws.ToInt64(capacity.WorkerCount)}
		if w := capacity.WorkerConfiguration; w != nil {
			for k, v := range resourceFields(w.Cpu, w.Memory, w.Disk) {
				fields[k] = v
			}
		}
		initial[strings.ToLower(worker)] = fields
	}
	if len(initial) > 0 {
		out["initialCapacity"] = initial
	}
	if s := app.AutoStopConfiguration; s != nil && aws.ToBool(s.Enabled) {
		out["autoStopIdleMinutes"] = aws.ToInt32(s.IdleTimeoutMinutes)
	}
	if s := app.SchedulerConfiguration; s != nil && s.MaxConcurrentRuns != nil {
		out["maxConcurrentRuns"] = *s.MaxConcurrentRuns
	}
	return out
}

func resourceFields(cpu, memory, disk *string) log.Fields {
	var out = make(log.Fields)
	for name, v := range map[string]*string{"cpu": cpu, "memory": memory, "disk": disk} {
		if v != nil {
			out[name] = *v
		}
	}
	return out
}

// Fetch reads the resources the workers of finished job runs used and were
// billed for, which a job run reports a while after it ends.
func (e *emrClient) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	var out = make(map[string]log.Fields)
	for _, id := range ids {
		got, err := e.c.GetJobRun(ctx, &emr.GetJobRunInput{
			ApplicationId: aws.String(e.cfg.ApplicationId),
			JobRunId:      aws.String(id),
		})
		if err != nil {
			return nil, fmt.Errorf("getting job run %s: %w", id, err)
		}
		if fields := utilizationFields(got.JobRun); fields != nil {
			out[id] = fields
		}
	}
	return out, nil
}

// utilizationFields are the resources a job run's workers used and were
// billed for, or nil until the job run reports both.
func utilizationFields(run *emrTypes.JobRun) log.Fields {
	var total, billed = run.TotalResourceUtilization, run.BilledResourceUtilization
	if total == nil || billed == nil {
		return nil
	}
	return log.Fields{
		"vCPUHours":            aws.ToFloat64(total.VCPUHour),
		"memoryGBHours":        aws.ToFloat64(total.MemoryGBHour),
		"storageGBHours":       aws.ToFloat64(total.StorageGBHour),
		"billedVCPUHours":      aws.ToFloat64(billed.VCPUHour),
		"billedMemoryGBHours":  aws.ToFloat64(billed.MemoryGBHour),
		"billedStorageGBHours": aws.ToFloat64(billed.StorageGBHour),
	}
}

// jobRunFields are what a job run reports of itself as it ends: its state,
// and how long it waited to start and ran.
func jobRunFields(run *emrTypes.JobRun) log.Fields {
	var out = log.Fields{"status": run.State}
	if run.ReleaseLabel != nil {
		out["releaseLabel"] = *run.ReleaseLabel
	}
	if run.CreatedAt != nil && run.StartedAt != nil {
		out["pendingMs"] = run.StartedAt.Sub(*run.CreatedAt).Milliseconds()
	}
	if run.StartedAt != nil && run.EndedAt != nil {
		out["executionMs"] = run.EndedAt.Sub(*run.StartedAt).Milliseconds()
	}
	if queued := aws.ToInt64(run.QueuedDurationMilliseconds); queued > 0 {
		out["queuedMs"] = queued
	}
	if attempt := aws.ToInt32(run.Attempt); attempt > 1 {
		out["attempt"] = attempt
	}
	return out
}
