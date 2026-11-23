package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	// deleteByQueryAction is the task action of a delete by query.
	deleteByQueryAction = "indices:data/write/delete/byquery"

	// truncationLogInterval is how often the progress of a running
	// truncation is logged.
	truncationLogInterval = 5 * time.Minute
)

type client struct {
	es *elasticsearch.Client
}

type createIndexParams struct {
	Settings indexSettings `json:"settings,omitempty"`
	Mappings indexMappings `json:"mappings"`
}

type indexSettings struct {
	Shards   *int `json:"number_of_shards,omitempty" jsonschema_extras:"nonsensitive=true"`
	Replicas *int `json:"number_of_replicas,omitempty" jsonschema_extras:"nonsensitive=true"`
}

type indexMappings struct {
	Properties map[string]property `json:"properties"`
}

// createIndex creates a new index and sets its mappings per indexProps if it doesn't already exist.
func (c *client) createIndex(ctx context.Context, index string, shards *int, replicas *int, indexProps map[string]property) error {
	existResp, err := c.es.Indices.Exists(
		[]string{index},
		c.es.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("checking if index exists: %w", err)
	}
	defer existResp.Body.Close()

	if existResp.StatusCode == http.StatusOK {
		return nil
	} else if existResp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("index exists unexpected status code: %d", existResp.StatusCode)
	}

	// Index does not exist, create a new one.
	params := createIndexParams{
		Settings: indexSettings{
			Shards:   shards,
			Replicas: replicas,
		},
		Mappings: indexMappings{
			Properties: indexProps,
		},
	}

	createResp, err := c.es.Indices.Create(
		index,
		c.es.Indices.Create.WithContext(ctx),
		c.es.Indices.Create.WithBody(esutil.NewJSONReader(params)),
	)
	if err != nil {
		return fmt.Errorf("createIndex: %w", err)
	}
	defer createResp.Body.Close()
	if createResp.IsError() {
		return fmt.Errorf("createIndex error response [%s] %s", createResp.Status(), createResp.String())
	}

	return nil
}

// deleteIndex deletes an index with the provided name.
func (c *client) deleteIndex(ctx context.Context, index string) error {
	res, err := c.es.Indices.Delete(
		[]string{index},
		c.es.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("deleting existing index: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("delete index error response [%s] %s", res.Status(), res.String())
	}

	return nil
}

// deleteBefore removes documents from an index whose dateField value is
// earlier than before, and returns the number deleted. A document with no
// value for dateField is left alone, since there's no publication time to
// compare. The delete is tagged with an opaque ID that starts with tagPrefix,
// which identifies it as this materialization's truncation of index.
// isServerless is true for a serverless cluster, which doesn't support
// listing tasks.
func (c *client) deleteBefore(ctx context.Context, index string, dateField string, before time.Time, tagPrefix string, isServerless bool) (int64, error) {
	// DeleteByQuery finds the documents to delete with a search, and a search
	// only sees the documents as of the index's last refresh. A bulk store is
	// durable once it's acknowledged, but its documents don't become visible
	// to searches until the next refresh.
	//
	// Refreshes happen periodically, on the index's refresh interval, which
	// can be lengthened or disabled. So without an explicit refresh, the
	// search could miss documents from the stores just completed, and could
	// match stale versions of documents they rewrote, which would fail the
	// delete with version conflicts.
	err := c.refreshIndex(ctx, index)
	if err != nil {
		return 0, fmt.Errorf("refreshing index %q before truncation: %w", index, err)
	}

	// A version conflict stops the delete and fails the transaction. A
	// document changes after the query matched it only if something else
	// writes to the index while the delete runs, since this connector doesn't
	// store anything then. That could be an overlapping delete from an
	// earlier run that couldn't be cancelled, or a writer other than this
	// connector. Either way, the documents the delete matched may no longer
	// be the ones it should delete, so the replay of the transaction
	// truncates again.
	query := map[string]any{
		"query": map[string]any{
			"range": map[string]any{dateField: map[string]any{
				"lt":     before.UTC().Format(time.RFC3339Nano),
				"format": "strict_date_optional_time_nanos",
			}},
		},
	}

	// The delete runs as a background task that is polled until it
	// completes. Deleting from a large index can take longer than proxies in
	// front of the cluster allow a single request to take.
	//
	// To avoid starting multiple tasks, before a retried start sends another
	// request, it looks for a task with the query's opaque ID. If an earlier
	// attempt started that task and it's still running, the retry waits on it
	// instead. The opaque ID ends with a random suffix, so a retry never
	// mistakes a delete from an earlier run for its own.
	//
	// A serverless cluster can't list tasks, so there a retried start always
	// sends another request. If that starts a second task, the two overlap
	// and the later one fails with a version conflict.
	var opaqueID = tagPrefix + uuid.NewString()
	var taskID string
	var attempted bool
	err = requestRetry.Do(ctx, "starting delete by query", func(ctx context.Context) error {
		if attempted && !isServerless {
			running, err := c.listDeleteByQueryTasks(ctx, []string{tagPrefix})
			if err != nil {
				return err
			}
			for id, tag := range running {
				if tag == opaqueID {
					taskID = id
					return nil
				}
			}
		}
		attempted = true

		var err error
		taskID, err = c.startDeleteByQuery(ctx, index, query, opaqueID)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("starting delete by query of index %q: %w", index, err)
	}

	var res *deleteByQueryResult
	var started = time.Now()
	var lastLogged = started
	err = taskPollRetry.Do(ctx, "waiting for delete by query", func(ctx context.Context) error {
		var err error
		res, err = c.checkDeleteByQuery(ctx, taskID)
		if running, ok := errors.AsType[*taskRunningError](err); ok && time.Since(lastLogged) >= truncationLogInterval {
			lastLogged = time.Now()
			log.WithFields(log.Fields{
				"index":   index,
				"task":    taskID,
				"total":   running.total,
				"deleted": running.deleted,
				"elapsed": time.Since(started).Truncate(time.Second).String(),
			}).Info("truncation in progress")
		}
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("deleting documents from index %q: %w", index, err)
	}

	err = res.err()
	if err != nil {
		return 0, fmt.Errorf("delete by query of index %q %w", index, err)
	}

	return res.Deleted, nil
}

// refreshIndex refreshes index, making its recent writes visible to searches.
func (c *client) refreshIndex(ctx context.Context, index string) error {
	return requestRetry.Do(ctx, "refreshing index", func(ctx context.Context) error {
		r, err := c.es.Indices.Refresh(
			c.es.Indices.Refresh.WithContext(ctx),
			c.es.Indices.Refresh.WithIndex(index),
		)
		if err != nil {
			return &transportError{err}
		}
		defer r.Body.Close()
		if r.IsError() {
			return newResponseError(r)
		}
		return nil
	})
}

// listDeleteByQueryTasks returns the running delete by query tasks with an
// opaque ID starting with one of prefixes, as a map of task ID to opaque ID.
func (c *client) listDeleteByQueryTasks(ctx context.Context, prefixes []string) (map[string]string, error) {
	r, err := c.es.Tasks.List(
		c.es.Tasks.List.WithContext(ctx),
		c.es.Tasks.List.WithActions(deleteByQueryAction),
		c.es.Tasks.List.WithDetailed(true),
	)
	if err != nil {
		return nil, &transportError{err}
	}
	defer r.Body.Close()
	if r.IsError() {
		return nil, newResponseError(r)
	}

	var parsed tasksListResponse
	err = json.NewDecoder(r.Body).Decode(&parsed)
	if err != nil {
		return nil, fmt.Errorf("decoding task list response: %w", err)
	}
	return parsed.matching(prefixes), nil
}

// tasksListResponse is a response listing tasks grouped by node, keyed by
// task ID.
type tasksListResponse struct {
	Nodes map[string]struct {
		Tasks map[string]struct {
			Action  string            `json:"action"`
			Headers map[string]string `json:"headers"`
		} `json:"tasks"`
	} `json:"nodes"`
}

// matching returns the delete by query tasks with an opaque ID starting with
// one of prefixes, as a map of task ID to opaque ID.
func (r tasksListResponse) matching(prefixes []string) map[string]string {
	out := make(map[string]string)
	for _, node := range r.Nodes {
		for id, task := range node.Tasks {
			tag, ok := task.Headers["X-Opaque-Id"]
			if !ok || task.Action != deleteByQueryAction {
				continue
			}
			for _, prefix := range prefixes {
				if strings.HasPrefix(tag, prefix) {
					out[id] = tag
					break
				}
			}
		}
	}
	return out
}

// cancelTask cancels the task taskID and waits for it to stop. A task that
// already stopped is left alone.
func (c *client) cancelTask(ctx context.Context, taskID string) error {
	return requestRetry.Do(ctx, "cancelling task", func(ctx context.Context) error {
		r, err := c.es.Tasks.Cancel(
			c.es.Tasks.Cancel.WithContext(ctx),
			c.es.Tasks.Cancel.WithTaskID(taskID),
			c.es.Tasks.Cancel.WithWaitForCompletion(true),
		)
		if err != nil {
			return &transportError{err}
		}
		defer r.Body.Close()
		if r.IsError() {
			return newResponseError(r)
		}

		var parsed cancelTaskResponse
		err = json.NewDecoder(r.Body).Decode(&parsed)
		if err != nil {
			return fmt.Errorf("decoding cancel task response: %w", err)
		}
		return parsed.err(taskID)
	})
}

// esError is an error reported within an Elasticsearch response.
type esError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// cancelTaskResponse is a response to cancelling a task.
type cancelTaskResponse struct {
	NodeFailures []struct {
		esError
		// A node failure's own reason only names the node, so its cause
		// says what went wrong.
		CausedBy *esError `json:"caused_by"`
	} `json:"node_failures"`
	TaskFailures []struct {
		Reason esError `json:"reason"`
	} `json:"task_failures"`
}

// err returns why cancelling taskID failed, or nil if it succeeded. A task
// that already stopped is reported as a node failure caused by the task not
// being found, which is not an error.
func (r cancelTaskResponse) err(taskID string) error {
	if len(r.TaskFailures) > 0 {
		reason := r.TaskFailures[0].Reason
		return fmt.Errorf("cancelling task %s failed: %s: %s", taskID, reason.Type, reason.Reason)
	}
	for _, f := range r.NodeFailures {
		cause := f.esError
		if f.CausedBy != nil {
			cause = *f.CausedBy
		}
		if cause.Type == "resource_not_found_exception" {
			continue
		}
		return fmt.Errorf("cancelling task %s failed: %s: %s", taskID, cause.Type, cause.Reason)
	}
	return nil
}

// deleteByQueryResult is the outcome of a completed delete by query.
type deleteByQueryResult struct {
	Deleted  int64 `json:"deleted"`
	TimedOut bool  `json:"timed_out"`
	// Failures includes a version conflict, which stops the delete.
	Failures []deleteByQueryFailure `json:"failures"`
	// Canceled is the reason the delete was cancelled, if it was. A
	// cancelled delete stops partway through without reporting a failure.
	Canceled string `json:"canceled"`
}

// deleteByQueryFailure is a failure of a delete by query: either a document
// that failed to be deleted, whose error is its cause, or a search that
// failed, whose error is its reason.
type deleteByQueryFailure struct {
	Cause  *esError `json:"cause"`
	Reason *esError `json:"reason"`
}

// err returns why the delete stopped before deleting every matched document,
// or nil if it didn't.
func (r *deleteByQueryResult) err() error {
	if len(r.Failures) > 0 {
		cause := r.Failures[0].Cause
		if cause == nil {
			cause = r.Failures[0].Reason
		}
		if cause == nil {
			return fmt.Errorf("failed after deleting %d documents", r.Deleted)
		}
		return fmt.Errorf("failed after deleting %d documents: %s: %s", r.Deleted, cause.Type, cause.Reason)
	} else if r.TimedOut {
		return fmt.Errorf("timed out after deleting %d documents", r.Deleted)
	} else if r.Canceled != "" {
		return fmt.Errorf("was cancelled (%s) after deleting %d documents", r.Canceled, r.Deleted)
	}
	return nil
}

// startDeleteByQuery starts a delete by query of index as a background task
// tagged with opaqueID, and returns the task's ID.
//
// A background task stores its result as a document in the .tasks system
// index when it completes, and nothing ever removes it. The document is left
// in place: deleting it requires the delete privilege on .tasks with
// allow_restricted_indices, which even the superuser role lacks, and direct
// access to system indices is deprecated.
func (c *client) startDeleteByQuery(ctx context.Context, index string, query map[string]any, opaqueID string) (string, error) {
	r, err := c.es.DeleteByQuery(
		[]string{index},
		esutil.NewJSONReader(query),
		c.es.DeleteByQuery.WithContext(ctx),
		c.es.DeleteByQuery.WithRefresh(true),
		c.es.DeleteByQuery.WithWaitForCompletion(false),
		c.es.DeleteByQuery.WithOpaqueID(opaqueID),
	)
	if err != nil {
		return "", &transportError{err}
	}
	defer r.Body.Close()
	if r.IsError() {
		return "", newResponseError(r)
	}

	var parsed struct {
		Task string `json:"task"`
	}
	err = json.NewDecoder(r.Body).Decode(&parsed)
	if err != nil {
		return "", fmt.Errorf("decoding delete by query response: %w", err)
	} else if parsed.Task == "" {
		return "", fmt.Errorf("delete by query response has no task ID")
	}
	return parsed.Task, nil
}

// checkDeleteByQuery checks the delete by query task taskID once, and returns
// its result if it has completed, or a taskRunningError with its progress if
// it hasn't.
func (c *client) checkDeleteByQuery(ctx context.Context, taskID string) (*deleteByQueryResult, error) {
	r, err := c.es.Tasks.Get(taskID, c.es.Tasks.Get.WithContext(ctx))
	if err != nil {
		return nil, &transportError{err}
	}
	defer r.Body.Close()
	if r.IsError() {
		return nil, newResponseError(r)
	}

	var task struct {
		Completed bool `json:"completed"`
		Task      struct {
			Status struct {
				Total   int64 `json:"total"`
				Deleted int64 `json:"deleted"`
			} `json:"status"`
		} `json:"task"`
		Response *deleteByQueryResult `json:"response"`
		Error    *esError             `json:"error"`
	}
	err = json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("decoding task response: %w", err)
	}

	if !task.Completed {
		return nil, &taskRunningError{
			taskID:  taskID,
			total:   task.Task.Status.Total,
			deleted: task.Task.Status.Deleted,
		}
	} else if task.Error != nil {
		return nil, fmt.Errorf("delete by query task %s failed: %s: %s", taskID, task.Error.Type, task.Error.Reason)
	} else if task.Response == nil {
		return nil, fmt.Errorf("delete by query task %s completed without a response", taskID)
	}
	return task.Response, nil
}

func (c *client) addMappingToIndex(ctx context.Context, index string, field string, prop property) error {
	res, err := c.es.Indices.PutMapping(
		[]string{index},
		esutil.NewJSONReader(map[string]map[string]property{"properties": {field: prop}}),
		c.es.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("addMappingToIndex: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("addMappingToIndex error response [%s] %s", res.Status(), res.String())
	}

	return nil
}

type indexMetaResponse struct {
	Mappings struct {
		Properties map[string]property `json:"properties"`
	} `json:"mappings"`
}

func (c *client) populateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema) error {
	res, err := c.es.Indices.Get(
		[]string{"*"}, // Get info for all indices the connector has access to.
		c.es.Indices.Get.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("getting index metadata: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("getting index metadata error response [%s] %s", res.Status(), res.String())
	}

	var indexMeta map[string]indexMetaResponse
	if err := json.NewDecoder(res.Body).Decode(&indexMeta); err != nil {
		return fmt.Errorf("decoding index metadata response: %w", err)
	}

	for index, meta := range indexMeta {
		res := is.PushResource(index)
		for field, prop := range meta.Mappings.Properties {
			res.PushField(boilerplate.ExistingField{
				Name:               field,
				Nullable:           true,
				Type:               string(prop.Type),
				CharacterMaxLength: 0,
				Format:             prop.Format,
			})
		}
	}

	return nil
}

// serverInfo describes the cluster on the other end of the connection, as
// reported by the root info endpoint. It is used to distinguish Elasticsearch
// from OpenSearch (an API-compatible fork), and Elastic Cloud serverless from
// self-managed clusters.
type serverInfo struct {
	// Distribution is "opensearch" for OpenSearch clusters and empty (the field
	// is absent) for Elasticsearch.
	Distribution string
	// BuildFlavor is "serverless" for Elastic Cloud serverless clusters.
	BuildFlavor string
}

func (i serverInfo) isOpenSearch() bool {
	return strings.EqualFold(i.Distribution, "opensearch")
}

func (i serverInfo) isServerless() bool {
	return strings.EqualFold(i.BuildFlavor, "serverless")
}

func (c *client) info(ctx context.Context) (serverInfo, error) {
	res, err := c.es.Info(c.es.Info.WithContext(ctx))
	if err != nil {
		return serverInfo{}, fmt.Errorf("getting server info: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return serverInfo{}, fmt.Errorf("getting server info error response [%s] %s", res.Status(), res.String())
	}

	type infoResponse struct {
		Version struct {
			Distribution string `json:"distribution"`
			BuildFlavor  string `json:"build_flavor"`
		} `json:"version"`
	}

	var info infoResponse
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return serverInfo{}, fmt.Errorf("decoding server info response: %w", err)
	}

	return serverInfo{
		Distribution: info.Version.Distribution,
		BuildFlavor:  info.Version.BuildFlavor,
	}, nil
}
