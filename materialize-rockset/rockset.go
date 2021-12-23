package materialize_rockset

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

// Only creates the named collection if it does not already exist.
func createNewWorkspace(ctx context.Context, client *client, workspace string) (*Workspace, error) {
	if res, err := getWorkspace(ctx, client, workspace); err != nil {
		return nil, err
	} else if res != nil {
		// This workspace exists within Rockset already.
		return nil, nil
	} else {
		// This collection does not exist within Rockset yet, so we should create it.
		return createWorkspace(ctx, client, workspace)
	}
}

func getWorkspace(ctx context.Context, client *client, workspace string) (*Workspace, error) {
	res, err := client.GetWorkspace(ctx, workspace)
	if se, ok := err.(*StatusError); ok && se.NotFound() {
		// Everything worked, but this workspace does not exist.
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch workspace `%s`: %w", workspace, err)
	} else {
		return res, nil
	}
}

func createWorkspace(ctx context.Context, client *client, workspaceName string) (*Workspace, error) {
	workspace := CreateWorkspace{Name: workspaceName}
	if res, err := client.CreateWorkspace(ctx, &workspace); err != nil {
		return nil, fmt.Errorf("failed to create workspace `%s`: %w", workspaceName, err)
	} else {
		return res, nil
	}
}

// Only creates the named collection if it does not already exist. The returned boolean indicates whether it was
// actually created. It will be false if the collection already exists or if an error is returned.
func createNewCollection(ctx context.Context, client *client, workspace string, collection string, integration *cloudStorageIntegration) (bool, error) {
	if res, err := getCollection(ctx, client, workspace, collection); err != nil {
		return false, err
	} else if res != nil {
		// This collection exists within Rockset already, so just validate that
		// it has the required integration.  Collection definitions in Rockset
		// are immutable, so there's no way to add an integration to an existing
		// collection.  Thus, if the integration named in the resource
		// configuration does not exist, it must be returned as an error.
		if integration != nil && res.GetIntegrationSource(integration.Integration) == nil {
			return false, fmt.Errorf("expected collection '%s' to have a source with an integration named '%s', but no such integration source exists", collection, integration)
		}
		return false, nil
	} else {
		// This collection does not exist within Rockset yet, so we should create it.
		var err = createCollection(ctx, client, workspace, collection, integration)
		return err == nil, err
	}
}

func getCollection(ctx context.Context, client *client, workspace string, collection string) (*Collection, error) {
	res, err := client.GetCollection(ctx, workspace, collection)
	if se, ok := err.(*StatusError); ok && se.NotFound() {
		// Everything worked, but this collection does not exist.
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch collection `%s`: %w", collection, err)
	} else {
		return res, nil
	}
}

func createCollection(ctx context.Context, client *client, workspace string, collectionName string, integration *cloudStorageIntegration) error {
	collection := CreateCollection{Name: collectionName}
	if integration != nil {
		collection.Sources = []CreateCollectionSource{{
			IntegrationName: integration.Integration,
			S3: &S3Integration{
				Bucket:  integration.Bucket,
				Region:  integration.Region,
				Prefix:  integration.Prefix,
				Pattern: integration.Pattern,
			},
		}}
	}
	_, err := client.CreateCollection(ctx, workspace, &collection)
	if err != nil {
		return fmt.Errorf("failed to create collection `%s`: %w", collectionName, err)
	}
	return nil
}

// awaitCollectionReady blocks until the given Rockset collection has a READY status AND has completed
// ingestion of all the files in the cloud storage bucket for the given integration. This is required in order to
// successfully hand off from a cloud storage materailization while preserving the ingestion order of documents.
// Basically, we need to wait until all the documents in the cloud storage bucket have been ingested before we can start
// using the write API, or else an earlier document from cloud storage may overwrite the one we ingest using the write
// API.
func awaitCollectionReady(ctx context.Context, client *client, workspace, collectionName, integration string) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var collection, err = getCollection(ctx, client, workspace, collectionName)
		if err != nil {
			return err
		}
		if collection == nil {
			return fmt.Errorf("rockset collection '%s' does not exist or has been deleted", collectionName)
		}

		var ready = collection.Status == STATUS_READY

		// We'll check to see if a collection has made no progress and emit a specific warning in that case.  If a
		// Rockset collection isn't using a large enough instance, then it'll silently refuse to actually use the
		// integration that was configured. This is a really easy mistake to make, and there doesn't appear to be a way
		// to actually detect the instance size via the API, so we'll try to log a helpful warning if the collection
		// goes too long without downloading any of the objects from cloud storage.
		var bulkIngestStalled = false
		var collectionAge = time.Since(collection.CreatedAt)
		// If an integration was specified, then we need to _also_ wait for that integration to fully process all of its
		// files. Technically, this will already be the case if there was enough data in the source bucket to trigger a
		// "bulk load", because in that case Rockset will not set the status to READY until after all the files have
		// been uploaded. BUT, if the source bucket didn't contain enough data to trigger the "bulk load", then the
		// status may be set to READY before all of the source files have been processed. This condition is a guard
		// against ingesting data out of order in that specific case.
		if integration != "" {
			var source = collection.GetCloudStorageSource(integration)
			if source == nil {
				return fmt.Errorf("expected collection '%s' to have a compatible cloud-storage integration named '%s', but no such source exists", collectionName, integration)
			}
			ready = ready && source.ObjectCountTotal == source.ObjectCountDownloaded
			bulkIngestStalled = source.ObjectCountTotal > 0 && source.ObjectCountDownloaded == 0 && collectionAge > time.Minute*5
		}

		var logEntry = log.WithFields(log.Fields{
			"rocksetWorkspace":  workspace,
			"rocksetCollection": collection,
		})
		if ready {
			logEntry.Info("Rockset collection has completed bulk ingestion")
			return nil
		} else if bulkIngestStalled {
			logEntry.Warn("Rockset collection integration has not made any progress. Is your Rockset virtual instance large enough to support bulk ingestion?")
		} else {
			logEntry.Info("Rockset collection integration has not yet caught up (will retry)")
		}

		time.Sleep(nextIngestCompletionBackoff(collectionAge))
	}
}

func nextIngestCompletionBackoff(timeSinceCreation time.Duration) time.Duration {
	var baseSeconds = 30
	// If we've been waiting _this_ long, we might as well slow down a little to
	// avoid getting rate limited by the Rockset API in cases where there's a
	// lot of shards with a lot of bindings all waiting.
	if timeSinceCreation > time.Hour {
		baseSeconds = 90
	}
	// Add a significant random jitter, so that requests for multiple bindings
	// don't all get sent at the same time and run into rate limits.
	baseSeconds += rand.Intn(30)
	return time.Second * time.Duration(baseSeconds)
}

const STATUS_READY = "READY"
