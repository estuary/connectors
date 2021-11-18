package materialize_rockset

import (
	"context"
	"fmt"
	"time"
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

// Only creates the named collection if it does not already exist.
func createNewCollection(ctx context.Context, client *client, workspace string, collection string) (*Collection, error) {
	if res, err := getCollection(ctx, client, workspace, collection); err != nil {
		return nil, err
	} else if res != nil {
		// This collection exists within Rockset already.
		return nil, nil
	} else {
		// This collection does not exist within Rockset yet, so we should create it.
		return createCollection(ctx, client, workspace, collection)
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

func createCollection(ctx context.Context, client *client, workspace string, collectionName string) (*Collection, error) {
	collection := CreateCollection{Name: collectionName}
	_, err := client.CreateCollection(ctx, workspace, &collection)
	if err != nil {
		return nil, fmt.Errorf("failed to create collection `%s`: %w", collectionName, err)
	}

	time.Sleep(time.Second * 2)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for collection `%s/%s` to be created: %w", workspace, collection.Name, err)
		default:
			if col, err := client.GetCollection(ctx, workspace, collection.Name); err != nil {
				return nil, fmt.Errorf("fetching collection `%s/%s` while waiting for it to be created: %w", workspace, collection.Name, err)
			} else if col.Status != "READY" {
				// Not ready yet, so wait a couple of seconds then we'll try again.
				time.Sleep(time.Second * 2)
			} else {
				return col, nil
			}
		}
	}
}
