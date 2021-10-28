package materialize_rockset

import (
	"context"
	"fmt"
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
	if res, err := client.GetWorkspace(ctx, workspace); err == ErrNotFound {
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

		// TODO: Try fetching the collection until it exists. Rockset isn't quite ready as soon as this API call returns.
		return createCollection(ctx, client, workspace, collection)
	}
}

func getCollection(ctx context.Context, client *client, workspace string, collection string) (*Collection, error) {
	if res, err := client.GetCollection(ctx, workspace, collection); err == ErrNotFound {
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
	if res, err := client.CreateCollection(ctx, workspace, &collection); err != nil {
		return nil, fmt.Errorf("failed to create collection `%s`: %w", collectionName, err)
	} else {
		return res, nil
	}
}
