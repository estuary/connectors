package materialize_rockset

import (
	"context"
	"fmt"

	rockset "github.com/rockset/rockset-go-client"
	rtypes "github.com/rockset/rockset-go-client/openapi"
	"github.com/rockset/rockset-go-client/option"
	log "github.com/sirupsen/logrus"
)

// Only creates the named collection if it does not already exist.
func ensureWorkspaceExists(ctx context.Context, client *rockset.RockClient, workspace string) (*rtypes.Workspace, error) {
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

func getWorkspace(ctx context.Context, client *rockset.RockClient, workspace string) (*rtypes.Workspace, error) {
	res, err := client.GetWorkspace(ctx, workspace)
	if se, ok := err.(rockset.Error); ok && se.IsNotFoundError() {
		// Everything worked, but this workspace does not exist.
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch workspace `%s`: %w", workspace, err)
	} else {
		return &res, nil
	}
}

func createWorkspace(ctx context.Context, client *rockset.RockClient, workspaceName string) (*rtypes.Workspace, error) {
	if res, err := client.CreateWorkspace(ctx, workspaceName); err != nil {
		return nil, fmt.Errorf("failed to create workspace `%s`: %w", workspaceName, err)
	} else {
		return &res, nil
	}
}

// Only creates the named collection if it does not already exist. The returned boolean indicates whether it was
// actually created. It will be false if the collection already exists or if an error is returned.
func ensureCollectionExists(ctx context.Context, client *rockset.RockClient, resource *resource) (bool, error) {
	if existingCollection, err := getCollection(ctx, client, resource.Workspace, resource.Collection); err != nil {
		return false, err
	} else if existingCollection != nil {
		return false, nil
	} else {
		// This collection does not exist within Rockset yet, so we should create it.
		var err = createCollection(ctx, client, resource)
		return err == nil, err
	}
}

func getCollection(ctx context.Context, client *rockset.RockClient, workspace string, collection string) (*rtypes.Collection, error) {
	res, err := client.GetCollection(ctx, workspace, collection)
	if se, ok := err.(rockset.Error); ok && se.IsNotFoundError() {
		// Everything worked, but this collection does not exist.
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch collection `%s`: %w", collection, err)
	} else {
		return &res, nil
	}
}

func createCollection(ctx context.Context, client *rockset.RockClient, resource *resource) error {
	var opts []option.CollectionOption

	if advanced := resource.AdvancedCollectionSettings; advanced != nil {
		if advanced.RetentionSecs != nil {
			opts = append(opts, option.WithCollectionRetentionSeconds(*advanced.RetentionSecs))
		}

		for _, field := range advanced.ClusteringKey {
			opts = append(opts, field.toOpt())
		}
	}

	log.WithFields(
		log.Fields{
			"workspace":  resource.Workspace,
			"collection": resource.Collection,
		}).Info("Will create a new Rockset collection")
	_, err := client.CreateCollection(ctx, resource.Workspace, resource.Collection, opts...)
	if err != nil {
		return fmt.Errorf("failed to create collection `%s`: %w", resource.Collection, err)
	}
	return nil
}
