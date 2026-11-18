package connector

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	if _, err := d.client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: ns}, nil); err != nil && !isStatus(err, http.StatusConflict) {
		return "", fmt.Errorf("creating database %q: %w", ns, err)
	}
	return fmt.Sprintf("create database %q", ns), nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	pk, err := res.Config.partitionKey(res.Collection.Key)
	if err != nil {
		return "", nil, err
	}
	path, err := cosmosPartitionPath(pk)
	if err != nil {
		return "", nil, err
	}
	db, err := d.client.NewDatabase(res.ResourcePath[0])
	if err != nil {
		return "", nil, err
	}
	name := res.ResourcePath[1]
	return fmt.Sprintf("create container %q with partition key %q", name, path), func(ctx context.Context) error {
		_, err := db.CreateContainer(ctx, azcosmos.ContainerProperties{
			ID: name,
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths:   []string{path},
				Kind:    azcosmos.PartitionKeyKindHash,
				Version: 2,
			},
		}, nil)
		if isStatus(err, http.StatusConflict) {
			container, err := db.NewContainer(name)
			if err != nil {
				return err
			}
			existing, err := container.Read(ctx, nil)
			if err != nil {
				return fmt.Errorf("reading existing container %q: %w", name, err)
			}
			return checkPartitionKey(name, pk, existing.ContainerProperties.PartitionKeyDefinition)
		} else if err != nil {
			return fmt.Errorf("creating container %q: %w", name, err)
		}
		return nil
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	container, err := d.client.NewContainer(resourcePath[0], resourcePath[1])
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("delete container %q", resourcePath[1]), func(ctx context.Context) error {
		if _, err := container.Delete(ctx, nil); err != nil && !isStatus(err, http.StatusNotFound) {
			return fmt.Errorf("deleting container %q: %w", resourcePath[1], err)
		}
		return nil
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported")
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) UpdateResource(ctx context.Context, resourcePath []string, existing boilerplate.ExistingResource, update boilerplate.BindingUpdate[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	pk, err := update.Binding.Config.partitionKey(update.Binding.Collection.Key)
	if err != nil {
		return "", nil, err
	}
	return "", nil, checkPartitionKey(resourcePath[1], pk, existing.Meta.(containerMeta).partitionKey)
}
