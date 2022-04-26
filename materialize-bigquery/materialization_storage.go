package main

import (
	"context"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	pf "github.com/estuary/flow/go/protocols/flow"
	proto "github.com/gogo/protobuf/proto"
)

type MaterialiationStorage struct {
	client *storage.Client
	config *config
}

func NewMaterializationStorage(ctx context.Context, cfg *config) (*MaterialiationStorage, error) {
	client, err := cfg.StorageClient(ctx)

	if err != nil {
		return nil, err
	}

	return &MaterialiationStorage{
		client: client,
		config: cfg,
	}, nil
}

// Load the MaterializationSpec from Cloud Storage. It's possible that there is no materialization
// to be found in storage if the materialization is new. In this case, the return value is an empty map.
func (s *MaterialiationStorage) LoadBindings(ctx context.Context, name string) (map[string]*pf.MaterializationSpec_Binding, error) {
	var existing pf.MaterializationSpec

	bucket := s.client.Bucket(s.config.Bucket)
	specKey := materializationPath(s.config.BucketPath, name)
	reader, err := bucket.Object(specKey).NewReader(ctx)

	if err != nil {
		// The file not existing means this is the first validate, otherwise it's an actual error
		if err != storage.ErrObjectNotExist {
			return nil, fmt.Errorf("downloading existing spec failed: %w", err)
		}
	} else {
		buf, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("reading content of the binding remotely: %w", err)
		}

		err = proto.Unmarshal(buf, &existing)
		if err != nil {
			return nil, fmt.Errorf("parsing existing materialization spec: %w", err)
		}
	}

	bindingsByTable := make(map[string]*pf.MaterializationSpec_Binding)

	for _, binding := range existing.Bindings {
		var r bindingResource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &r); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		bindingsByTable[r.Table] = binding
	}

	return bindingsByTable, nil
}

func (s *MaterialiationStorage) Write(ctx context.Context, materialization *pf.MaterializationSpec, version string) error {
	bucket := s.client.Bucket(s.config.Bucket)
	specKey := materializationPath(s.config.BucketPath, materialization.Materialization.String())
	writer := bucket.Object(specKey).NewWriter(ctx)

	data, err := proto.Marshal(materialization)
	if err != nil {
		return fmt.Errorf("marshalling materialization spec: %w", err)
	}

	writer.Metadata = map[string]string{
		"version": version,
	}

	if _, err = writer.Write(data); err != nil {
		return fmt.Errorf("uploading materialization spec %s: %w", specKey, err)
	}

	if err = writer.Close(); err != nil {
		return fmt.Errorf("uploading materialization spec %s: %w", specKey, err)
	}

	return nil
}

func (s *MaterialiationStorage) Delete(ctx context.Context, materialization *pf.MaterializationSpec) error {
	bucket := s.client.Bucket(s.config.Bucket)
	specKey := materializationPath(s.config.BucketPath, materialization.Materialization.String())
	err := bucket.Object(specKey).Delete(ctx)

	if err != nil {
		return fmt.Errorf("deleting materialization spec %s: %w", specKey, err)
	}

	return nil
}

func materializationPath(bucketPath, name string) string {
	return fmt.Sprintf("%s/%s.flow.materialization_spec", bucketPath, name)
}
