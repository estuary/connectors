package materialize_rockset

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestRocksetConfig(t *testing.T) {
	var invalid = config{}
	require.NotNil(t, invalid.Validate())

	var valid = config{ApiKey: fetchApiKey()}
	require.Nil(t, valid.Validate())
}

func TestRocksetResource(t *testing.T) {
	var invalid = resource{}
	require.Error(t, invalid.Validate())

	var too_ascii = resource{Workspace: "must only include letters and numbers", Collection: "widgets"}
	require.Error(t, too_ascii.Validate())

	var valid = resource{Workspace: "testing", Collection: "widgets"}
	require.Nil(t, valid.Validate())
}

func TestRocksetDriverSpec(t *testing.T) {
	var driver = new(rocksetDriver)
	// This isn't the right Endpoint type, but this is just a test.
	var specReq = pm.SpecRequest{EndpointType: pf.EndpointType_S3}
	var response, err = driver.Spec(context.Background(), &specReq)

	require.NoError(t, err)
	require.NotNil(t, response.EndpointSpecSchemaJson)
	require.NotNil(t, response.ResourceSpecSchemaJson)
	require.NotNil(t, response.DocumentationUrl)
}

func TestRocksetDriverValidate(t *testing.T) {
	driver := new(rocksetDriver)
	config := config{ApiKey: fetchApiKey()}
	var endpointSpecJson []byte

	endpointSpecJson, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
		return
	}

	resource := resource{Workspace: "testing", Collection: "widgets"}
	resourceSpecJson, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("failed to marshal resource: %v", err)
		return
	}

	projections := []pf.Projection{
		{
			Ptr:            "/id",
			Field:          "id",
			UserProvided:   true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{Types: []string{"string"}},
		},
		{
			Ptr:            "/foo",
			Field:          "foo",
			UserProvided:   true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{Types: []string{"object"}},
		},
	}
	collection := pf.CollectionSpec{
		Collection:  "widgets",
		SchemaUri:   "file:///schema.local",
		KeyPtrs:     []string{"/id"},
		Projections: projections,
	}
	fieldConfigJson := make(map[string]json.RawMessage)

	bindings := []*pm.ValidateRequest_Binding{
		{
			ResourceSpecJson: resourceSpecJson,
			Collection:       collection,
			FieldConfigJson:  fieldConfigJson,
		},
	}

	var validateReq = pm.ValidateRequest{
		Materialization:  "just-a-test",
		EndpointType:     pf.EndpointType_S3,
		EndpointSpecJson: endpointSpecJson,
		Bindings:         bindings,
	}
	response, err := driver.Validate(context.Background(), &validateReq)

	require.NoError(t, err)
	require.NotNil(t, response.Bindings)
	require.Len(t, response.Bindings, 1)

	var binding = response.Bindings[0]
	require.Len(t, binding.Constraints, 2)
	require.Equal(t, binding.ResourcePath, []string{"testing", "widgets"})
	require.True(t, binding.DeltaUpdates)
}

func TestRocksetDriverApply(t *testing.T) {
	workspaceName := randWorkspace()
	collectionName := randCollection()

	driver := new(rocksetDriver)
	config := config{ApiKey: fetchApiKey()}

	var endpointSpecJson []byte
	endpointSpecJson, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
		return
	}

	resource := resource{Workspace: workspaceName, Collection: collectionName}
	resourceSpecJson, err := json.Marshal(resource)
	if err != nil {
		t.Fatalf("failed to marshal resource: %v", err)
		return
	}

	projections := []pf.Projection{
		{
			Ptr:            "/id",
			Field:          "id",
			UserProvided:   true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{},
		},
	}
	collection := pf.CollectionSpec{
		Collection:  pf.Collection(collectionName),
		SchemaUri:   "file:///schema.local",
		KeyPtrs:     []string{"/id"},
		Projections: projections,
	}

	bindings := []*pf.MaterializationSpec_Binding{
		{
			ResourceSpecJson: resourceSpecJson,
			ResourcePath:     []string{workspaceName, collectionName},
			Collection:       collection,
			FieldSelection:   pf.FieldSelection{},
			DeltaUpdates:     true,
			Shuffle:          pf.Shuffle{},
		},
	}

	var applyReq = pm.ApplyRequest{
		Materialization: &pf.MaterializationSpec{
			Materialization:  pf.Materialization(collectionName),
			EndpointType:     pf.EndpointType_S3,
			EndpointSpecJson: endpointSpecJson,
			Bindings:         bindings,
		},
		Version: "1",
		DryRun:  false,
	}

	defer cleanup(config, workspaceName, collectionName)

	response, err := driver.Apply(context.Background(), &applyReq)
	log.Printf("Applied: %s", response.ActionDescription)

	require.NoError(t, err)
	require.Contains(t, response.ActionDescription, fmt.Sprintf("created %s collection", collectionName))
}

func cleanup(config config, workspaceName string, collectionName string) {
	ctx := context.Background()
	client, err := NewClient(config.ApiKey, false)
	if err != nil {
		log.Fatalf("failed to create client to cleanup resources: %s/%s", workspaceName, collectionName)
	}
	{
		ctx, cancel := context.WithTimeout(ctx, time.Second*30)
		defer cancel()

		if err := client.DestroyCollection(ctx, workspaceName, collectionName); err != nil {
			log.Fatalf("failed to cleanup collection: %s/%s", workspaceName, collectionName)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	for {
		if err := client.DestroyWorkspace(ctx, workspaceName); err == nil {
			return
		}

		select {
		case <-ctx.Done():
			log.Fatalf("failed to cleanup workspace: %s", workspaceName)
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func randWorkspace() string {
	return fmt.Sprintf("workspace-%s", RandString(10))
}

func randCollection() string {
	return fmt.Sprintf("collection-%s", RandString(10))
}

func fetchApiKey() string {
	return os.Getenv("ROCKSET_API_KEY")
}
