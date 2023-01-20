package materialize_rockset

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestRocksetConfig(t *testing.T) {
	var invalid = config{}
	require.NotNil(t, invalid.Validate())

	var valid = config{ApiKey: "some_key", RegionBaseUrl: "www.something.com"}
	require.Nil(t, valid.Validate())
}

func TestRocksetResource(t *testing.T) {
	var invalid = resource{}
	require.Error(t, invalid.Validate())

	var too_ascii = resource{Workspace: "must only include letters and numbers", Collection: "widgets"}
	require.Error(t, too_ascii.Validate())

	var valid = resource{Workspace: "testing-33", Collection: "widgets_1"}
	require.Nil(t, valid.Validate())
}

func TestRocksetDriverSpec(t *testing.T) {
	var driver = new(rocksetDriver)
	var specReq = pm.SpecRequest{}
	var response, err = driver.Spec(context.Background(), &specReq)
	require.NoError(t, err)

	t.Run("TestEndpointSpecSchema", func(t *testing.T) {
		formatted, err := json.MarshalIndent(response.EndpointSpecSchemaJson, "", "  ")
		require.NoError(t, err)
		cupaloy.SnapshotT(t, string(formatted))
	})
	t.Run("TestResourceSpecSchema", func(t *testing.T) {
		formatted, err := json.MarshalIndent(response.ResourceSpecSchemaJson, "", "  ")
		require.NoError(t, err)
		cupaloy.SnapshotT(t, string(formatted))
	})
}

func TestRocksetDriverValidate(t *testing.T) {
	driver := new(rocksetDriver)
	config := testConfig()
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
			Explicit:       true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{Types: []string{"string"}},
		},
		{
			Ptr:            "/foo",
			Field:          "foo",
			Explicit:       true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{Types: []string{"object"}},
		},
	}
	collection := pf.CollectionSpec{
		Collection:     "widgets",
		WriteSchemaUri: "file:///schema.local",
		KeyPtrs:        []string{"/id"},
		Projections:    projections,
		PartitionTemplate: &pf.JournalSpec{
			Name:        "widgets",
			Replication: 1,
			LabelSet:    pf.LabelSet{},
			Fragment: pb.JournalSpec_Fragment{
				Length:           2048,
				CompressionCodec: pb.CompressionCodec_GZIP,
				RefreshInterval:  time.Hour * 2,
			}},
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
	config := testConfig()

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
			Explicit:       true,
			IsPartitionKey: true,
			IsPrimaryKey:   true,
			Inference:      pf.Inference{},
		},
	}
	collection := pf.CollectionSpec{
		Collection:     pf.Collection(collectionName),
		WriteSchemaUri: "file:///schema.local",
		KeyPtrs:        []string{"/id"},
		Projections:    projections,
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
			EndpointSpecJson: endpointSpecJson,
			Bindings:         bindings,
		},
		Version: "1",
		DryRun:  false,
	}

	defer cleanup(config, workspaceName, collectionName)

	response, err := driver.ApplyUpsert(context.Background(), &applyReq)

	require.NoError(t, err)
	require.Contains(t, response.ActionDescription, fmt.Sprintf("created %s collection", collectionName))
	log.Printf("Applied: %s", response.ActionDescription)
}

func cleanup(config config, workspaceName string, collectionName string) {
	ctx := context.Background()
	client, err := config.client()
	if err != nil {
		log.Fatalf("initializing client: %s", err.Error())
	}

	if err := client.DeleteCollection(ctx, workspaceName, collectionName); err != nil {
		log.Fatalf("failed to cleanup collection: %s/%s: %s", workspaceName, collectionName, err.Error())
	}

	client.WaitUntilCollectionGone(ctx, workspaceName, collectionName)

	if err := client.DeleteWorkspace(ctx, workspaceName); err != nil {
		log.Fatalf("failed to cleanup workspace: %s/%s: %s", workspaceName, collectionName, err.Error())
	}
}

func testConfig() config {
	return config{
		ApiKey:        fetchApiKey(),
		RegionBaseUrl: "api.usw2a1.rockset.com",
	}
}

func randWorkspace() string {
	return fmt.Sprintf("automated-tests-%s", randString(6))
}

func randCollection() string {
	return fmt.Sprintf("c-%s", randString(6))
}

func fetchApiKey() string {
	return os.Getenv("ROCKSET_API_KEY")
}

func randString(len int) string {
	var buffer = make([]byte, len)
	if _, err := rand.Read(buffer); err != nil {
		panic("failed to generate random string")
	}
	return hex.EncodeToString(buffer)
}
