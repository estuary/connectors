package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Match the values in testdata/config.local.yaml. The Spanner emulator does
// not require these to exist in GCP, but the instance and database do have to
// be created via the admin APIs before the materialization can use them.
const (
	testProjectID  = "flow-test"
	testInstanceID = "flow-test"
	testDatabase   = "flow"
	emulatorAddr   = "localhost:9010"
)

func TestMain(m *testing.M) {
	if isShortMode() {
		os.Exit(m.Run())
	}

	if err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "-d").Run(); err != nil {
		log.Fatalf("starting docker compose: %v", err)
	}
	teardown := func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	}

	// This env var is used by the Spanner client library.
	// https://docs.cloud.google.com/spanner/docs/emulator#client-libraries
	// https://pkg.go.dev/cloud.google.com/go/spanner#hdr-Creating_a_Client
	os.Setenv("SPANNER_EMULATOR_HOST", emulatorAddr)

	if err := bootstrapEmulator(); err != nil {
		teardown()
		log.Fatalf("bootstrapping spanner emulator: %v", err)
	}

	code := m.Run()
	teardown()
	os.Exit(code)
}

// bootstrapEmulator waits for the emulator to be reachable and creates the
// instance and database used by the integration tests. It is idempotent:
// AlreadyExists errors are ignored so repeated runs against a long-lived
// emulator don't fail.
func bootstrapEmulator() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var instanceAdmin *instance.InstanceAdminClient
	var err error
	for {
		instanceAdmin, err = instance.NewInstanceAdminClient(ctx)
		if err == nil {
			// A NotFound response means the emulator is responding and ready.
			_, err = instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{
				Name: fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID),
			})
			if status.Code(err) == codes.NotFound {
				err = nil
				break
			}
		}
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("emulator did not become ready: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + testProjectID,
		InstanceId: testInstanceID,
		Instance: &instancepb.Instance{
			Config:      "projects/" + testProjectID + "/instanceConfigs/emulator-config",
			DisplayName: testInstanceID,
			NodeCount:   1,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating instance: %w", err)
	} else if err == nil {
		if _, err := op.Wait(ctx); err != nil {
			return fmt.Errorf("waiting for instance creation: %w", err)
		}
	}

	cfg := config{ProjectID: testProjectID, InstanceID: testInstanceID, Database: testDatabase}
	clients, err := newSpannerClients(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	defer clients.Close()

	// Create the database. The admin client built above fails immediately if
	// the database is missing, so we call through adminClient directly.
	dbOp, err := clients.adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating database: %w", err)
	} else if err == nil {
		if _, err := dbOp.Wait(ctx); err != nil {
			return fmt.Errorf("waiting for database creation: %w", err)
		}
	}

	return nil
}

func isShortMode() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.short") || arg == "-short" {
			return true
		}
	}
	return false
}
