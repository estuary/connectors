package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestParseDatabasePath(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantProjectID string
		wantDBID      string
		wantErr       bool
	}{
		{
			name:          "valid default database",
			input:         "projects/my-project/databases/(default)",
			wantProjectID: "my-project",
			wantDBID:      "(default)",
			wantErr:       false,
		},
		{
			name:          "valid named database",
			input:         "projects/my-project/databases/my-named-db",
			wantProjectID: "my-project",
			wantDBID:      "my-named-db",
			wantErr:       false,
		},
		{
			name:          "valid with complex project ID",
			input:         "projects/my-project-123456/databases/production-db",
			wantProjectID: "my-project-123456",
			wantDBID:      "production-db",
			wantErr:       false,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing databases segment",
			input:   "projects/my-project",
			wantErr: true,
		},
		{
			name:    "wrong order",
			input:   "databases/foo/projects/bar",
			wantErr: true,
		},
		{
			name:    "too many segments",
			input:   "projects/my-project/databases/db/extra",
			wantErr: true,
		},
		{
			name:    "too few segments",
			input:   "projects/my-project/databases",
			wantErr: true,
		},
		{
			name:    "wrong prefix",
			input:   "project/my-project/databases/db",
			wantErr: true,
		},
		{
			name:    "wrong middle segment",
			input:   "projects/my-project/database/db",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectID, databaseID, err := parseDatabasePath(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid database path")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantProjectID, projectID)
				require.Equal(t, tt.wantDBID, databaseID)
			}
		})
	}
}

func TestResolveDatabasePath(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit path is used directly", func(t *testing.T) {
		configPath := "projects/my-project/databases/my-db"
		databasePath, projectID, databaseID, err := resolveDatabasePath(ctx, configPath, nil)
		require.NoError(t, err)
		require.Equal(t, configPath, databasePath)
		require.Equal(t, "my-project", projectID)
		require.Equal(t, "my-db", databaseID)
	})

	t.Run("explicit default database path", func(t *testing.T) {
		configPath := "projects/another-project/databases/(default)"
		databasePath, projectID, databaseID, err := resolveDatabasePath(ctx, configPath, nil)
		require.NoError(t, err)
		require.Equal(t, configPath, databasePath)
		require.Equal(t, "another-project", projectID)
		require.Equal(t, "(default)", databaseID)
	})

	t.Run("invalid explicit path returns parse error", func(t *testing.T) {
		configPath := "invalid/path"
		_, _, _, err := resolveDatabasePath(ctx, configPath, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid database path")
	})

	t.Run("empty path with invalid credentials returns error", func(t *testing.T) {
		invalidCreds := option.WithCredentialsJSON([]byte("not valid json"))
		_, _, _, err := resolveDatabasePath(ctx, "", invalidCreds)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to get credentials")
	})

	t.Run("empty path auto-detects from credentials", func(t *testing.T) {
		dummyCreds := []byte(`{
			"type": "service_account",
			"project_id": "auto-detected-project"
		}`)
		databasePath, projectID, databaseID, err := resolveDatabasePath(ctx, "", option.WithCredentialsJSON(dummyCreds))
		require.NoError(t, err)
		require.Equal(t, "projects/auto-detected-project/databases/(default)", databasePath)
		require.Equal(t, "auto-detected-project", projectID)
		require.Equal(t, "(default)", databaseID)
	})
}
