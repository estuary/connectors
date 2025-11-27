package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	parserSpec, err := os.ReadFile("tests/parser_spec.json")
	require.NoError(t, err)

	formatted, err := json.
		MarshalIndent(
			configSchema(json.RawMessage(parserSpec)), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestParseSharePointURL(t *testing.T) {
	tests := []struct {
		name            string
		url             string
		wantHostname    string
		wantSitePath    string
		wantLibraryName string
		wantFolderPath  string
		wantErr         bool
		errContains     string
	}{
		{
			name:            "valid URL with site only",
			url:             "https://contoso.sharepoint.com/sites/Marketing",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "",
			wantFolderPath:  "",
			wantErr:         false,
		},
		{
			name:            "valid URL with site and library",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Documents",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Documents",
			wantFolderPath:  "",
			wantErr:         false,
		},
		{
			name:            "valid URL with URL-encoded library name",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Shared%20Documents",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Shared Documents",
			wantFolderPath:  "",
			wantErr:         false,
		},
		{
			name:            "valid URL with folder path and filename (strips filename)",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Documents/folder/file.docx",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Documents",
			wantFolderPath:  "/folder",
			wantErr:         false,
		},
		{
			name:            "valid URL with URL-encoded folder path",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Documents/my%20folder/sub%20folder",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Documents",
			wantFolderPath:  "/my folder/sub folder",
			wantErr:         false,
		},
		{
			name:            "valid URL with filename at root level (strips filename)",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Documents/file.xlsx",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Documents",
			wantFolderPath:  "/",
			wantErr:         false,
		},
		{
			name:        "invalid URL - malformed",
			url:         "://invalid-url",
			wantErr:     true,
			errContains: "invalid URL",
		},
		{
			name:        "invalid URL - missing hostname",
			url:         "/sites/Marketing",
			wantErr:     true,
			errContains: "must include a hostname",
		},
		{
			name:        "invalid URL - missing path",
			url:         "https://contoso.sharepoint.com",
			wantErr:     true,
			errContains: "must include at least /sites/{sitename}",
		},
		{
			name:        "invalid URL - incomplete path (only /sites)",
			url:         "https://contoso.sharepoint.com/sites",
			wantErr:     true,
			errContains: "must include at least /sites/{sitename}",
		},
		{
			name:            "valid URL with teams prefix",
			url:             "https://contoso.sharepoint.com/teams/Marketing",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/teams/Marketing",
			wantLibraryName: "",
			wantFolderPath:  "",
			wantErr:         false,
		},
		{
			name:        "invalid URL - wrong path prefix",
			url:         "https://contoso.sharepoint.com/groups/Marketing",
			wantErr:     true,
			errContains: "must start with /sites/ or /teams/",
		},
		{
			name:            "valid URL - trailing slash",
			url:             "https://contoso.sharepoint.com/sites/Marketing/",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "",
			wantFolderPath:  "",
			wantErr:         false,
		},
		{
			name:            "valid URL with trailing slash on folder",
			url:             "https://contoso.sharepoint.com/sites/Marketing/Documents/my-folder/",
			wantHostname:    "contoso.sharepoint.com",
			wantSitePath:    "/sites/Marketing",
			wantLibraryName: "Documents",
			wantFolderPath:  "/my-folder",
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostname, sitePath, libraryName, folderPath, err := parseSharePointURL(tt.url)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantHostname, hostname)
				require.Equal(t, tt.wantSitePath, sitePath)
				require.Equal(t, tt.wantLibraryName, libraryName)
				require.Equal(t, tt.wantFolderPath, folderPath)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	validCreds := credentials{
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		RefreshToken: "test-refresh-token",
	}

	tests := []struct {
		name        string
		cfg         config
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config with URL method",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "url",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing/Documents",
				},
				Credentials: validCreds,
			},
			wantErr: false,
		},
		{
			name: "valid config with components method - site_id only",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method: "components",
					SiteID: "site-123",
				},
				Credentials: validCreds,
			},
			wantErr: false,
		},
		{
			name: "valid config with components method - all fields",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "components",
					SiteID:  "site-123",
					DriveID: "drive-456",
					Path:    "/my-folder",
				},
				Credentials: validCreds,
			},
			wantErr: false,
		},
		{
			name: "invalid - URL method with site_id",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "url",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
					SiteID:  "site-123",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site_id cannot be provided when using URL configuration method",
		},
		{
			name: "invalid - URL method with drive_id",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "url",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
					DriveID: "drive-456",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "drive_id cannot be provided when using URL configuration method",
		},
		{
			name: "invalid - URL method with path",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "url",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
					Path:    "/folder",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "path cannot be provided when using URL configuration method",
		},
		{
			name: "invalid - components method with site_url",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "components",
					SiteID:  "site-123",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site_url cannot be provided when using component IDs configuration method",
		},
		{
			name: "invalid - components method missing site_id",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method: "components",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site_id is required when using component IDs configuration method",
		},
		{
			name: "invalid - URL method missing site_url",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method: "url",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site_url is required when using URL configuration method",
		},
		{
			name: "invalid - missing method",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site configuration method must be specified",
		},
		{
			name: "invalid - invalid method value",
			cfg: config{
				SiteConfiguration: &siteConfiguration{
					Method:  "invalid-method",
					SiteURL: "https://contoso.sharepoint.com/sites/Marketing",
				},
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "invalid site configuration method",
		},
		{
			name: "invalid - missing site_configuration",
			cfg: config{
				Credentials: validCreds,
			},
			wantErr:     true,
			errContains: "site_configuration is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
