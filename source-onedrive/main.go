package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)

type config struct {
	Path        string         `json:"path"`
	Credentials credentials    `json:"credentials"`
	Parser      *parser.Config `json:"parser"`
	MatchKeys   string         `json:"matchKeys,omitempty"`
	Advanced    advancedConfig `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

func (c config) Validate() error {
	requiredProperties := [][]string{
		{"Path", c.Path},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if !strings.HasPrefix(c.Path, "/") {
		return fmt.Errorf("invalid configured path '%s': path must be absolute (must start with a '/')", c.Path)
	}

	err := c.Credentials.validate()
	if err != nil {
		return err
	}

	return nil
}

func (c config) DiscoverRoot() string {
	return c.Path
}

func (c config) RecommendedName() string {
	return strings.Trim(c.DiscoverRoot(), "/")
}

func (c config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return c.MatchKeys
}

func newOneDriveStore(ctx context.Context, cfg config) (*oneDriveStore, error) {
	// Create authentication provider using Azure Identity pattern with our OAuth2 token
	authProvider, err := cfg.Credentials.getAuthProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating auth provider: %w", err)
	}
	
	// Create request adapter using the auth provider
	adapter, err := msgraphsdk.NewGraphRequestAdapter(authProvider)
	if err != nil {
		return nil, fmt.Errorf("creating graph adapter: %w", err)
	}

	client := msgraphsdk.NewGraphServiceClient(adapter)
	
	store := oneDriveStore{
		client: client,
		config: cfg,
	}

	if err := store.check(ctx); err != nil {
		return nil, err
	}

	return &store, nil
}

type oneDriveStore struct {
	client *msgraphsdk.GraphServiceClient
	config config
}

// check verifies that the specified path exists in OneDrive
func (odStore *oneDriveStore) check(ctx context.Context) error {
	// First get the user's drive to obtain the drive ID
	drive, err := odStore.client.Me().Drive().Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("getting user drive: %w", err)
	}
	
	if drive.GetId() == nil {
		return fmt.Errorf("drive ID is nil")
	}
	
	driveId := *drive.GetId()
	path := strings.Trim(odStore.config.Path, "/")
	
	if path == "" {
		// Check root
		_, err := odStore.client.Drives().ByDriveId(driveId).Root().Get(ctx, nil)
		if err != nil {
			return fmt.Errorf("checking root path: %w", err)
		}
	} else {
		// Check specific path using ItemWithPath
		_, err := odStore.client.Drives().ByDriveId(driveId).Items().ByDriveItemId("root:"+path).Get(ctx, nil)
		if err != nil {
			return fmt.Errorf("checking path %s: %w", odStore.config.Path, err)
		}
	}

	return nil
}


func (odStore *oneDriveStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	path := strings.Trim(query.Prefix, "/")

	var allItems []models.DriveItemable
	
	if query.Recursive {
		// For recursive queries, we need to traverse all subdirectories
		err := odStore.collectItemsRecursive(ctx, path, &allItems)
		if err != nil {
			return nil, fmt.Errorf("collecting items recursively: %w", err)
		}
	} else {
		// For non-recursive queries, just get direct children
		items, err := odStore.fetchItems(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("listing items: %w", err)
		}
		allItems = items
	}

	// Convert to filesource.ObjectInfo and filter
	var objects []filesource.ObjectInfo
	for _, item := range allItems {
		obj := odStore.driveItemToObjectInfo(item, query.Prefix)
		
		// Apply StartAt filter
		if query.StartAt != "" && obj.Path < query.StartAt {
			continue
		}

		objects = append(objects, obj)
	}

	// Sort by path
	slices.SortStableFunc(objects, func(a, b filesource.ObjectInfo) int {
		return cmp.Compare(a.Path, b.Path)
	})

	return &oneDriveListing{
		objects: objects,
		index:   0,
	}, nil
}

func (odStore *oneDriveStore) fetchItems(ctx context.Context, path string) ([]models.DriveItemable, error) {
	// First get the user's drive to obtain the drive ID
	drive, err := odStore.client.Me().Drive().Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("getting user drive: %w", err)
	}
	
	if drive.GetId() == nil {
		return nil, fmt.Errorf("drive ID is nil")
	}
	
	driveId := *drive.GetId()
	var allItems []models.DriveItemable
	
	if path == "" {
		// Get root children
		response, err := odStore.client.Drives().ByDriveId(driveId).Items().ByDriveItemId("root").Children().Get(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("getting root children: %w", err)
		}
		if response != nil && response.GetValue() != nil {
			allItems = append(allItems, response.GetValue()...)
		}
	} else {
		// Get children of specific path
		response, err := odStore.client.Drives().ByDriveId(driveId).Items().ByDriveItemId("root:"+path).Children().Get(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("getting children for path %s: %w", path, err)
		}
		if response != nil && response.GetValue() != nil {
			allItems = append(allItems, response.GetValue()...)
		}
	}

	return allItems, nil
}

func (odStore *oneDriveStore) collectItemsRecursive(ctx context.Context, basePath string, allItems *[]models.DriveItemable) error {
	items, err := odStore.fetchItems(ctx, basePath)
	if err != nil {
		return err
	}

	for _, item := range items {
		*allItems = append(*allItems, item)
		
		// If it's a folder, recurse into it
		if item.GetFolder() != nil {
			childPath := basePath
			if childPath != "" {
				childPath += "/"
			}
			if item.GetName() != nil {
				childPath += *item.GetName()
			}
			
			err := odStore.collectItemsRecursive(ctx, childPath, allItems)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (odStore *oneDriveStore) driveItemToObjectInfo(item models.DriveItemable, prefix string) filesource.ObjectInfo {
	obj := filesource.ObjectInfo{
		Path:     odStore.getItemPath(item, prefix),
		IsPrefix: item.GetFolder() != nil,
	}

	if item.GetFile() != nil {
		// It's a file
		if item.GetSize() != nil {
			obj.Size = *item.GetSize()
		}
		if item.GetLastModifiedDateTime() != nil {
			obj.ModTime = *item.GetLastModifiedDateTime()
		}
		if item.GetCTag() != nil {
			obj.ContentSum = *item.GetCTag()
		}
	}

	return obj
}

func (odStore *oneDriveStore) getItemPath(item models.DriveItemable, prefix string) string {
	if item.GetParentReference() != nil && item.GetParentReference().GetPath() != nil {
		parentPath := *item.GetParentReference().GetPath()
		// Remove the "/drive/root:" prefix if present
		parentPath = strings.TrimPrefix(parentPath, "/drive/root:")
		if parentPath == "" {
			parentPath = "/"
		}
		if !strings.HasSuffix(parentPath, "/") {
			parentPath += "/"
		}
		if item.GetName() != nil {
			return parentPath + *item.GetName()
		}
	}
	
	// Fallback: construct path relative to prefix
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	if prefix == "" {
		prefix = "/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	
	if item.GetName() != nil {
		return prefix + *item.GetName()
	}
	return prefix
}

func (odStore *oneDriveStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	// First get the user's drive to obtain the drive ID
	drive, err := odStore.client.Me().Drive().Get(ctx, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("getting user drive: %w", err)
	}
	
	if drive.GetId() == nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("drive ID is nil")
	}
	
	driveId := *drive.GetId()
	path := strings.Trim(obj.Path, "/")
	
	// Get the item metadata first
	item, err := odStore.client.Drives().ByDriveId(driveId).Items().ByDriveItemId("root:"+path).Get(ctx, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("getting item metadata: %w", err)
	}

	// Download the file content using SDK
	content, err := odStore.client.Drives().ByDriveId(driveId).Items().ByDriveItemId("root:"+path).Content().Get(ctx, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("downloading content: %w", err)
	}

	// Convert []byte to ReadCloser
	contentReader := io.NopCloser(bytes.NewReader(content))

	// Update object info with actual metadata
	updatedObj := odStore.driveItemToObjectInfo(item, "")
	updatedObj.Path = obj.Path // Keep the original path format

	return contentReader, updatedObj, nil
}

type oneDriveListing struct {
	objects []filesource.ObjectInfo
	index   int
}

func (l *oneDriveListing) Next() (filesource.ObjectInfo, error) {
	if l.index >= len(l.objects) {
		return filesource.ObjectInfo{}, io.EOF
	}

	obj := l.objects[l.index]
	l.index++
	return obj, nil
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {
	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "OneDrive Source",
		"type": "object",
		"required": [
			"path",
			"credentials"
		],
		"properties": {
			"path": {
				"type": "string",
				"title": "Path",
				"format": "string",
				"pattern": "^/.+",
				"description": "The path to the OneDrive folder to read from. For example, \"/Documents/my-folder\".",
				"order": 1
			},
			"credentials": {
				"title": "Credentials",
				"description": "OAuth2 credentials for OneDrive. Those are automatically handled by the Web UI.",
				"type": "object",
				"x-oauth2-provider": "microsoft",
				"properties": {
					"refresh_token": {
						"title": "Refresh Token",
						"description": "The refresh token for the Microsoft account.",
						"type": "string",
						"secret": true
					},
					"client_id": {
						"title": "Client ID",
						"description": "The client ID for the Microsoft application.",
						"type": "string",
						"secret": true
					},
					"client_secret": {
						"title": "Client Secret",
						"description": "The client secret for the Microsoft application.",
						"type": "string",
						"secret": true
					}
				},
				"required": ["refresh_token", "client_id", "client_secret"],
				"order": 2
			},
			"matchKeys": {
				"type": "string",
				"title": "Match Keys",
				"format": "regex",
				"description": "Filter applied to all file paths under the prefix. If provided, only files whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order": 3
			},
			"advanced": {
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire folder prefix. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering.",
					"default":     false
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true,
				"order": 4
			},
			"parser": ` + string(parserSchema) + `
		}
	}`)
}

func main() {
	var src = filesource.Source{
		NewConfig: func(raw json.RawMessage) (filesource.Config, error) {
			var cfg config
			if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf("parsing config json: %w", err)
			}
			return cfg, nil
		},
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newOneDriveStore(ctx, cfg.(config))
		},
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-onedrive",
		Oauth2:           oauth2Spec(),
		// Set the delta to 30 seconds in the past, similar to Dropbox
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}