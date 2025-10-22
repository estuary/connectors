package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	"github.com/microsoftgraph/msgraph-sdk-go/drives"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
)

type config struct {
	Path        string         `json:"path"`
	Credentials credentials    `json:"credentials"`
	DriveID     string         `json:"drive_id"`
	Parser      *parser.Config `json:"parser"`
	MatchKeys   string         `json:"matchKeys,omitempty"`
	Advanced    advancedConfig `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

func (c config) Validate() error {
	if err := c.Credentials.validate(); err != nil {
		return err
	}

	if c.Path == "" {
		return fmt.Errorf("missing 'Path'")
	}

	if !strings.HasPrefix(c.Path, "/") {
		return fmt.Errorf(
			"invalid configured path '%s': path must be absolute (must start with a '/')",
			c.Path)
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
	authProvider, err := cfg.
		Credentials.getAuthProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating auth provider: %w", err)
	}

	adapter, err := msgraphsdk.
		NewGraphRequestAdapter(authProvider)
	if err != nil {
		return nil, fmt.Errorf("error creating Graph adapter: %w", err)
	}

	client := msgraphsdk.NewGraphServiceClient(adapter)

	driveID := cfg.DriveID
	if driveID == "" {
		drive, err := client.Me().Drive().Get(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching user's default drive: %w", err)
		}
		if drive.GetId() == nil {
			return nil, fmt.Errorf("user's default drive has no ID")
		}
		driveID = *drive.GetId()
	}

	store := oneDriveStore{
		client:  client,
		config:  cfg,
		driveID: driveID,
	}

	if err := store.check(); err != nil {
		return nil, fmt.Errorf("error checking for access: %w", err)
	}

	return &store, nil
}

type oneDriveStore struct {
	client  *msgraphsdk.GraphServiceClient
	config  config
	driveID string
}

type folderEntry struct {
	path  string
	items []models.DriveItemable
}

func (odStore *oneDriveStore) check() error {
	_, err := odStore.
		client.
		Drives().
		ByDriveId(odStore.driveID).
		Items().
		ByDriveItemId("root:"+odStore.config.Path+":").
		Children().
		Get(context.Background(), nil)
	if err != nil {
		return err
	}
	return nil
}

func (odStore *oneDriveStore) listFolderDirectChildren(
	ctx context.Context, folderPath string,
) ([]models.DriveItemable, error) {
	requestConfig := &drives.
		ItemItemsItemChildrenRequestBuilderGetRequestConfiguration{
		QueryParameters: &drives.ItemItemsItemChildrenRequestBuilderGetQueryParameters{
			Orderby: []string{"name"},
		},
	}

	items, err := odStore.
		client.
		Drives().
		ByDriveId(odStore.driveID).
		Items().
		ByDriveItemId("root:"+folderPath+":").
		Children().
		Get(ctx, requestConfig)
	if err != nil {
		return nil, fmt.Errorf("listing folder %s: %w", folderPath, err)
	}

	pageIterator, err := msgraphcore.
		NewPageIterator[models.DriveItemable](
		items,
		odStore.client.GetAdapter(),
		models.CreateDriveItemCollectionResponseFromDiscriminatorValue,
	)
	if err != nil {
		return nil, err
	}

	var allItems []models.DriveItemable

	err = pageIterator.Iterate(ctx, func(item models.DriveItemable) bool {
		allItems = append(allItems, item)
		return true
	})
	if err != nil {
		return nil, err
	}

	return allItems, nil
}

func (odStore *oneDriveStore) List(
	ctx context.Context, query filesource.Query,
) (filesource.Listing, error) {
	rootItems, err := odStore.
		listFolderDirectChildren(ctx, query.Prefix)
	if err != nil {
		return nil, fmt.Errorf("listing root folder: %w", err)
	}

	listing := &oneDriveListing{
		store:     odStore,
		ctx:       ctx,
		stack:     []folderEntry{{path: query.Prefix, items: rootItems}},
		recursive: query.Recursive,
		startAt:   query.StartAt,
	}

	return listing, nil
}

func (odStore *oneDriveStore) Read(
	ctx context.Context, obj filesource.ObjectInfo,
) (io.ReadCloser, filesource.ObjectInfo, error) {
	content, err := odStore.
		client.
		Drives().
		ByDriveId(odStore.driveID).
		Items().
		ByDriveItemId("root:"+obj.Path+":").
		Content().
		Get(ctx, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	return io.NopCloser(bytes.NewReader(content)), obj, nil
}

type oneDriveListing struct {
	store     *oneDriveStore
	ctx       context.Context
	stack     []folderEntry
	recursive bool
	startAt   string
}

func (listing *oneDriveListing) Next() (filesource.ObjectInfo, error) {
	for {
		stackSize := len(listing.stack)
		if stackSize == 0 {
			return filesource.ObjectInfo{}, io.EOF
		}

		folder := &listing.stack[stackSize-1]
		if len(folder.items) == 0 {
			listing.stack = listing.stack[:stackSize-1] // Pop up stack to parent folder.
			continue
		}

		firstUnprocessedItem := folder.items[0]
		folder.items = folder.items[1:]

		var fullItemPath string
		if name := firstUnprocessedItem.GetName(); name != nil {
			if folder.path == "" {
				fullItemPath = "/" + *name
			} else {
				fullItemPath = folder.path + "/" + *name
			}
		}

		if listing.startAt > fullItemPath &&
			!strings.HasPrefix(listing.startAt, fullItemPath) {
			continue
		}

		var modTime time.Time
		if lastMod := firstUnprocessedItem.GetLastModifiedDateTime(); lastMod != nil {
			modTime = *lastMod
		}

		if firstUnprocessedItem.GetFolder() != nil {
			if !listing.recursive {
				return filesource.ObjectInfo{
					Path:     fullItemPath,
					IsPrefix: true,
					ModTime:  modTime,
				}, nil
			}

			childItems, err := listing.
				store.
				listFolderDirectChildren(listing.ctx, fullItemPath)
			if err != nil {
				return filesource.ObjectInfo{},
					fmt.Errorf("listing folder %s: %v", fullItemPath, err)
			}
			listing.stack = append(listing.stack, folderEntry{path: fullItemPath, items: childItems})
			continue
		}

		var size int64
		if itemSize := firstUnprocessedItem.GetSize(); itemSize != nil {
			size = *itemSize
		}

		var contentSum string
		if file := firstUnprocessedItem.GetFile(); file != nil {
			if hashes := file.GetHashes(); hashes != nil {
				if quickXor := hashes.GetQuickXorHash(); quickXor != nil {
					contentSum = *quickXor
				} else if sha1 := hashes.GetSha1Hash(); sha1 != nil {
					contentSum = *sha1
				}
			}
		}

		return filesource.ObjectInfo{
			Path:       fullItemPath,
			IsPrefix:   false,
			ContentSum: contentSum,
			Size:       size,
			ModTime:    modTime,
		}, nil
	}
}

func configSchema(parserSchema json.RawMessage) json.RawMessage {
	return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Onedrive Source",
		"type": "object",
		"required": [
			"path",
			"credentials"
		],
		"properties": {
			"credentials": {
				"title": "Credentials",
				"description": "OAuth2 credentials for OneDrive.",
				"type": "object",
				"properties": {
					"client_id": {
						"title": "Client ID",
						"description": "The client ID for the OneDrive account.",
						"type": "string",
						"secret": true
					},
					"client_secret": {
						"title": "Client Secret",
						"description": "The client secret for the OneDrive account.",
						"type": "string",
						"secret": true
					},
					"refresh_token": {
						"title": "Refresh Token",
						"description": "The OAuth2 refresh token for the OneDrive account.",
						"type": "string",
						"secret": true
					}
				},
				"required": ["client_id", "client_secret", "refresh_token"],
				"x-oauth2-provider": "microsoft",
				"order": 0
			},
			"path": {
				"type": "string",
				"title": "Path",
				"format": "string",
				"pattern": "^/.*",
				"description": "The path to the OneDrive folder to read from. For example, \"/my-folder\".",
				"order": 1
			},
			"matchKeys": {
				"type": "string",
				"title": "Match Keys",
				"format": "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order": 2
			},
			"drive_id": {
				"type": "string",
				"title": "Drive ID",
				"format": "string",
				"description": "The ID of the OneDrive drive to access. If not provided, defaults to the authenticated user's personal OneDrive.",
				"order": 3
			},
			"advanced": {
				"title": "Advanced",
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering.",
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
	src := filesource.Source{
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
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
