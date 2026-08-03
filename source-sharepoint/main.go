package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	pf "github.com/estuary/flow/go/protocols/flow"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	"github.com/microsoftgraph/msgraph-sdk-go/drives"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	"golang.org/x/oauth2"
)

type siteConfiguration struct {
	Method  string `json:"method"` // "url" or "components"
	SiteURL string `json:"site_url,omitempty"`
	SiteID  string `json:"site_id,omitempty"`
	DriveID string `json:"drive_id,omitempty"`
	Path    string `json:"path,omitempty"`
}

type config struct {
	Credentials       credentials        `json:"credentials"`
	SiteConfiguration *siteConfiguration `json:"site_configuration"`
	MatchKeys         string             `json:"matchKeys,omitempty"`
	Advanced          advancedConfig     `json:"advanced"`
	Parser            *parser.Config     `json:"parser"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys,omitempty"`
}

func (c config) Validate() error {
	if err := c.Credentials.validate(); err != nil {
		return err
	}

	if c.SiteConfiguration == nil {
		return fmt.Errorf("site_configuration is required")
	}

	switch c.SiteConfiguration.Method {
	case "url":
		if c.SiteConfiguration.SiteURL == "" {
			return fmt.Errorf("site_url is required when using URL configuration method")
		}
		if c.SiteConfiguration.SiteID != "" {
			return fmt.Errorf("site_id cannot be provided when using URL configuration method")
		}
		if c.SiteConfiguration.DriveID != "" {
			return fmt.Errorf("drive_id cannot be provided when using URL configuration method")
		}
		if c.SiteConfiguration.Path != "" {
			return fmt.Errorf("path cannot be provided when using URL configuration method; include the path in the URL instead")
		}

	case "components":
		if c.SiteConfiguration.SiteID == "" {
			return fmt.Errorf("site_id is required when using component IDs configuration method")
		}
		if c.SiteConfiguration.SiteURL != "" {
			return fmt.Errorf("site_url cannot be provided when using component IDs configuration method")
		}

	case "":
		return fmt.Errorf("site configuration method must be specified (either 'url' or 'components')")

	default:
		return fmt.Errorf("invalid site configuration method '%s' (must be 'url' or 'components')", c.SiteConfiguration.Method)
	}

	return nil
}

func (c config) DiscoverRoot() string {
	if c.SiteConfiguration != nil {
		return c.SiteConfiguration.Path
	}
	return ""
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

// parseSharePointURL extracts the hostname, site path, optional library name,
// and folder path from a SharePoint URL. Supports both /sites/ and /teams/ URLs.
// Example: https://contoso.sharepoint.com/sites/Marketing/Shared%20Documents/my-folder/subfolder/file.docx
// Returns: hostname="contoso.sharepoint.com", sitePath="/sites/Marketing", libraryName="Shared Documents", folderPath="/my-folder/subfolder"
func parseSharePointURL(
	sharePointURL string,
) (hostname, sitePath, libraryName, folderPath string, err error) {
	parsedURL, err := url.Parse(sharePointURL)
	if err != nil {
		return "", "", "", "", fmt.Errorf("invalid URL: %w", err)
	}

	hostname = parsedURL.Hostname()
	if hostname == "" {
		return "", "", "", "", fmt.Errorf("URL must include a hostname")
	}

	urlPath := strings.TrimPrefix(parsedURL.Path, "/")
	pathParts := strings.Split(urlPath, "/")

	if len(pathParts) < 2 {
		return "",
			"",
			"",
			"",
			fmt.Errorf("URL path must include at least /sites/{sitename} or /teams/{teamname}")
	}

	if pathParts[0] != "sites" && pathParts[0] != "teams" {
		return "",
			"",
			"",
			"",
			fmt.Errorf("URL path must start with /sites/ or /teams/ (got: /%s/)", pathParts[0])
	}
	sitePath = "/" + pathParts[0] + "/" + pathParts[1]

	if len(pathParts) >= 3 && pathParts[2] != "" {
		libraryName, err = url.PathUnescape(pathParts[2])
		if err != nil {
			return "", "", "", "", fmt.Errorf("invalid library name encoding: %w", err)
		}
	}

	// Extract the rest of the URL components as the folder path
	folderPath = ""
	if len(pathParts) > 3 {
		var folderParts []string

		for _, part := range pathParts[3:] {
			if part != "" {
				unescaped, err := url.PathUnescape(part)
				if err != nil {
					return "", "", "", "", fmt.Errorf("invalid folder path encoding: %w", err)
				}
				folderParts = append(folderParts, unescaped)
			}
		}

		if len(folderParts) > 0 {
			folderPath = "/" + strings.Join(folderParts, "/")

			// Strip filename if the last component has a file extension
			if path.Ext(folderPath) != "" {
				folderPath = path.Dir(folderPath)
			}
		}
	}

	return hostname, sitePath, libraryName, folderPath, nil
}

// getSiteIDFromPath resolves a SharePoint site ID from a hostname and site path
// Example: hostname="contoso.sharepoint.com", sitePath="/sites/Marketing" -> site ID
func getSiteIDFromPath(
	ctx context.Context, graphClient *msgraphsdk.GraphServiceClient, hostname, sitePath string,
) (string, error) {
	// The Graph API uses the format: /sites/{hostname}:{sitePath}
	// For example: /sites/contoso.sharepoint.com:/sites/Marketing
	siteIdentifier := hostname + ":" + sitePath

	site, err := graphClient.
		Sites().
		BySiteId(siteIdentifier).
		Get(ctx, nil)
	if err != nil {
		return "",
			fmt.Errorf("error resolving site ID for %s: %w", siteIdentifier, err)
	}

	if site.GetId() == nil {
		return "", fmt.Errorf("site ID is nil for site identifier '%s'", siteIdentifier)
	}

	return *site.GetId(), nil
}

func getDriveByLibraryName(
	ctx context.Context, graphClient *msgraphsdk.GraphServiceClient, siteID, libraryName string,
) (string, error) {
	drives, err := graphClient.
		Sites().
		BySiteId(siteID).
		Drives().
		Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("error listing drives for site: %w", err)
	}

	for _, drive := range drives.GetValue() {
		if drive.GetName() != nil && *drive.GetName() == libraryName {
			if drive.GetId() == nil {
				return "", fmt.Errorf("drive ID is nil for library '%s'", libraryName)
			}
			return *drive.GetId(), nil
		}
	}

	return "", fmt.Errorf("no drive found with name '%s' in site", libraryName)
}

func getDefaultSiteDrive(
	ctx context.Context, graphClient *msgraphsdk.GraphServiceClient, siteID string,
) (string, error) {
	drive, err := graphClient.
		Sites().
		BySiteId(siteID).
		Drive().
		Get(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("error fetching site's default drive: %w", err)
	}

	if drive.GetId() == nil {
		return "", fmt.Errorf("drive ID is nil for site '%s' (site may not exist or have no default drive)", siteID)
	}

	return *drive.GetId(), nil
}

func newSharePointStore(ctx context.Context, cfg config) (*sharePointStore, error) {
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

	httpClient := &http.Client{}
	graphClient := msgraphsdk.NewGraphServiceClient(adapter)

	var libraryName string

	// If using URL method, parse and resolve to components
	if cfg.SiteConfiguration.Method == "url" {
		hostname,
			sitePath,
			parsedLibraryName,
			folderPath,
			err := parseSharePointURL(cfg.SiteConfiguration.SiteURL)
		if err != nil {
			return nil, fmt.Errorf("error parsing site URL: %w", err)
		}

		cfg.SiteConfiguration.SiteID, err = getSiteIDFromPath(ctx, graphClient, hostname, sitePath)
		if err != nil {
			return nil, fmt.Errorf("error resolving site ID: %w", err)
		}

		libraryName = parsedLibraryName

		if folderPath != "" {
			cfg.SiteConfiguration.Path = folderPath
		}
	}

	// Resolve drive if not provided
	if cfg.SiteConfiguration.DriveID == "" {
		if libraryName != "" {
			cfg.SiteConfiguration.DriveID, err = getDriveByLibraryName(ctx, graphClient, cfg.SiteConfiguration.SiteID, libraryName)
			if err != nil {
				return nil, fmt.Errorf("error resolving drive by library name: %w", err)
			}
		} else {
			cfg.SiteConfiguration.DriveID, err = getDefaultSiteDrive(ctx, graphClient, cfg.SiteConfiguration.SiteID)
			if err != nil {
				return nil, fmt.Errorf("error resolving default drive: %w", err)
			}
		}
	}

	if cfg.SiteConfiguration.Path == "" {
		cfg.SiteConfiguration.Path = "/"
	}

	tokenSource := cfg.Credentials.getTokenSource(ctx)

	store := sharePointStore{
		httpClient:  httpClient,
		graphClient: graphClient,
		config:      cfg,
		tokenSource: tokenSource,
	}

	if err := store.check(ctx); err != nil {
		return nil, fmt.Errorf("error checking for access: %w", err)
	}

	return &store, nil
}

type sharePointStore struct {
	httpClient  *http.Client
	graphClient *msgraphsdk.GraphServiceClient
	config      config
	tokenSource oauth2.TokenSource
}

type folderEntry struct {
	path  string
	items []models.DriveItemable
}

func (spStore *sharePointStore) check(ctx context.Context) error {
	_, err := spStore.
		graphClient.
		Drives().
		ByDriveId(spStore.config.SiteConfiguration.DriveID).
		Items().
		ByDriveItemId("root:"+spStore.config.SiteConfiguration.Path+":").
		Children().
		Get(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (spStore *sharePointStore) listFolderDirectChildren(
	ctx context.Context, folderPath string,
) ([]models.DriveItemable, error) {
	requestConfig := &drives.
		ItemItemsItemChildrenRequestBuilderGetRequestConfiguration{
		QueryParameters: &drives.ItemItemsItemChildrenRequestBuilderGetQueryParameters{
			Orderby: []string{"name"},
		},
	}

	items, err := spStore.
		graphClient.
		Drives().
		ByDriveId(spStore.config.SiteConfiguration.DriveID).
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
		spStore.graphClient.GetAdapter(),
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

func (spStore *sharePointStore) List(
	ctx context.Context, query filesource.Query,
) (filesource.Listing, error) {
	rootItems, err := spStore.
		listFolderDirectChildren(ctx, query.Prefix)
	if err != nil {
		return nil, fmt.Errorf("listing root folder: %w", err)
	}

	listing := &sharePointListing{
		store:     spStore,
		ctx:       ctx,
		stack:     []folderEntry{{path: query.Prefix, items: rootItems}},
		recursive: query.Recursive,
		startAt:   query.StartAt,
	}

	return listing, nil
}

func (spStore *sharePointStore) Read(
	ctx context.Context, obj filesource.ObjectInfo,
) (io.ReadCloser, filesource.ObjectInfo, error) {
	token, err := spStore.tokenSource.Token()
	if err != nil {
		return nil,
			filesource.ObjectInfo{},
			fmt.Errorf("getting access token: %w", err)
	}

	reqURL, err := url.JoinPath(
		"https://graph.microsoft.com",
		"/v1.0/drives",
		spStore.config.SiteConfiguration.DriveID,
		fmt.Sprintf("root:%s:/content", obj.Path),
	)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("constructing URL: %w", err)
	}

	req, err := http.
		NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	resp, err := spStore.httpClient.Do(req)
	if err != nil {
		return nil, filesource.ObjectInfo{}, fmt.Errorf("executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, filesource.ObjectInfo{}, fmt.Errorf(
			"unexpected status %d: %s, body: %s",
			resp.StatusCode,
			resp.Status,
			string(body),
		)
	}

	return resp.Body, obj, nil
}

type sharePointListing struct {
	store     *sharePointStore
	ctx       context.Context
	stack     []folderEntry
	recursive bool
	startAt   string
}

func (listing *sharePointListing) Next() (filesource.ObjectInfo, error) {
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

		// Skip if this item is lexicographically before startAt
		// but don't skip if this path is a prefix of startAt (we need to traverse into it)
		if listing.startAt != "" &&
			listing.startAt > fullItemPath &&
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
		"title": "SharePoint Source",
		"type": "object",
		"required": [
			"credentials",
			"site_configuration"
		],
		"properties": {
			"credentials": {
				"title": "Credentials",
				"description": "OAuth2 credentials for SharePoint.",
				"type": "object",
				"properties": {
					"client_id": {
						"title": "Client ID",
						"description": "The client ID for the SharePoint account.",
						"type": "string",
						"secret": true
					},
					"client_secret": {
						"title": "Client Secret",
						"description": "The client secret for the SharePoint account.",
						"type": "string",
						"secret": true
					},
					"refresh_token": {
						"title": "Refresh Token",
						"description": "The OAuth2 refresh token for the SharePoint account.",
						"type": "string",
						"secret": true
					}
				},
				"required": ["client_id", "client_secret", "refresh_token"],
				"x-oauth2-provider": "microsoft",
				"order": 0
			},
			"site_configuration": {
				"title": "Site Configuration",
				"description": "How to specify the SharePoint site and location to access.",
				"type": "object",
				"discriminator": {
					"propertyName": "method"
				},
				"oneOf": [
					{
						"title": "URL",
						"description": "Specify the SharePoint site using a complete URL including library and folder path.",
						"type": "object",
						"required": ["method", "site_url"],
						"properties": {
							"method": {
								"type": "string",
								"title": "Configuration Method",
								"const": "url",
								"default": "url",
								"order": 0
							},
							"site_url": {
								"type": "string",
								"title": "Site URL",
								"description": "The URL of the SharePoint site or Teams site including the document library and folder path. For example: \"https://contoso.sharepoint.com/sites/Marketing/Documents/my-folder\".",
								"order": 1
							}
						}
					},
					{
						"title": "Component IDs",
						"description": "Specify the SharePoint site using individual component IDs (site, drive, path).",
						"type": "object",
						"required": ["method", "site_id"],
						"properties": {
							"method": {
								"type": "string",
								"title": "Configuration Method",
								"const": "components",
								"default": "components",
								"order": 0
							},
							"site_id": {
								"type": "string",
								"title": "Site ID",
								"description": "The SharePoint site identifier. Accepts: (1) Full ID from Microsoft Graph API (e.g., \"contoso.sharepoint.com,2C712604-1370-44E7-A1F5-426573FDA80A,2D2244C3-251A-49EA-93A8-39E1C3A060FE\") or (2) Hostname with path (e.g., \"contoso.sharepoint.com:/sites/Marketing\").",
								"order": 1
							},
							"drive_id": {
								"type": "string",
								"title": "Drive ID",
								"description": "The ID of the SharePoint document library (drive) to access. If not provided, defaults to the site's default document library.",
								"order": 2
							},
							"path": {
								"type": "string",
								"title": "Path",
								"pattern": "^/.*",
								"description": "The path to the SharePoint folder to read from within the document library. Defaults to \"/\" if not provided. For example: \"/my-folder\".",
								"order": 3
							}
						}
					}
				],
				"order": 1
			},
			"matchKeys": {
				"type": "string",
				"title": "Match Keys",
				"format": "regex",
				"description": "Filter applied to all file paths under the specified folder. If provided, only files whose absolute path matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order": 2
			},
			"advanced": {
				"title": "Advanced",
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire path prefix. This requires that you write files in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering.",
					"default":     false
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true,
				"order": 3
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
			return newSharePointStore(ctx, cfg.(config))
		},
		ConfigSchema:     configSchema,
		DocumentationURL: "https://go.estuary.dev/source-sharepoint",
		Oauth2:           oauth2Spec(),
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta: time.Second * -30,
	}

	src.Main()
}
