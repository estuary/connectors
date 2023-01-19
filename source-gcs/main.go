package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type config struct {
	Bucket            string         `json:"bucket"`
	GoogleCredentials string         `json:"googleCredentials"`
	MatchKeys         string         `json:"matchKeys"`
	Parser            *parser.Config `json:"parser"`
	Prefix            string         `json:"prefix"`
	Advanced          advancedConfig `json:"advanced"`
}

type advancedConfig struct {
	AscendingKeys bool `json:"ascendingKeys"`
}

func (c *config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("missing bucket")
	}

	return nil
}

func (c *config) DiscoverRoot() string {
	return filesource.PartsToPath(c.Bucket, c.Prefix)
}

func (c *config) FilesAreMonotonic() bool {
	return c.Advanced.AscendingKeys
}

func (c *config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c *config) PathRegex() string {
	return c.MatchKeys
}

type gcStore struct {
	gcs *storage.Client
}

func newGCStore(ctx context.Context, cfg *config) (*gcStore, error) {

	var opt option.ClientOption
	if cfg.GoogleCredentials != "" {
		opt = option.WithCredentialsJSON([]byte(cfg.GoogleCredentials))
	} else {
		opt = option.WithoutAuthentication()
	}

	var client, err = storage.NewClient(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("creating GCS session: %w", err)
	}

	if err := validateBucket(ctx, cfg, client); err != nil {
		return nil, err
	}

	return &gcStore{gcs: client}, nil
}

// validateBucket verifies that we can list objects in the bucket and read an object in the bucket.
// This is done in a way that requires only storage.objects.list and storage.objects.get since these
// are the permissions required by the connector.
func validateBucket(ctx context.Context, cfg *config, client *storage.Client) error {
	iter := client.Bucket(cfg.Bucket).Objects(ctx, &storage.Query{
		Prefix: cfg.Prefix,
	})

	// Optimization to limit the number of buffered objects, since all we care about is a successful
	// listing rather than iterating on all the objects.
	iter.PageInfo().MaxSize = 1

	// No objects in the bucket is represented by iterator.Done. This can be considered a successful
	// outcome, since a bucket we don't have access to or doesn't exist will return a specific error
	// for that case.
	if _, err := iter.Next(); err != nil && err != iterator.Done {
		return fmt.Errorf("unable to list objects in bucket %q: %w", cfg.Bucket, err)
	}

	// storage.objects.get allows reading object data as well as object metadata. The name of the
	// object is not important here. We have verified our ability to list objects in this bucket
	// (bucket exists & correct access) via the list operation, so we can interpret a "not found" as
	// a successful outcome.
	objectKey := strings.TrimPrefix(path.Join(cfg.Prefix, uuid.NewString()), "/")
	if _, err := client.Bucket(cfg.Bucket).Object(objectKey).Attrs(ctx); err != nil && err != storage.ErrObjectNotExist {
		return fmt.Errorf("unable to read objects in bucket %q: %w", cfg.Bucket, err)
	}

	return nil
}

func (s *gcStore) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	var bucket, prefix = filesource.PathToParts(query.Prefix)

	var gcsQuery = &storage.Query{
		Prefix: prefix,
	}

	if !query.Recursive {
		gcsQuery.Delimiter = gcsDelimiter
	}
	if query.StartAt != "" {
		_, startAt := filesource.PathToParts(query.StartAt)
		gcsQuery.StartOffset = startAt
	}

	var it = s.gcs.Bucket(bucket).Objects(ctx, gcsQuery)

	return filesource.ListingFunc(func() (filesource.ObjectInfo, error) {
		var obj, err = it.Next()

		if err == iterator.Done {
			return filesource.ObjectInfo{}, io.EOF
		} else if err != nil {
			return filesource.ObjectInfo{}, err
		} else if obj.Prefix != "" {
			return filesource.ObjectInfo{
				Path:     filesource.PartsToPath(bucket, obj.Prefix),
				IsPrefix: true,
			}, nil
		} else {
			return filesource.ObjectInfo{
				Path:            filesource.PartsToPath(bucket, obj.Name),
				IsPrefix:        false,
				ContentEncoding: obj.ContentEncoding,
				ContentSum:      obj.Etag,
				ContentType:     obj.ContentType,
				ModTime:         obj.Updated,
				Size:            obj.Size,
			}, nil
		}
	}), nil
}

func (s *gcStore) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	var bucket, key = filesource.PathToParts(obj.Path)

	r, err := s.gcs.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	// Note that bucket listings have sub-second granularity, but LastModified
	// is rounded to seconds (and is thus often slightly before obj.ModTime).
	if r.Attrs.LastModified.After(obj.ModTime) {
		obj.ModTime = r.Attrs.LastModified
	}

	return r, obj, nil
}

func main() {

	var src = filesource.Source{
		NewConfig: func() filesource.Config { return new(config) },
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newGCStore(ctx, cfg.(*config))
		},
		ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
			return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "GCS Source",
		"type":    "object",
		"required": [
			"bucket"
		],
		"properties": {
			"googleCredentials": {
				"type":        "string",
				"title":       "Google Service Account",
				"description": "Service account JSON key to use as Application Default Credentials",
				"multiline":   true,
				"secret":      true,
				"order":       0
			},
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the Google Cloud Storage bucket",
				"order":       1
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from",
				"order":       2
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files.",
				"order":       3
			},
			"advanced": {
				"properties": {
				  "ascendingKeys": {
					"type":        "boolean",
					"title":       "Ascending Keys",
					"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering. For more information see https://go.estuary.dev/fOMT4s.",
					"default":     false
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"description": "Options for advanced users. You should not typically need to modify these.",
				"advanced": true
			},
			"parser": ` + string(parserSchema) + `
		}
    }`)
		},
		DocumentationURL: "https://go.estuary.dev/source-gcs",
		// Set the delta to 30 seconds in the past, to guard against new files appearing with a
		// timestamp that's equal to the `MinBound` in the state.
		TimeHorizonDelta:    time.Second * -30,
	}

	src.Main()
}

const gcsDelimiter = "/"
