package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/estuary/connectors/go-types/filesource"
	"github.com/estuary/connectors/go-types/parser"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type config struct {
	AscendingKeys     bool            `json:"ascendingKeys"`
	Bucket            string          `json:"bucket"`
	GoogleCredentials json.RawMessage `json:"googleCredentials"`
	MatchKeys         string          `json:"matchKeys"`
	Parser            *parser.Config  `json:"parser"`
	Prefix            string          `json:"prefix"`
}

func (c *config) Validate() error {
	return nil
}

func (c *config) DiscoverRoot() string {
	return filesource.PartsToPath(c.Bucket, c.Prefix)
}

func (c *config) FilesAreMonotonic() bool {
	return c.AscendingKeys
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
	if !bytes.Equal(cfg.GoogleCredentials, []byte("null")) {
		opt = option.WithCredentialsJSON([]byte(cfg.GoogleCredentials))
	} else {
		opt = option.WithoutAuthentication()
	}

	var client, err = storage.NewClient(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("creating GCS session: %w", err)
	}

	return &gcStore{gcs: client}, nil
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
	obj.ModTime = r.Attrs.LastModified

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
		"title":   "GCS Source Specification",
		"type":    "object",
		"required": [
			"bucket"
		],
		"properties": {
			"ascendingKeys": {
				"type":        "boolean",
				"title":       "Ascending Keys",
				"description": "Improve sync speeds by listing files from the end of the last sync, rather than listing the entire bucket prefix. This requires that you write objects in ascending lexicographic order, such as an RFC-3339 timestamp, so that key ordering matches modification time ordering.",
				"default":     false
			},
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the Google Cloud Storage bucket"
			},
			"googleCredentials": {
				"type":        "object",
				"title":       "Google Service Account",
				"description": "Service account JSON file to use as Application Default Credentials"
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files."
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from"
			}
		}
    }`)
		},
	}

	src.Main()
}

const gcsDelimiter = "/"
