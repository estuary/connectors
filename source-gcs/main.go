package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/estuary/connectors/go-types/filesource"
	"github.com/estuary/connectors/go-types/parser"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type config struct {
	Bucket            string          `json:"bucket"`
	Prefix            string          `json:"prefix"`
	MatchKeys         string          `json:"matchKeys"`
	GoogleCredentials json.RawMessage `json:"googleCredentials"`
	Parser            *parser.Config  `json:"parser"`
}

func (c *config) Validate() error {
	return nil
}

func (c *config) DiscoverRoot() string {
	return partsToPath(c.Bucket, c.Prefix)
}

func (c *config) FilesAreMonotonic() bool {
	return false
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
	var bucket, prefix, err = pathToParts(query.Prefix)
	if err != nil {
		return nil, err
	}

	var gcsQuery = &storage.Query{
		Prefix: prefix,
	}

	if !query.Recursive {
		gcsQuery.Delimiter = gcsDelimiter
	}
	if query.StartAt != "" {
		if _, startAt, err := pathToParts(query.StartAt); err != nil {
			return nil, err
		} else {
			gcsQuery.StartOffset = startAt
		}
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
				Path:     partsToPath(bucket, obj.Prefix),
				IsPrefix: true,
			}, nil
		} else {
			return filesource.ObjectInfo{
				Path:            partsToPath(bucket, obj.Name),
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
	var bucket, key, err = pathToParts(obj.Path)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	r, err := s.gcs.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}
	obj.ModTime = r.Attrs.LastModified

	return r, obj, nil
}

// Parses a path into its constituent bucket and key. The key may be empty.
func pathToParts(path string) (bucket, key string, _ error) {
	if ind := strings.IndexByte(path, ':'); ind == -1 {
		return "", "", fmt.Errorf("%q is missing `bucket:` prefix", path)
	} else {
		return path[:ind], path[ind+1:], nil
	}
}

// Forms a path from a bucket and key.
func partsToPath(bucket, key string) string {
	return bucket + ":" + key
}

func main() {

	var src = filesource.Source{
		NewConfig: func() filesource.Config { return new(config) },
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newGCStore(ctx, cfg.(*config))
		},
		ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
			return json.RawMessage(fmt.Sprintf(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "GCS Source Specification",
		"type":    "object",
		"required": [
			"bucket"
		],
		"properties": {
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the Google Cloud Storage bucket"
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from"
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files."
			},
			"googleCredentials": {
				"type":        "object",
				"title":       "Google Service Account",
				"description": "Service account JSON file to use as Application Default Credentials"
			},
			"parser": %s
		}
    }`, string(parserSchema)))
		},
	}

	src.Main()
}

const gcsDelimiter = "/"
