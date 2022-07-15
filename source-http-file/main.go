package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
)

type modificationTime struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

type credentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type config struct {
	URL              string           `json:"url"`
	ModificationTime modificationTime `json:"modificationTime"`
	Credentials      *credentials     `json:"credentials"`
	Parser           *parser.Config   `json:"parser"`
}

func (c *config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required")
	}
	if c.ModificationTime.Type == "" || c.ModificationTime.Key == "" {
		return fmt.Errorf("modification time type and key are required")
	}
	return nil
}

func (c *config) DiscoverRoot() string {
	var parsedURL, err = url.Parse(c.URL)
	if err != nil {
		return c.URL
	}
	var pathPieces = strings.Split(parsedURL.Path, "/")
	return pathPieces[len(pathPieces)-1]
}

func (c *config) FilesAreMonotonic() bool {
	// FIXME / Question: we have only one file, so I suppose it's monotonic
	return true
}

func (c *config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c *config) PathRegex() string {
	return ""
}

type httpSource struct {
	client    http.Client
	cfg       *config
	parsedURL *url.URL
	head      *http.Response
	modTime   time.Time
	fileName  string
}

func ModTime(typ string, key string, resp *http.Response) (time.Time, error) {
	const LastModifiedHeaderLayout = "Mon, 02 Jan 2006 15:04:05 GMT"

	if typ == "header" {
		var value = resp.Header.Get(key)
		if value == "" {
			return time.Now(), fmt.Errorf("Could not find header %s in response of HEAD request", key)
		}

		if key == "Last-Modified" {
			return time.Parse(LastModifiedHeaderLayout, value)
		} else {
			return time.Parse(time.RFC3339, value)
		}
	} else {
		return time.Now(), fmt.Errorf("Unsupported Modification Time Type %s", typ)
	}
}

func newHttpSource(ctx context.Context, cfg *config) (*httpSource, error) {
	var parsedURL, err = url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("could not parse URL %s: %w", cfg.URL, err)
	}

	var pathPieces = strings.Split(parsedURL.Path, "/")

	var client = http.Client{}
	resp, err := client.Head(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("could not fetch HEAD %s: %w", cfg.URL, err)
	}

	modTime, err := ModTime(strings.ToLower(cfg.ModificationTime.Type), cfg.ModificationTime.Key, resp)
	if err != nil {
		return nil, fmt.Errorf("processing modification time: %w", err)
	}

	return &httpSource{
		client:    client,
		cfg:       cfg,
		head:      resp,
		parsedURL: parsedURL,
		fileName:  pathPieces[len(pathPieces)-1],
		modTime:   modTime,
	}, nil
}

func (s *httpSource) List(ctx context.Context, query filesource.Query) (filesource.Listing, error) {
	var emitted = false

	return filesource.ListingFunc(func() (filesource.ObjectInfo, error) {
		if emitted {
			return filesource.ObjectInfo{}, io.EOF
		} else {
			var size, err = strconv.ParseInt(s.head.Header.Get("Content-Length"), 10, 64)
			if err != nil {
				return filesource.ObjectInfo{}, err
			}
			emitted = true
			return filesource.ObjectInfo{
				Path:            s.fileName,
				IsPrefix:        false,
				ContentEncoding: s.head.Header.Get("Content-Encoding"),
				ContentSum:      s.head.Header.Get("ETag"),
				ContentType:     s.head.Header.Get("Content-Type"),
				ModTime:         s.modTime,
				Size:            size,
			}, nil
		}
	}), nil
}

func (s *httpSource) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	var resp, err = s.client.Get(s.cfg.URL)
	if err != nil {
		return nil, filesource.ObjectInfo{}, err
	}

	return resp.Body, obj, nil
}

func main() {
	var src = filesource.Source{
		NewConfig: func() filesource.Config { return new(config) },
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newHttpSource(ctx, cfg.(*config))
		},
		ConfigSchema: func(parserSchema json.RawMessage) json.RawMessage {
			return json.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "HTTP File Source",
		"type":    "object",
		"required": [
			"url",
      "modificationTime"
		],
		"properties": {
			"url": {
				"type":        "string",
				"title":       "HTTP File URL",
				"description": "A valid HTTP url for downloading the source file."
			},
      "modificationTime": {
        "type": "object",
        "title": "Modification Time",
        "description": "Specify how the last modification time of the file can be determined from the HTTP response. For example the 'Last-Modified' key in header.",
        "required": ["type", "key"],
        "properties": {
          "type": {
            "type":        "string",
            "title":       "Modification Time Type",
            "enum": ["Header"],
            "description": "Where in the HTTP response is the last modification time of the file available.",
            "default": "Header"
          },
          "key": {
            "type":        "string",
            "title":       "Modification Time Key",
            "description": "What is the modification time's key.",
            "default": "Last-Modified"
          }
        }
      },
      "credentials": {
        "title": "Credentials",
        "oneOf": [{
          "type": "object",
          "title": "Basic Authentication",
          "properties": {
            "user": {
              "type": "string",
              "secret": true
            },
            "password": {
              "type": "string",
              "secret": true
            }
          }
        }]
      },
			"parser": ` + string(parserSchema) + `
		}
    }`)
		},
		DocumentationURL: "https://go.estuary.dev/source-http-file",
	}

	src.Main()
}
