package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/filesource"
	"github.com/estuary/flow/go/parser"
)

type credentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type config struct {
	URL         string         `json:"url"`
	Credentials *credentials   `json:"credentials"`
	Parser      *parser.Config `json:"parser"`
}

func (c *config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required")
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
	// Conceptually, if the source files are monotonic, the same file will not be visited again, and only
	// ascending file names are visited, so we set this to false.
	return false
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

func ModTime(resp *http.Response) (time.Time, error) {
	const LastModifiedHeaderLayout = "Mon, 02 Jan 2006 15:04:05 GMT"

	var value = resp.Header.Get("Last-Modified")
	if value == "" {
		return time.Now(), fmt.Errorf("Could not find Last-Modified header in response of HEAD request")
	}

	return time.Parse(LastModifiedHeaderLayout, value)
}

func newHttpSource(ctx context.Context, cfg *config) (*httpSource, error) {
	var parsedURL, err = url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("could not parse URL %s: %w", cfg.URL, err)
	}

	var pathPieces = strings.Split(parsedURL.Path, "/")

	var client = http.Client{}
	var req = http.Request{
		Method: "HEAD",
		URL:    parsedURL,
		Header: http.Header{},
	}
	if cfg.Credentials != nil {
		req.SetBasicAuth(cfg.Credentials.User, cfg.Credentials.Password)
	}
	resp, err := client.Do(&req)
	if err != nil {
		return nil, fmt.Errorf("could not fetch HEAD %s: %w", cfg.URL, err)
	}

	modTime, err := ModTime(resp)
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
	var req = http.Request{
		Method: "GET",
		URL:    s.parsedURL,
		Header: http.Header{},
	}
	if s.cfg.Credentials != nil {
		req.SetBasicAuth(s.cfg.Credentials.User, s.cfg.Credentials.Password)
	}
	var resp, err = s.client.Do(&req)
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
        "required": ["url"],
        "properties": {
          "url": {
            "type":        "string",
            "title":       "HTTP File URL",
            "description": "A valid HTTP url for downloading the source file."
          },
          "credentials": {
            "title": "Credentials",
            "description": "User credentials, if required to access the data at the HTTP URL.",
            "oneOf": [{
              "type": "object",
              "title": "Basic Authentication",
              "properties": {
                "user": {
                  "type": "string",
                  "title": "Username",
                  "description":"Username, if required to access the HTTP endpoint.",
                  "secret": true
                },
                "password": {
                  "type": "string",
                  "secret": true,
                  "description":"Password, if required to access the HTTP endpoint.",
                  "title": "Password"
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
