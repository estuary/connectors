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

type headers struct {
	Items []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"items"`
}

type config struct {
	URL         string         `json:"url"`
	Credentials *credentials   `json:"credentials"`
	Parser      *parser.Config `json:"parser"`
	Headers     headers        `json:"headers"`
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
	cfg      *config
	req      *http.Request
	resp     *http.Response
	fileName string
}

func newHttpSource(ctx context.Context, cfg *config) (*httpSource, error) {
	var parsedURL, err = url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("could not parse URL %s: %w", cfg.URL, err)
	}
	var pathPieces = strings.Split(parsedURL.Path, "/")

	// Prepare request.
	var req = (&http.Request{
		Method: "GET",
		URL:    parsedURL,
		Header: make(http.Header),
	}).WithContext(ctx)

	for _, header := range cfg.Headers.Items {
		req.Header.Add(header.Key, header.Value)
	}
	if cfg.Credentials != nil {
		req.SetBasicAuth(cfg.Credentials.User, cfg.Credentials.Password)
	}

	// Initiate, fetch headers, and queue body for future reading.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not GET %s: %w", cfg.URL, err)
	}

	return &httpSource{
		cfg:      cfg,
		req:      req,
		resp:     resp,
		fileName: pathPieces[len(pathPieces)-1],
	}, nil
}

func (s *httpSource) List(_ context.Context, query filesource.Query) (filesource.Listing, error) {
	var emitted = false

	return filesource.ListingFunc(func() (filesource.ObjectInfo, error) {
		if emitted {
			return filesource.ObjectInfo{}, io.EOF
		}
		emitted = true

		var err error
		var modTime time.Time
		var size int64

		if mt := s.resp.Header.Get("Last-Modified"); mt == "" {
			modTime = time.Now()
		} else if modTime, err = http.ParseTime(mt); err != nil {
			return filesource.ObjectInfo{}, fmt.Errorf("invalid Last-Modified: %w", err)
		}

		if cl := s.resp.Header.Get("Content-Length"); cl == "" {
			size = -1 // Unbounded or unknown.
		} else if size, err = strconv.ParseInt(cl, 10, 64); err != nil {
			return filesource.ObjectInfo{}, fmt.Errorf("invalid Content-Length: %w", err)
		}

		return filesource.ObjectInfo{
			Path:            s.fileName,
			IsPrefix:        false,
			ContentEncoding: s.resp.Header.Get("Content-Encoding"),
			ContentSum:      s.resp.Header.Get("ETag"),
			ContentType:     s.resp.Header.Get("Content-Type"),
			ModTime:         modTime,
			Size:            size,
		}, nil
	}), nil
}

func (s *httpSource) Read(ctx context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	// We already started our read under a different parent context.
	// Retain this Read `ctx` and wrap so that we cancel appropriately.
	return &ctxReader{
		ReadCloser: s.resp.Body,
		ctx:        ctx,
	}, obj, nil
}

type ctxReader struct {
	io.ReadCloser
	ctx context.Context
}

func (r *ctxReader) Read(p []byte) (n int, err error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.ReadCloser.Read(p)
	}
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
			"parser": ` + string(parserSchema) + `,
				"headers": {
					"title": "Headers",
					"type": "object",
					"properties": {
						"items": {
							"title": "Additional HTTP Headers",
							"description": "Additional HTTP headers when requesting the file. These are uncommon.",
							"type": "array",
							"items": {
								"type": "object",
								"properties": {
									"key": {
										"type": "string",
										"title": "Header Key"
									},
									"value": {
										"type": "string",
										"title": "Header Value",
										"secret": true
									}
								}
							}
						}
					}
				}
       		}
      	}`)
		},
		DocumentationURL: "https://go.estuary.dev/source-http-file",
		// Set the delta slightly in the future to include modification times which
		// are just after the sweep start, such as near-realtime file sources.
		// The scenario that the the `MaxBound` protects against is only possible
		// when a listing can return multiple files, which is never the case for
		// this single-file connector.
		//
		// This value should not be very large. If it's too far in the future
		// then a connector which is restarted part-way through a file will
		// not re-process that file until it's modification time is greater
		// than the prior invocation time plus this delta.
		TimeHorizonDelta: 2 * time.Second,
	}

	src.Main()
}
