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
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type credentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// headers is simply a list of headers. The only reason we wrap and nest in an
// enclosing struct is to convince the UI to render headers as a separate tab
// and not within the principal configuration.
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

func (c *config) parsedURL() (*url.URL, string, error) {
	if c.URL == "" {
		return nil, "", fmt.Errorf("url is required")
	}

	var p, err = url.Parse(c.URL)
	if err != nil {
		return nil, "", fmt.Errorf("URL is invalid: %w", err)
	} else if !p.IsAbs() {
		return nil, "", fmt.Errorf(
			"URL %q is not valid (it must be a complete URL with an HTTP scheme and domain)", c.URL)
	}

	var pathPieces = strings.Split(strings.TrimRight(p.Path, "/"), "/")
	var name = pathPieces[len(pathPieces)-1]

	if name == "" {
		return p, p.Hostname(), nil
	}
	return p, name, nil
}

func isWikimediaStream(url string) bool {
	return strings.Contains(url, "stream.wikimedia.org") && strings.Contains(url, "/stream/")
}

// isStreamCancelledError detects if an error is due to an HTTP/2 stream being cancelled from the server.
// An error due to HTTP/2 stream cancellation looks like "stream error: stream ID 1; CANCEL; received from peer".
func isStreamCancelledError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "stream ID") &&
		strings.Contains(errStr, "CANCEL") &&
		strings.Contains(errStr, "received from peer")
}

func (c config) Validate() error {
	if _, _, err := c.parsedURL(); err != nil {
		return err
	}
	return nil
}

func (c config) DiscoverRoot() string {
	var _, name, err = c.parsedURL()
	if err != nil {
		panic(fmt.Sprintf("invalid URL: %s", err))
	}
	return name
}

func (c config) RecommendedName() string {
	return strings.Trim(c.DiscoverRoot(), "/")
}

func (c config) FilesAreMonotonic() bool {
	// Conceptually, if the source files are monotonic, the same file will not be visited again, and only
	// ascending file names are visited, so we set this to false.
	return false
}

func (c config) ParserConfig() *parser.Config {
	return c.Parser
}

func (c config) PathRegex() string {
	return ""
}

type httpSource struct {
	cfg             *config
	req             *http.Request
	resp            *http.Response
	fileName        string
	isReconnectable bool
	retryCount      int
	maxRetries      int
}

func doHttpRequest(ctx context.Context, cfg *config) (*http.Request, *http.Response, error) {
	parsedURL, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse URL %s: %w", cfg.URL, err)
	}

	// Prepare request.
	req := (&http.Request{
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
		return nil, nil, fmt.Errorf("could not GET %s: %w", cfg.URL, err)
	}

	// Return an error for a non 2xx responses.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var bodyBytes []byte
		if resp.Body != nil {
			if bodyBytes, err = io.ReadAll(resp.Body); err != nil {
				return nil, nil, fmt.Errorf("could not read response body: %w", err)
			}
			if err = resp.Body.Close(); err != nil {
				return nil, nil, fmt.Errorf("could not close response body: %w", err)
			}
		}
		bodyText := string(bodyBytes)
		if len(bodyText) > 500 {
			bodyText = bodyText[:500] + "..."
		}
		return nil, nil, fmt.Errorf("HTTP request failed with status %s: %s", resp.Status, bodyText)
	}

	return req, resp, nil
}

func newHttpSource(ctx context.Context, cfg config) (*httpSource, error) {
	var parsedURL, err = url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("could not parse URL %s: %w", cfg.URL, err)
	}
	var pathPieces = strings.Split(parsedURL.Path, "/")

	req, resp, err := doHttpRequest(ctx, &cfg)
	if err != nil {
		return nil, err
	}

	return &httpSource{
		cfg:      &cfg,
		req:      req,
		resp:     resp,
		fileName: pathPieces[len(pathPieces)-1],
		// isResumable indicates whether or not the connector should reconnect on an
		// HTTP/2 connection cancellation error. This is currently a carve out just for
		// the wikimedia stream. The httpSource does not try to resume where it left off previously,
		// and it simply creates a new connection that starts reading at the stream's head.
		isReconnectable: isWikimediaStream(cfg.URL),
		retryCount:      0,
		maxRetries:      10,
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
	reader := &ctxReader{
		ReadCloser: s.resp.Body,
		ctx:        ctx,
	}

	if s.isReconnectable {
		return &reconnectableWrapper{
			ctxReader: reader,
			source:    s,
		}, obj, nil
	}

	return reader, obj, nil
}

type reconnectableWrapper struct {
	*ctxReader
	source *httpSource
}

func (w *reconnectableWrapper) Read(p []byte) (n int, err error) {
	n, err = w.ctxReader.Read(p)
	if err != nil && err != io.EOF {
		if isStreamCancelledError(err) && w.source.retryCount < w.source.maxRetries {
			log.WithFields(log.Fields{
				"error":      err,
				"retryCount": w.source.retryCount,
			}).Info("stream was cancelled, attempting to reconnect")

			// Close the old connection.
			if err = w.source.resp.Body.Close(); err != nil {
				return n, fmt.Errorf("closing previous connection: %w", err)
			}

			// Create a new connection.
			_, resp, err := doHttpRequest(w.ctxReader.ctx, w.source.cfg)
			if err != nil {
				return n, fmt.Errorf("reconnecting to stream: %w", err)
			}

			w.source.resp = resp
			w.ctxReader.ReadCloser = resp.Body
			w.source.retryCount++

			log.Info("reconnected to stream")
			return w.Read(p)
		} else if w.source.retryCount >= w.source.maxRetries {
			log.WithField("maxRetries", w.source.maxRetries).Error("max retries exceeded for reconnecting to stream")
		}
	}
	return n, err
}

func (w *reconnectableWrapper) Close() error {
	return w.source.resp.Body.Close()
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
		NewConfig: func(raw json.RawMessage) (filesource.Config, error) {
			var cfg config
			if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf("parsing config json: %w", err)
			}
			return cfg, nil
		},
		Connect: func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
			return newHttpSource(ctx, cfg.(config))
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
			"description": "The HTTP URL of the source file to capture."
			},
			"credentials": {
			"title": "Credentials",
			"description": "User credentials, if required to access the data at the HTTP URL.",
			"type": "object",
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
