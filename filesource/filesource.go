package filesource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/flow/go/parser"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"golang.org/x/sync/errgroup"
)

// Config of a filesource.
type Config interface {
	// Validate returns an error if the Config is malformed.
	Validate() error
	// DiscoverRoot path to use when walking discoverable directories.
	DiscoverRoot() string
	// FilesAreMonotonic is true if files are created or modified in
	// strictly monotonic, lexicographic order within each and every
	// captured stream prefix.
	FilesAreMonotonic() bool
	// ParserConfig returns the parser.Config of the Config.
	ParserConfig() *parser.Config
	// PathRegex returns an optional regular expression string which
	// is matched against paths.
	PathRegex() string
}

// Source is implements a capture connector using provided callbacks.
type Source struct {
	// ConfigSchema returns the JSON schema of the source's configuration,
	// given a parser JSON schema it may wish to embed.
	ConfigSchema func(parserSchema json.RawMessage) json.RawMessage
	// NewConfig decodes the input raw json into Config
	NewConfig func(raw json.RawMessage) (Config, error)
	// Connect using to the Source's Store using the decoded and validated Config.
	Connect func(context.Context, Config) (Store, error)
	// DocumentationURL links to the Source's extended user documentation.
	DocumentationURL string
	// TimeHorizonDelta is added to the current time in order to determine the upper time bound for
	// a sweep of the store. The final upper time bound used to bound modification times we'll
	// examine in a given sweep of the Store. Given a sampled wall-time T, we presume (hope) that no
	// files will appear in the store after T having a modification time of T - horizonDelta.
	TimeHorizonDelta time.Duration
}

// Store is a minimal interface of an binary large object storage service.
// It supports ordered listings of objects, and reading an single object.
type Store interface {
	// List objects of the Store.
	List(context.Context, Query) (Listing, error)
	// Read an object of the Store. The argument ObjectInfo is returned,
	// and may be enhanced with additional fields which weren't previously
	// available from the List API (for example, ContentEncoding).
	Read(context.Context, ObjectInfo) (io.ReadCloser, ObjectInfo, error)
}

// Query of objects to be returned by a Listing.
type Query struct {
	// Prefix constrains the listing to paths which begin with the prefix.
	Prefix string
	// StartAt constraints the listing to paths which are lexicographically equal or greater to StartAt.
	StartAt string
	// If Recursive is false, then file-like entries under the current Prefix
	// are returned, and additional entries are returned with IsPrefix set to
	// represent sub-directory like entries. If true, then IsPrefix entries are
	// not returned and all recursive files under the Prefix are listed.
	Recursive bool
}

type Listing interface {
	// Next returns the next ObjectInfo, an io.EOF if no entries remain,
	// or another encountered error.
	Next() (ObjectInfo, error)
}

// ListingFunc adapts a listing closure to a Listing interface.
type ListingFunc func() (ObjectInfo, error)

func (f ListingFunc) Next() (ObjectInfo, error) { return f() }

// ObjectInfo is returned by a Listing.
type ObjectInfo struct {
	// Path is the absolute path of the object or common prefix (if IsPrefix).
	Path string
	// IsPrefix indicates that Path is a common prefix of other objects.
	IsPrefix bool
	// ContentSum is an implementation-defined content sum.
	ContentSum string
	// Size of the object, in bytes, or -1 if unbounded or unknown.
	Size int64
	// ContentType of the object, if known.
	ContentType string
	// ContentEncoding of the object, if known.
	ContentEncoding string
	// ModTime of the object.
	ModTime time.Time
}

// PathToParts splits a path into a bucket and key. The key may be empty.
func PathToParts(path string) (bucket, key string) {
	if ind := strings.IndexByte(path, '/'); ind == -1 {
		return path, ""
	} else {
		return path[:ind], path[ind+1:]
	}
}

// PartsToPath maps a bucket and key into a path.
func PartsToPath(bucket, key string) string {
	return bucket + "/" + key
}

type connector struct {
	config Config
	store  Store
}

type resource struct {
	Stream   string `json:"stream" jsonschema:"title=Stream to capture from" jsonschema_extras="x-collection-name=true"`
	SyncMode string `json:"syncMode,omitempty" jsonschema:"-"`
}

func (r resource) Validate() error {
	if r.Stream == "" {
		return fmt.Errorf("stream is required")
	}
	return nil
}

func (src Source) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var parserSpec, err = parser.GetSpec()
	if err != nil {
		panic(err)
	}

	var endpointSchema = src.ConfigSchema(parserSpec)
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         src.DocumentationURL,
	}, nil
}

func (src *Source) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var bindings = []*pc.Response_Validated_Binding{}

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		bindings = append(bindings, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Stream},
		})
	}

	return &pc.Response_Validated{Bindings: bindings}, nil
}

// Discover returns the set of resources available from this Driver.
func (src *Source) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg, err = src.NewConfig(req.ConfigJson)
	if err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

  store, err := src.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to store: %w", err)
	}

	var conn = connector{config: cfg, store: store}

  var root = conn.config.DiscoverRoot()
  resourceJSON, err := json.Marshal(resource{Stream: root})

	return &pc.Response_Discovered{Bindings: []*pc.Response_Discovered_Binding{{
			RecommendedName:    pf.Collection(root),
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: json.RawMessage(minimalDocumentSchema),
			Key:                []string{"/_meta/file", "/_meta/offset"},
  }}}, nil
}

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (src *Source) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg, err = src.NewConfig(open.Capture.ConfigJson)
	if err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var ctx = stream.Context()

  store, err := src.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to store: %w", err)
	}

	var conn = connector{config: cfg, store: store}

	var states = make(States)
	if open.StateJson != nil {
		if err := pf.UnmarshalStrict(open.StateJson, &states); err != nil {
			return fmt.Errorf("parsing state checkpoint: %w", err)
		}
	}

	// Time horizon used for identifying files which fall within a modification time window.
	var horizon = time.Now().Add(src.TimeHorizonDelta).Round(time.Second).UTC()

	var sharedMu = new(sync.Mutex)

	var pathRe *regexp.Regexp
	if r := conn.config.PathRegex(); r != "" {
		if pathRe, err = regexp.Compile(r); err != nil {
			return fmt.Errorf("building regex: %w", err)
		}
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

  grp, ctx := errgroup.WithContext(ctx)
	for i, binding := range open.Capture.Bindings {
		// Stream names represent an absolute path prefix to capture.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}
		var prefix = res.Stream
		var state = states[prefix]

		state.startSweep(horizon)

		var r = &reader{
			connector: &conn,
      binding:   i,
      stream:    stream,
			pathRe:    pathRe,
			prefix:    prefix,
			schema:    binding.Collection.WriteSchemaJson,
			state:     state,
			range_:    open.Range,
		}

		r.shared.mu = sharedMu
		r.shared.states = states

		grp.Go(func() error {
			if err := r.sweep(ctx); err != nil {
				return fmt.Errorf("prefix %s: %w", prefix, err)
			}
			return nil
		})
	}

	return grp.Wait()
}

func (src *Source) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

func (src *Source) Main() {
	boilerplate.RunMain(src)
}

type reader struct {
	*connector

	pathRe *regexp.Regexp
	prefix string
	schema json.RawMessage
	state  State
  binding int
  stream *boilerplate.PullOutput
	range_ *pf.RangeSpec

	shared struct {
		mu     *sync.Mutex
		states States
	}
}

func (r *reader) sweep(ctx context.Context) error {
	log.Info(fmt.Sprintf("sweeping %s starting at %q, from %s through %s",
		r.prefix, r.state.Path, r.state.MinBound, r.state.MaxBound))

	var listing, err = r.store.List(ctx, Query{
		Prefix:    r.prefix,
		StartAt:   r.state.Path,
		Recursive: true,
	})
	if err != nil {
		return fmt.Errorf("starting listing: %w", err)
	}

	for {
		var obj, err = listing.Next()
		if err == io.EOF {
			break // Expected indication that the listing is complete.
		} else if err != nil {
			return fmt.Errorf("during listing: %w", err)
		} else if obj.IsPrefix {
			panic("implementation error (IsPrefix entry returned with Recursive: true Query)")
		}

		if skip, reason := r.shouldSkip(obj); skip {
			log.WithFields(log.Fields{"path": obj.Path, "reason": reason}).Debug("skipping file")
		} else if err = r.processObject(ctx, obj); err != nil {
			return fmt.Errorf("reading %s: %w", obj.Path, err)
		}
	}

	log.Info(fmt.Sprintf("completed sweep of %s from %s through %s",
		r.prefix, r.state.MinBound, r.state.MaxBound))
	r.state.finishSweep(r.config.FilesAreMonotonic())

	// Write a final checkpoint to mark the completion of the sweep.
	if err := r.emit(nil); err != nil {
		return err
	}

	return nil
}

func (r *reader) shouldSkip(obj ObjectInfo) (_ bool, reason string) {
	if skip, reason := r.state.shouldSkip(obj.Path, obj.ModTime); skip {
		return skip, reason
	}
	// Is this a 0-sized object? These are commonly used to represent something approximating a
	// directory, and should always be filtered out.
	if obj.Size == 0 {
		return true, "object has size of 0"
	}
	// Is the path excluded by our regex ?
	if r.pathRe != nil && !r.pathRe.MatchString(obj.Path) {
		return true, "regex not matched"
	}
	// Is it outside of our responsible key range?
	if !boilerplate.IncludesHwHash(r.range_, []byte(obj.Path)) {
		return true, "path not in range"
	}

	return false, ""
}

func (r *reader) processObject(ctx context.Context, obj ObjectInfo) error {
	rr, obj, err := r.store.Read(ctx, obj)
	if err != nil {
		return err
	}
	// Report errors from deferred closing of the reader.
	defer func() {
		closeErr := rr.Close()
		if err == nil {
			err = fmt.Errorf("processObject closing file: %w", closeErr)
		}
	}()

	if ok := r.state.startPath(obj.Path, obj.ModTime); !ok {
		log.WithField("path", obj.Path).Debug("skipping path (after Read)")
		return nil
	}
	log.Info(fmt.Sprintf("processing file %q modified at %s", obj.Path, obj.ModTime))

	tmp, err := ioutil.TempFile("", "parser-config-*.json")
	if err != nil {
		return fmt.Errorf("creating parser config: %w", err)
	}
	defer os.Remove(tmp.Name())

	if err = r.makeParseConfig(obj).WriteToFile(tmp); err != nil {
		return fmt.Errorf("writing parser config: %w", err)
	}

	err = parser.ParseStream(ctx, tmp.Name(), rr, func(lines []json.RawMessage) error {
		if lines = r.state.nextLines(lines); lines == nil {
			return nil
		}
		return r.emit(lines)
	})
	if err != nil {
		return fmt.Errorf("failed to parse object %q: %w", obj.Path, err)
	}
	r.state.finishPath()

	// Write a final checkpoint to mark the completion of the file.
	if err := r.emit(nil); err != nil {
		return err
	}

	return nil
}

func (r *reader) makeParseConfig(obj ObjectInfo) *parser.Config {
	var cfg = new(parser.Config)
	if c := r.config.ParserConfig(); c != nil {
		*cfg = c.Copy()
	}

	return configureParser(cfg, obj)
}

func (r *reader) emit(lines []json.RawMessage) error {
	for _, line := range lines {
		if err := r.stream.Documents(r.binding, line); err != nil {
			return err
		}
	}

	r.shared.mu.Lock()
	defer r.shared.mu.Unlock()
	r.shared.states[r.prefix] = r.state

  if encodedCheckpoint, err := json.Marshal(r.shared.states); err != nil {
    return err
  } else if err := r.stream.Checkpoint(encodedCheckpoint, true); err != nil {
		return err
	}

	return nil
}

func configureParser(cfg *parser.Config, obj ObjectInfo) *parser.Config {
	cfg.Filename = obj.Path
	if obj.ContentType != "" {
		cfg.ContentType = obj.ContentType
	}
	if obj.ContentEncoding != "" {
		cfg.ContentEncoding = obj.ContentEncoding
	}
	// If the user supplied a location for this, then we'll use that. Otherwise, use the default
	if cfg.AddRecordOffset == "" {
		cfg.AddRecordOffset = metaOffsetLocation
	}
	if cfg.AddValues == nil {
		cfg.AddValues = make(map[parser.JsonPointer]interface{})
	}
	cfg.AddValues[metaFileLocation] = obj.Path
	return cfg
}

const (
	// Location of the filename in produced documents.
	metaFileLocation = "/_meta/file"
	// Location of the record offset in produced documents.
	metaOffsetLocation = "/_meta/offset"
	// Baseline document schema for resource streams we discover.
	minimalDocumentSchema = `{
		"x-infer-schema": true,
		"type": "object",
		"properties": {
			"_meta": {
				"type": "object",
				"properties": {
					"file": { 
						"description": "The key of the source file, added automatically by Flow",
						"type": "string"
					},
					"offset": {
						"description": "The offset of the record within the source file, added automatically by Flow",
						"type": "integer",
						"minimum": 0
					}
				},
				"required": ["file", "offset"]
			}
		},
		"required": ["_meta"]
	}`
)
