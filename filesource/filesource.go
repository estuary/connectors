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

	"github.com/estuary/flow/go/parser"
	"github.com/estuary/flow/go/protocols/airbyte"
	log "github.com/sirupsen/logrus"
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
	// NewConfig returns a zero-valued Config which may be decoded into.
	NewConfig func() Config
	// Connect using to the Source's Store using the decoded and validated Config.
	Connect func(context.Context, Config) (Store, error)
	// DocumentationURL links to the Source's extended user documentation.
	DocumentationURL string
	// TimeHorizonDelta is added to the current time in order to determine the upper time bound for
	// a sweep of the store. The final upper time bound used to bound modification times we'll
	// examine in a given sweep of the Store. Given a sampled wall-time T, we presume (hope) that no
	// files will appear in the store after T having a modification time of T - horizonDelta.
	TimeHorizonDelta time.Duration
	// SkipSchemaInference allows connectors using the filesource framework to opt-in to bypass
	// schema inference based on actual discovered documents during the discovery phase, and instead
	// return a minimal baseline schema without analyzing discovered documents. This is a temporary
	// measure for cloud storage connectors, and inference will be removed entirely when the new
	// schema inference system is rolled out.
	SkipSchemaInference bool
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

func newConnector(src Source, args airbyte.ConfigFile) (*connector, error) {
	var cfg = src.NewConfig()

	if err := args.ConfigFile.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing configuration: %w", err)
	}

	var store, err = src.Connect(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to store: %w", err)
	}

	return &connector{config: cfg, store: store}, nil
}

func (src Source) Main() {
	var parserSpec, err = parser.GetSpec()
	if err != nil {
		panic(err)
	}
	var spec = airbyte.Spec{
		SupportsIncremental:           true,
		SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes,
		ConnectionSpecification:       src.ConfigSchema(parserSpec),
		DocumentationURL:              src.DocumentationURL,
	}
	airbyte.RunMain(spec, src.Check, src.Discover, src.Read)
}

func (src Source) Check(args airbyte.CheckCmd) error {
	var result = &airbyte.ConnectionStatus{
		Status: airbyte.StatusSucceeded,
	}

	var _, err = newConnector(src, args.ConfigFile)
	if err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func (src Source) Discover(args airbyte.DiscoverCmd) error {
	if src.SkipSchemaInference {
		var conn, err = newConnector(src, args.ConfigFile)
		if err != nil {
			return err
		}

		return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
			Type: airbyte.MessageTypeCatalog,
			Catalog: &airbyte.Catalog{
				Streams: []airbyte.Stream{{
					Name:               conn.config.DiscoverRoot(),
					JSONSchema:         json.RawMessage(minimalDocumentSchema),
					SupportedSyncModes: airbyte.AllSyncModes,
					SourceDefinedPrimaryKey: [][]string{
						{"_meta", "file"},
						{"_meta", "offset"},
					},
				}},
			},
		})
	}

	return src.discoverSchema(args)
}

func (src Source) Read(args airbyte.ReadCmd) error {
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var catalog = airbyte.ConfiguredCatalog{}
	if err = args.CatalogFile.Parse(&catalog); err != nil {
		return fmt.Errorf("parsing configured catalog: %w", err)
	}
	if catalog.Range.IsZero() {
		// Process all files, unless the parsed catalog says otherwise.
		catalog.Range = airbyte.NewFullRange()
	}

	var states = make(States)
	if args.StateFile != "" {
		if err = args.StateFile.Parse(&states); err != nil {
			return fmt.Errorf("parsing state: %w", err)
		}
	}

	// Time horizon used for identifying files which fall within a modification time window.
	var horizon = time.Now().Add(src.TimeHorizonDelta).Round(time.Second).UTC()

	var sharedMu = new(sync.Mutex)
	var enc = airbyte.NewStdoutEncoder()

	var pathRe *regexp.Regexp
	if r := conn.config.PathRegex(); r != "" {
		if pathRe, err = regexp.Compile(r); err != nil {
			return fmt.Errorf("building regex: %w", err)
		}
	}

	var grp, ctx = errgroup.WithContext(context.Background())
	for _, stream := range catalog.Streams {
		// Stream names represent an absolute path prefix to capture.
		var prefix = stream.Stream.Name
		var state = states[prefix]

		state.startSweep(horizon)

		var r = &reader{
			connector: conn,
			pathRe:    pathRe,
			prefix:    prefix,
			range_:    catalog.Range,
			schema:    stream.Stream.JSONSchema,
			state:     state,
		}

		r.shared.mu = sharedMu
		r.shared.states = states
		r.shared.enc = enc

		grp.Go(func() error {
			if err := r.sweep(ctx); err != nil {
				return fmt.Errorf("prefix %s: %w", prefix, err)
			}
			return nil
		})
	}

	return grp.Wait()
}

type reader struct {
	*connector

	pathRe *regexp.Regexp
	prefix string
	range_ airbyte.Range
	schema json.RawMessage
	state  State

	shared struct {
		mu     *sync.Mutex
		states States
		enc    *json.Encoder
	}
}

func (r *reader) sweep(ctx context.Context) error {
	r.log("sweeping %s starting at %q, from %s through %s",
		r.prefix, r.state.Path, r.state.MinBound, r.state.MaxBound)

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

	r.log("completed sweep of %s from %s through %s",
		r.prefix, r.state.MinBound, r.state.MaxBound)
	r.state.finishSweep(r.config.FilesAreMonotonic())

	// Write a final checkpoint to mark the completion of the sweep.
	if err := r.emit(nil); err != nil {
		return err
	}

	return nil
}

func (r *reader) log(msg string, args ...interface{}) {
	r.shared.mu.Lock()
	defer r.shared.mu.Unlock()
	_ = r.shared.enc.Encode(airbyte.NewLogMessage(airbyte.LogLevelInfo, msg, args...))
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
	if !r.range_.IncludesHwHash([]byte(obj.Path)) {
		return true, "path not in range"
	}

	return false, ""
}

func (r *reader) processObject(ctx context.Context, obj ObjectInfo) error {
	rr, obj, err := r.store.Read(ctx, obj)
	if err != nil {
		return err
	}
	defer rr.Close()

	if ok := r.state.startPath(obj.Path, obj.ModTime); !ok {
		log.WithField("path", obj.Path).Debug("skipping path (after Read)")
		return nil
	}
	r.log("processing file %q modified at %s", obj.Path, obj.ModTime)

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
	// Message which wraps Records we'll generate.
	var wrapper = &airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Stream:    r.prefix,
			EmittedAt: time.Now().UTC().UnixNano() / int64(time.Millisecond),
		},
	}
	// Message which wraps State checkpoints we'll generate.
	// Use a custom struct that matches the shape of airbyte.Message,
	// but specializes State.Data to our type and avoids a double-encoding.
	var stateWrapper = &struct {
		Type  airbyte.MessageType `json:"type"`
		State struct {
			Data States `json:"data"`
		} `json:"state,omitempty"`
	}{
		Type: airbyte.MessageTypeState,
	}

	r.shared.mu.Lock()
	defer r.shared.mu.Unlock()

	// Write out all records, wrapped in |wrapper|.
	for _, line := range lines {
		wrapper.Record.Data = line
		if err := r.shared.enc.Encode(wrapper); err != nil {
			return err
		}
	}

	// Update and write out state.
	r.shared.states[r.prefix] = r.state
	stateWrapper.State.Data = r.shared.states

	if err := r.shared.enc.Encode(stateWrapper); err != nil {
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
