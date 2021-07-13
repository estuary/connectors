package filesource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/parser"
	"github.com/estuary/connectors/go-types/shardrange"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

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

type Source struct {
	// ConfigSchema returns the JSON schema of the source's configuration,
	// given a parser JSON schema it may wish to embed.
	ConfigSchema func(parserSchema json.RawMessage) json.RawMessage
	// NewConfig returns a zero-valued Config which may be decoded into.
	NewConfig func() Config
	// Connect using to the Source's Store using the decoded and validated Config.
	Connect func(context.Context, Config) (Store, error)
}

type Store interface {
	List(context.Context, Query) (Listing, error)
	Read(context.Context, ObjectInfo) (io.ReadCloser, ObjectInfo, error)
}

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
	Path            string
	IsPrefix        bool
	ContentSum      string
	Size            int64
	ContentType     string
	ContentEncoding string
	ModTime         time.Time
}

// ----------------------------------------------------------------
//

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
		return nil, fmt.Errorf("building filesystem: %w", err)
	}

	return &connector{
		config: cfg,
		store:  store,
	}, nil
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
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var ctx = context.Background()

	// Breadth-first search.
	var stack = []string{conn.config.DiscoverRoot()}
	var streams []airbyte.Stream

	for len(stack) != 0 && len(streams) < 10 {
		var prefix = stack[0]

		var listing, err = conn.store.List(ctx, Query{
			Prefix:    prefix,
			Recursive: false,
		})
		if err != nil {
			return fmt.Errorf("starting listing %q: %w", prefix, err)
		}

		var hasObjects bool
		for i := 0; i != discoverListLimit; i++ {
			var entry, err = listing.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("during listing %q: %w", prefix, err)
			} else if entry.IsPrefix {
				stack = append(stack, entry.Path)
			} else {
				hasObjects = true
			}
		}

		// Make streams of non-empty directories and the discovery root,
		// even if the latter is empty.
		if hasObjects || len(streams) == 0 {
			streams = append(streams, airbyte.Stream{
				Name:               prefix,
				JSONSchema:         json.RawMessage(documentSchema),
				SupportedSyncModes: airbyte.AllSyncModes,
				SourceDefinedPrimaryKey: [][]string{
					{"_meta", "file"},
					{"_meta", "offset"},
				},
			})
		}
		stack = stack[1:]
	}

	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type: airbyte.MessageTypeCatalog,
		Catalog: &airbyte.Catalog{
			Streams: streams,
		},
	})
}

func (src Source) Read(args airbyte.ReadCmd) error {
	var conn, err = newConnector(src, args.ConfigFile)
	if err != nil {
		return err
	}
	var catalog = airbyte.ConfiguredCatalog{
		// Process all files, unless the parsed catalog says otherwise.
		Range: shardrange.NewFullRange(),
	}
	if err = args.CatalogFile.Parse(&catalog); err != nil {
		return fmt.Errorf("parsing configured catalog: %w", err)
	}

	var states = make(prefixStates)
	if args.StateFile != "" {
		if err = args.StateFile.Parse(&states); err != nil {
			return fmt.Errorf("parsing state: %w", err)
		}
	}

	// Time horizon used for identifying files which fall within a modification time window.
	var horizon = time.Now().Add(-30 * time.Second).Round(time.Second).UTC()

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

		// If MaxMod is zero, then we're beginning a new sweep of this prefix.
		if state.MaxMod.IsZero() {
			state.MaxMod = horizon
		}

		var r = &reader{
			connector:   conn,
			prefix:      prefix,
			state:       state,
			range_:      catalog.Range,
			projections: make(map[string]parser.JsonPointer),
			pathRe:      pathRe,
			schema:      stream.Stream.JSONSchema,
		}
		for k, v := range stream.Projections {
			r.projections[k] = parser.JsonPointer(v)
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

	projections map[string]parser.JsonPointer
	schema      json.RawMessage
	prefix      string
	state       prefixState
	range_      shardrange.Range
	pathRe      *regexp.Regexp

	shared struct {
		mu     *sync.Mutex
		states prefixStates
		enc    *json.Encoder
	}
}

type prefixStates map[string]prefixState

func (p prefixStates) Validate() error {
	for prefix, state := range p {
		if err := state.Validate(); err != nil {
			return fmt.Errorf("prefix %s: %w", prefix, err)
		}
	}
	return nil
}

type prefixState struct {
	// Minimim time that's an inclusive lower-bound of files processed in this sweep.
	MinMod time.Time `json:"minMod,omitempty"`
	// Maximum time that's an exclusive upper-bound of files processed in this sweep.
	// If MaxMod is zero, then this state checkpoint was generated after the completion
	// of a prior sweep and before the commencement of the next one.
	MaxMod time.Time `json:"maxMod,omitempty"`
	// Base path which is currently being processed, or was last processed (if Complete).
	Path string `json:"path,omitempty"`
	// Number of records from the file at |Path| which have been emitted.
	Records int `json:"records,omitempty"`
	// Whether the file at |Path| is complete.
	Complete bool `json:"complete,omitempty"`
}

func (p prefixState) Validate() error {
	if !p.MaxMod.IsZero() && !p.MaxMod.After(p.MinMod) {
		return fmt.Errorf("expected MaxMod > MinMod")
	} else if p.Records < 0 {
		return fmt.Errorf("expected Records >= 0")
	} else if p.Complete && p.Records != 0 {
		return fmt.Errorf("didn't expect Records > 0 when Complete")
	} else if p.Complete && p.Path == "" {
		return fmt.Errorf("didn't expect Complete when Path is empty")
	}

	return nil
}

func (r *reader) sweep(ctx context.Context) error {
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

	// Update state to reflect that the sweep was completed.
	// A MaxMod of zero indicates this checkpoint is between sweeps.
	// The next invocation will supply a new MaxMod.
	r.state.MinMod, r.state.MaxMod = r.state.MaxMod, time.Time{}

	if !r.config.FilesAreMonotonic() {
		r.state.Path = ""
		r.state.Complete = false
	}

	// Write a final checkpoint to mark the completion of the sweep.
	if err := r.commitLines(nil); err != nil {
		return err
	}

	r.log("completed sweep of %q through modification time %s", r.prefix, r.state.MinMod)
	return nil
}

func (r *reader) log(msg string, args ...interface{}) {
	r.shared.mu.Lock()
	defer r.shared.mu.Unlock()
	_ = r.shared.enc.Encode(airbyte.NewLogMessage(airbyte.LogLevelInfo, msg, args...))
}

func (r *reader) shouldSkip(obj ObjectInfo) (_ bool, reason string) {
	// Have we already processed through this filename in this sweep?
	if r.state.Path > obj.Path {
		return true, "state.Path > obj.Path"
	}
	if r.state.Path == obj.Path && r.state.Complete {
		return true, "state.Path == obj.Path && Complete"
	}
	// Is the path excluded by our regex ?
	if r.pathRe != nil && !r.pathRe.MatchString(obj.Path) {
		return true, "regex not matched"
	}
	// Is the path modified before our window (inclusive) ?
	if r.state.MinMod.After(obj.ModTime) {
		return true, "MinMod.After(obj.ModTime)"
	}
	// Is the path modified after our window (exclusive) ?
	if r.state.MaxMod.Before(obj.ModTime) {
		return true, "MaxMod.After(obj.ModTime)"
	}

	return false, ""
}

func (r *reader) processObject(ctx context.Context, obj ObjectInfo) error {
	rr, obj, err := r.store.Read(ctx, obj)
	if err != nil {
		return err
	}
	defer rr.Close()

	r.log("processing file %q", obj.Path)

	tmp, err := ioutil.TempFile("", "parser-config-*.json")
	if err != nil {
		return fmt.Errorf("creating parser config: %w", err)
	}
	defer os.Remove(tmp.Name())

	if err = r.makeParseConfig(obj).WriteToFile(tmp); err != nil {
		return fmt.Errorf("writing parser config: %w", err)
	}

	// Skip tracks records remaining to skip, because they've already
	// been ingested in a prior invocation of this capture.
	var skip int

	if r.state.Path == obj.Path {
		if r.state.Complete {
			panic("should have been filtered already")
		}
		skip = r.state.Records
	}

	r.state.Path = obj.Path
	r.state.Records = 0
	r.state.Complete = false

	err = parser.ParseStream(ctx, tmp.Name(), rr, func(lines []json.RawMessage) error {
		r.state.Records += len(lines)

		if ll := len(lines); ll < skip {
			skip -= ll
			return nil
		} else if skip != 0 {
			lines = lines[skip:]
			skip = 0
		}

		return r.commitLines(lines)
	})
	if err != nil {
		return err
	}

	// Write a final checkpoint to mark the completion of the file.
	r.state.Complete = true
	r.state.Records = 0

	if err := r.commitLines(nil); err != nil {
		return err
	}

	return nil
}

func (r *reader) makeParseConfig(obj ObjectInfo) *parser.Config {
	var cfg = new(parser.Config)
	if c := r.config.ParserConfig(); c != nil {
		*cfg = c.Copy()
	}

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
	if cfg.Schema == nil {
		cfg.Schema = r.schema
	}
	cfg.Projections = r.projections

	cfg.AddValues[metaFileLocation] = obj.Path
	return cfg
}

func (r *reader) commitLines(lines []json.RawMessage) error {
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
			Data prefixStates
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

const discoverListLimit = 256
const metaFileLocation = "/_meta/file"
const metaOffsetLocation = "/_meta/offset"

const documentSchema = `
{
	"type": "object",
	"properties": {
		"_meta": {
			"type": "object",
			"properties": {
				"file": { "type": "string" },
				"offset": {
					"type": "integer",
					"minimum": 0
				}
			},
			"required": ["file", "offset"]
		}
	},
	"required": ["_meta"]
}
`
