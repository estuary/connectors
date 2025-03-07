package connector

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

var _ boilerplate.MaterializerTransactor = (*transactor)(nil)

type binding struct {
	Idx    int
	Mapped *boilerplate.MappedBinding[config, resource, mapped]
	load   struct {
		keys        []python.NestedField
		mergeBounds *mergeBoundsBuilder
	}
	store struct {
		columns     []python.NestedField
		mergeBounds *mergeBoundsBuilder
	}
}

type transactor struct {
	cp                  map[string]*python.MergeBinding
	recovery            bool
	materializationName string

	be        *boilerplate.BindingEvents
	cfg       config
	s3Client  *s3.Client
	emrClient *emr.Client

	templates  templates
	bindings   []binding
	loadFiles  *boilerplate.StagedFiles
	storeFiles *boilerplate.StagedFiles

	emrAuth struct {
		credentialSecretName string
		scope                string
	}

	pyFiles struct {
		commonURI string
		loadURI   string
		mergeURI  string
	}
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (boilerplate.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(raw json.RawMessage) error {
	t.cp = make(map[string]*python.MergeBinding)
	if err := json.Unmarshal(raw, &t.cp); err != nil {
		return err
	}
	t.recovery = true

	return nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	ctx := it.Context()

	hadLoads := false
	for it.Next() {
		hadLoads = true
		b := t.bindings[it.Binding]

		if converted, err := b.Mapped.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = t.loadFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("encoding Load key: %w", err)
		} else {
			b.load.mergeBounds.nextKey(converted)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !hadLoads {
		return nil
	}

	defer t.loadFiles.CleanupCurrentTransaction(ctx)

	var pyBindings []python.LoadBinding
	var unionQueries []string
	for _, b := range t.bindings {
		if !t.loadFiles.Started(b.Idx) {
			continue
		}

		files, err := t.loadFiles.Flush(b.Idx)
		if err != nil {
			return fmt.Errorf("flushing load files: %w", err)
		}

		pyBindings = append(pyBindings, python.LoadBinding{
			Binding: b.Idx,
			Keys:    b.load.keys,
			Files:   files,
		})

		var subQuery strings.Builder
		if err := t.templates.loadQuery.Execute(&subQuery, templateInput{
			binding: b,
			Bounds:  b.load.mergeBounds.build(),
		}); err != nil {
			return fmt.Errorf("loadQuery template: %w", err)
		}
		unionQueries = append(unionQueries, subQuery.String())
	}

	outputPrefix := path.Join(t.cfg.Compute.BucketPath, uuid.NewString())
	loadInput := python.LoadInput{
		Query:          strings.Join(unionQueries, "\nUNION ALL\n"),
		Bindings:       pyBindings,
		OutputLocation: "s3://" + path.Join(t.cfg.Compute.Bucket, outputPrefix),
	}

	// In addition to removing the staged load keys, the loaded document result
	// files that are written to the staging location must also be removed.
	cleanupResults := t.cleanPrefixOnceFn(ctx, t.cfg.Compute.Bucket, outputPrefix)
	defer cleanupResults()

	t.be.StartedEvaluatingLoads()
	if err := t.runEmrJob(ctx, fmt.Sprintf("load for: %s", t.materializationName), loadInput, outputPrefix, t.pyFiles.loadURI); err != nil {
		return fmt.Errorf("load job failed: %w", err)
	} else if err := t.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up load files: %w", err)
	}
	t.be.FinishedEvaluatingLoads()

	if err := t.readLoadResults(ctx, outputPrefix, loaded); err != nil {
		return fmt.Errorf("reading load results: %w", err)
	} else if err := cleanupResults(); err != nil {
		return fmt.Errorf("cleaning up generated load result files: %w", err)
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	for it.Next() {
		if t.cfg.HardDelete && it.Delete && !it.Exists {
			continue
		}

		b := t.bindings[it.Binding]

		flowDocument := it.RawJSON
		if t.cfg.HardDelete && it.Delete {
			flowDocument = json.RawMessage(`"delete"`)
		}

		if converted, err := b.Mapped.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := t.storeFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		} else {
			b.store.mergeBounds.nextKey(converted[:len(b.Mapped.Keys)])
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		pyBindings := make(map[string]*python.MergeBinding)
		for idx, b := range t.bindings {
			if !t.storeFiles.Started(idx) {
				continue
			}

			files, err := t.storeFiles.Flush(idx)
			if err != nil {
				return nil, m.FinishedOperation(fmt.Errorf("flushing store files: %w", err))
			}

			var mergeQuery strings.Builder
			if err := t.templates.mergeQuery.Execute(&mergeQuery, templateInput{
				binding: b,
				Bounds:  b.store.mergeBounds.build(),
			}); err != nil {
				return nil, m.FinishedOperation(fmt.Errorf("rendering mergeQuery template: %w", err))
			}

			pyBindings[b.Mapped.StateKey] = &python.MergeBinding{
				Binding: idx,
				Query:   mergeQuery.String(),
				Columns: b.store.columns,
				Files:   files,
			}
		}

		cpUpdate, err := json.Marshal(pyBindings)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("encoding checkpoint update: %w", err))
		}

		for sk, binding := range pyBindings {
			t.cp[sk] = binding
		}

		return &pf.ConnectorState{UpdatedJson: cpUpdate, MergePatch: true}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	outputPrefix := path.Join(t.cfg.Compute.BucketPath, uuid.NewString())
	checkpointClear := make(map[string]*python.MergeBinding)
	var mergeInput python.MergeInput
	var allFileUris []string

	for _, b := range t.bindings {
		sk := b.Mapped.StateKey
		if pyMergeBinding, ok := t.cp[sk]; ok {
			delete(t.cp, sk)
			checkpointClear[sk] = nil

			if t.recovery {
				extantFiles, err := t.extantFiles(ctx, pyMergeBinding.Files)
				if err != nil {
					return nil, fmt.Errorf("listing previously staged files for recovery transaction: %w", err)
				}

				for idx, checkpointUri := range pyMergeBinding.Files {
					if _, ok := extantFiles[checkpointUri]; !ok {
						log.WithFields(log.Fields{
							"file":    checkpointUri,
							"binding": b.Mapped.StateKey,
						}).Info("previously checkpointed file no longer exists")
						pyMergeBinding.Files = slices.Delete(pyMergeBinding.Files, idx, idx+1)
					}
				}

				if len(pyMergeBinding.Files) == 0 {
					log.WithFields(log.Fields{
						"binding": b.Mapped.StateKey,
					}).Info("no checkpointed files remain, skipping recovery merge")
					continue
				}
			}

			mergeInput.Bindings = append(mergeInput.Bindings, *pyMergeBinding)
			allFileUris = append(allFileUris, pyMergeBinding.Files...)
		}
	}

	var stateUpdate *pf.ConnectorState
	if len(mergeInput.Bindings) > 0 {
		// Make sure the job status output file gets cleaned up.
		cleanupStatus := t.cleanPrefixOnceFn(ctx, t.cfg.Compute.Bucket, outputPrefix)
		defer cleanupStatus()

		if err := t.runEmrJob(ctx, fmt.Sprintf("store for: %s", t.materializationName), mergeInput, outputPrefix, t.pyFiles.mergeURI); err != nil {
			return nil, fmt.Errorf("store merge job failed: %w", err)
		} else if err := cleanupStatus(); err != nil {
			return nil, fmt.Errorf("cleaning up generated job status file: %w", err)
		} else if err := t.storeFiles.CleanupCheckpoint(ctx, allFileUris); err != nil {
			return nil, fmt.Errorf("cleaning up store files: %w", err)
		} else if cpUpdate, err := json.Marshal(checkpointClear); err != nil {
			return nil, fmt.Errorf("encoding checkpoint update: %w", err)
		} else {
			stateUpdate = &pf.ConnectorState{
				UpdatedJson: cpUpdate,
				MergePatch:  true,
			}
		}
	}

	t.recovery = false
	return stateUpdate, nil
}

func (t *transactor) Destroy() {}

func (t *transactor) readLoadResults(ctx context.Context, prefix string, loaded func(binding int, doc json.RawMessage) error) error {
	var mu sync.Mutex
	lockedAndLoaded := func(binding int, doc json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()

		return loaded(binding, doc)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	readFileKeys := make(chan string)

	for idx := 0; idx < loadFileWorkers; idx++ {
		group.Go(func() error { return t.loadWorker(groupCtx, lockedAndLoaded, readFileKeys) })
	}

	paginator := s3.NewListObjectsV2Paginator(t.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.cfg.Compute.Bucket),
		Prefix: aws.String(prefix),
	})

	var sawSuccess bool
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}

		for _, obj := range page.Contents {
			base := path.Base(*obj.Key)

			// Sanity checks for the general layout of files we expect. This
			// shouldn't strictly be necessary, although we do need to not try
			// to parse loaded documents from the Spark _SUCCESS file or our own
			// status file.
			if base == "_SUCCESS" && !sawSuccess {
				sawSuccess = true
				continue
			} else if base == "_SUCCESS" {
				return fmt.Errorf("application error: multiple _SUCCESS files")
			} else if !sawSuccess {
				return fmt.Errorf("application error: missing _SUCCESS file, got key %q", *obj.Key)
			} else if base == statusFile {
				continue
			} else if !strings.HasPrefix(base, "part-") {
				return fmt.Errorf("application error: unexpected key %q", *obj.Key)
			}

			select {
			case <-groupCtx.Done():
				return group.Wait()
			case readFileKeys <- *obj.Key:
				continue
			}
		}
	}

	close(readFileKeys)
	return group.Wait()
}

func (t *transactor) loadWorker(ctx context.Context, loaded func(binding int, doc json.RawMessage) error, fileKeys <-chan string) error {
	for fileKey := range fileKeys {
		obj, err := t.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Compute.Bucket),
			Key:    aws.String(fileKey),
		})
		if err != nil {
			return err
		}

		// Process the raw bytes of the object, which represent loaded
		// documents. This relies on the generated files to be in a particular
		// format, which isn't quite CSV where each line has:
		//
		// 1) An integer for the binding number
		// 2) A comma
		// 3) A single character "quote", which is somewhat arbitrarily chosen
		// as a '|'
		// 4) Some number of bytes which are assumed to be a valid JSON object,
		// which is the loaded document
		// 5) A final single character "quote"
		//
		// This format is generated by the "load" EMR job, by setting quote="|"
		// and escapeQuotes=False.
		gzr, err := gzip.NewReader(obj.Body)
		if err != nil {
			return fmt.Errorf("get gzip reader for load results: %w", err)
		}

		scanner := bufio.NewScanner(gzr)
		for scanner.Scan() {
			line := scanner.Bytes()
			if commaIdx := bytes.IndexByte(line, ','); commaIdx == -1 {
				return fmt.Errorf("could not find comma in load binding line: %s", line)
			} else if binding, err := strconv.Atoi(string(line[:commaIdx])); err != nil {
				return fmt.Errorf("parsing binding index number: %w", err)
			} else if err := loaded(binding, line[commaIdx+2:len(line)-1]); err != nil {
				return err
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		} else if err := gzr.Close(); err != nil {
			return fmt.Errorf("closing gzip reader: %w", err)
		} else if err := obj.Body.Close(); err != nil {
			return fmt.Errorf("closing body: %w", err)
		}
	}

	return nil
}

func (t *transactor) cleanPrefixOnceFn(ctx context.Context, bucket string, prefix string) func() error {
	didClean := false
	return func() error {
		if didClean {
			return nil
		}

		paginator := s3.NewListObjectsV2Paginator(t.s3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		})
		if !paginator.HasMorePages() {
			log.WithFields(log.Fields{
				"bucket": bucket,
				"prefix": prefix,
			}).Warn("called cleanPrefixOnceFn on an empty prefix")
		}

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to get next page: %w", err)
			}

			thisPage := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
			for _, obj := range page.Contents {
				thisPage = append(thisPage, s3types.ObjectIdentifier{Key: obj.Key})
			}

			if _, err := t.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3types.Delete{Objects: thisPage},
			}); err != nil {
				return err
			}

		}
		didClean = true
		return nil
	}
}

func (t *transactor) extantFiles(ctx context.Context, originalFileUris []string) (map[string]struct{}, error) {
	var prefix string
	for _, uri := range originalFileUris {
		_, key := s3UriToParts(uri)
		this := path.Dir(key)
		if prefix == "" {
			prefix = this
		} else if prefix != this {
			return nil, fmt.Errorf("files have different prefixes: %q and %q", prefix, this)
		}
	}

	out := make(map[string]struct{})

	paginator := s3.NewListObjectsV2Paginator(t.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.cfg.Compute.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page: %w", err)
		}

		for _, obj := range page.Contents {
			out[fmt.Sprintf("s3://%s/%s", t.cfg.Compute.Bucket, *obj.Key)] = struct{}{}
		}
	}

	return out, nil

}
