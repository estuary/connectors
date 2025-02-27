package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
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

const (
	// The number of concurrent workers reading from S3 objects that contain
	// loaded document data. It probably won't take many of these to max out the
	// connector CPU.
	loadFileWorkers = 3
)

var _ boilerplate.MaterializerTransactor = (*transactor)(nil)

type binding struct {
	Idx    int
	Mapped *boilerplate.MappedBinding[mapped, resource]
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
	cp       map[string]*python.MergeBinding
	recovery bool

	be        *boilerplate.BindingEvents
	cfg       config
	s3Client  *s3.Client
	emrClient *emr.Client

	templates  templates
	bindings   []binding
	loadFiles  *boilerplate.StagedFiles
	storeFiles *boilerplate.StagedFiles

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

	t.be.StartedEvaluatingLoads()
	if input, err := encodeInput(loadInput); err != nil {
		return fmt.Errorf("encoding load input: %w", err)
	} else if err := t.runEmrJob(ctx, "load", input, outputPrefix, t.pyFiles.loadURI); err != nil {
		return fmt.Errorf("load job failed: %w", err)
	}
	t.be.FinishedEvaluatingLoads()

	if err := t.readLoadResults(ctx, outputPrefix, loaded); err != nil {
		return fmt.Errorf("reading load results: %w", err)
	} else if err := t.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up load files: %w", err)
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
	mergeInput := python.MergeInput{OutputLocation: "s3://" + path.Join(t.cfg.Compute.Bucket, outputPrefix)}
	var allFileUris []string

	for _, b := range t.bindings {
		sk := b.Mapped.StateKey
		if pyMergeBinding, ok := t.cp[sk]; ok {
			t.cp[sk] = nil
			checkpointClear[sk] = nil

			if t.recovery {
				prefix, err := commonPrefix(pyMergeBinding.Files)
				if err != nil {
					return nil, fmt.Errorf("computing common prefix: %w", err)
				}

				extantFiles, err := t.filesInPrefix(ctx, prefix)
				if err != nil {
					return nil, fmt.Errorf("listing files in prefix: %w", err)
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
					}).Info("no checkpointed files remain, skipping merge")
					continue
				}
			}

			mergeInput.Bindings = append(mergeInput.Bindings, *pyMergeBinding)
			allFileUris = append(allFileUris, pyMergeBinding.Files...)
		}
	}

	var stateUpdate *pf.ConnectorState
	if len(mergeInput.Bindings) > 0 {
		stateUpdate = &pf.ConnectorState{MergePatch: true}
		if input, err := encodeInput(mergeInput); err != nil {
			return nil, fmt.Errorf("encoding store merge input: %w", err)
		} else if err := t.runEmrJob(ctx, "store merge", input, outputPrefix, t.pyFiles.mergeURI); err != nil {
			return nil, fmt.Errorf("store merge job failed: %w", err)
		} else if err := t.storeFiles.CleanupCheckpoint(ctx, allFileUris); err != nil {
			return nil, fmt.Errorf("cleaning up store files: %w", err)
		} else if cpUpdate, err := json.Marshal(checkpointClear); err != nil {
			return nil, fmt.Errorf("encoding checkpoint update: %w", err)
		} else {
			stateUpdate.UpdatedJson = cpUpdate
		}
	}

	t.recovery = false
	return stateUpdate, nil
}

func (t *transactor) Destroy() {
	var toDelete []s3types.ObjectIdentifier

	for _, uri := range []string{t.pyFiles.commonURI, t.pyFiles.loadURI, t.pyFiles.mergeURI} {
		_, key := s3UriToParts(uri)
		toDelete = append(toDelete, s3types.ObjectIdentifier{Key: aws.String(key)})
	}

	_, err := t.s3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(t.cfg.Compute.Bucket),
		Delete: &s3types.Delete{
			Objects: toDelete,
		},
	})

	if err != nil {
		log.WithError(err).Error("failed to clean up transactor artifacts")
	}
}

func encodeInput(in any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetAppendNewline(false)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(in); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (t *transactor) runEmrJob(ctx context.Context, name, input, statusOutputPrefix, entryPointUri string) error {
	/***
	Available arguments to the pyspark script:
	| --input                  | Input for the program, as serialized JSON                              | Required |
	| --status-output          | Location where the final status object will be written.                | Required |
	| --catalog-url            | The catalog URL                                                        | Required |
	| --warehouse              | REST Warehouse                                                         | Required |
	| --region                 | EMR & SigV4 Region                                                     | Required |
	| --credential-secret-name | Name of the secret in Secrets Manager if using client credentials auth | Optional |
	| --scope                  | Scope if using client credentials auth                                 | Optional |
	***/
	getStatus := func() (*python.StatusOutput, error) {
		statusObj, err := t.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Compute.Bucket),
			Key:    aws.String(path.Join(statusOutputPrefix, "status.json")),
		})
		if err != nil {
			return nil, err
		}

		var status python.StatusOutput
		if err := json.NewDecoder(statusObj.Body).Decode(&status); err != nil {
			return nil, err
		}

		return &status, nil
	}

	args := []string{
		"--input", input,
		"--status-output", "s3://" + path.Join(t.cfg.Compute.Bucket, statusOutputPrefix, "status.json"),
		"--catalog-url", t.cfg.URL,
		"--warehouse", t.cfg.Warehouse,
		"--region", t.cfg.Compute.Region,
	}

	if n := t.cfg.Compute.emrConfig.EmrCatalogAuthentication.emrAuthClientCredentialConfig.CredentialSecretName; n != "" {
		args = append(args, "--credential-secret-name", n)
	}
	if s := t.cfg.Compute.emrConfig.EmrCatalogAuthentication.emrAuthClientCredentialConfig.Scope; s != "" {
		args = append(args, "--scope", s)
	}

	start, err := t.emrClient.StartJobRun(ctx, &emr.StartJobRunInput{
		ApplicationId:    aws.String(t.cfg.Compute.ApplicationId),
		ClientToken:      aws.String(uuid.NewString()),
		ExecutionRoleArn: aws.String(t.cfg.Compute.ExecutionRoleArn),
		JobDriver: &types.JobDriverMemberSparkSubmit{
			Value: types.SparkSubmit{
				SparkSubmitParameters: aws.String(fmt.Sprintf("--py-files %s", t.pyFiles.commonURI)),
				EntryPoint:            aws.String(entryPointUri),
				EntryPointArguments:   args,
			},
		},
		Name: aws.String(name),
	})
	if err != nil {
		return err
	}

	var runDetails string
	for {
		gotRun, err := t.emrClient.GetJobRun(ctx, &emr.GetJobRunInput{
			ApplicationId: aws.String(t.cfg.Compute.ApplicationId),
			JobRunId:      start.JobRunId,
		})
		if err != nil {
			return err
		}
		runDetails = *gotRun.JobRun.StateDetails

		switch gotRun.JobRun.State {
		case types.JobRunStateSuccess:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job succeeded but could not get final status: %w", err)
			} else if !status.Success {
				return fmt.Errorf("job failed ran successfully but had error: %s", status.Error)
			}

			return nil
		case types.JobRunStateFailed, types.JobRunStateCancelling, types.JobRunStateCancelled:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job failed with no status output: %s", runDetails)
			} else {
				log.WithField("runDetails", runDetails).Error("emr job failed")
				return fmt.Errorf("job failed: %s", status.Error)
			}
		default:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (t *transactor) readLoadResults(ctx context.Context, prefix string, loaded func(binding int, doc json.RawMessage) error) error {
	var mu sync.Mutex
	lockedAndLoaded := func(binding int, doc json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		log.WithFields(log.Fields{
			"binding": binding,
			"doc":     string(doc),
		}).Warn("loaded")

		return loaded(binding, doc)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	readFileKeys := make(chan string)

	for idx := 0; idx < loadFileWorkers; idx++ {
		group.Go(func() error { return t.loadWorker(groupCtx, lockedAndLoaded, readFileKeys) })
	}

	keys, pages, err := t.getLoadObjects(ctx, prefix)
	if err != nil {
		return err
	}

	var didDelete bool
	deletePages := func() error {
		if !didDelete {
			// Delete all generated objects in batches of 1000 since that is the
			// largest batch you can delete at once, which conveniently is also
			// the size of each page from ListObjects. Multiple pages of objects
			// could be deleted concurrently but it's rare that there will ever
			// be more than 1 page.
			didDelete = true
			for _, page := range pages {
				if _, err := t.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(t.cfg.Compute.Bucket),
					Delete: &s3types.Delete{Objects: page},
				}); err != nil {
					return fmt.Errorf("failed to delete objects: %w", err)
				}
			}
		}
		return nil
	}
	defer deletePages()

	for _, key := range keys {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case readFileKeys <- key:
			continue
		}
	}

	close(readFileKeys)
	if err := group.Wait(); err != nil {
		return err
	} else if err := deletePages(); err != nil {
		return err
	}

	return nil
}

func (t *transactor) getLoadObjects(ctx context.Context, prefix string) ([]string, [][]s3types.ObjectIdentifier, error) {
	var objKeys []string                      // relevant keys to be read
	var objPages [][]s3types.ObjectIdentifier // all the keys, organized as pages of 1000, to be scheduled for deletion

	paginator := s3.NewListObjectsV2Paginator(t.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.cfg.Compute.Bucket),
		Prefix: aws.String(prefix),
	})

	sawSuccess := false
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next page: %w", err)
		}

		thisPage := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			thisPage = append(thisPage, s3types.ObjectIdentifier{Key: obj.Key})
			base := path.Base(*obj.Key)

			// Sanity checks for the general layout of files we expect. This
			// isn't strictly necessary, although we do need to not try to parse
			// loaded documents from the Spark _SUCCESS file or our own status
			// file.
			if base == "_SUCCESS" && !sawSuccess {
				sawSuccess = true
				continue
			} else if base == "_SUCCESS" {
				return nil, nil, fmt.Errorf("application error: multiple _SUCCESS files")
			} else if !sawSuccess {
				return nil, nil, fmt.Errorf("application error: missing _SUCCESS file, got key %q", *obj.Key)
			} else if base == "status.json" {
				continue
			} else if !strings.HasPrefix(base, "part-") {
				return nil, nil, fmt.Errorf("application error: unexpected key %q", *obj.Key)
			}

			objKeys = append(objKeys, *obj.Key)
		}
		objPages = append(objPages, thisPage)
	}

	return objKeys, objPages, nil
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

		if err := processLoadObjBody(obj.Body, loaded); err != nil {
			return err
		}
	}

	return nil
}

// processLoadObjBody reads binding index integers and raw bytes representing
// JSON documents from a compressed stream. It relies on the generated files
// that contain the loaded document data to be in a particular format, which
// isn't quite CSV where each line has:
//
// 1) An integer for the binding number
// 2) A comma
// 3) A single character "quote", which is somewhat arbitrarily chosen as a '|'
// 4) Some number of bytes which are assumed to be a valid JSON object, which is
// the loaded document
// 5) A final single character "quote"
//
// This format is generated by the "load" EMR job, by setting quote="|" and
// escapeQuotes=False.
func processLoadObjBody(body io.ReadCloser, loaded func(binding int, doc json.RawMessage) error) error {
	gzr, err := gzip.NewReader(body)
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
	} else if err := body.Close(); err != nil {
		return fmt.Errorf("closing body: %w", err)
	}

	return nil
}

func (t *transactor) filesInPrefix(ctx context.Context, prefix string) (map[string]struct{}, error) {
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

func commonPrefix(uris []string) (string, error) {
	var out string
	for _, uri := range uris {
		_, key := s3UriToParts(uri)
		this := path.Dir(key)
		if out == "" {
			out = this
		} else if out != this {
			return "", fmt.Errorf("files have different prefixes: %q and %q", out, this)
		}
	}

	return out, nil
}
