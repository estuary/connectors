package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/estuary/protocols/fdb/tuple"
	"github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// A mock fileProcessor that implements FileProcessor interface.
type MockFileProcessor struct {
	StoreTimes         int
	CommitTimes        int
	DestroyTimes       int
	MockNextSeqNumList []int
	DestroyError       error
}

func newMockFileProcessor(mockNextSeqNumList []int) *MockFileProcessor {
	return &MockFileProcessor{
		StoreTimes:         0,
		CommitTimes:        0,
		DestroyTimes:       0,
		MockNextSeqNumList: mockNextSeqNumList,
		DestroyError:       nil,
	}
}
func (mfp *MockFileProcessor) Store(binding int, key tuple.Tuple, values tuple.Tuple) error {
	mfp.StoreTimes++
	return nil
}
func (mfp *MockFileProcessor) Commit() ([]int, error) {
	mfp.CommitTimes++
	return mfp.MockNextSeqNumList, nil
}
func (mfp *MockFileProcessor) Destroy() error {
	mfp.DestroyTimes++
	return mfp.DestroyError
}

// Structure of test data for MockS3Uploader.
type TestData struct {
	Id      int64
	Message string
}

// MockS3Uploader implements the Uploader interface.
type MockS3Uploader struct {
	t        *testing.T
	contents map[string][]TestData
}

func (u *MockS3Uploader) Upload(bucket, key, localFileName string) error {
	fr, err := local.NewLocalFileReader(localFileName)
	require.NoError(u.t, err)
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	require.NoError(u.t, err)
	defer pr.ReadStop()

	var num = int(pr.GetNumRows())
	var data = make([]TestData, num)
	err = pr.Read(&data)
	require.NoError(u.t, err)

	// Instead of uploading to the cloud, MockS3Uploader records the uploaded contents in memory.
	u.contents[fmt.Sprintf("s3://%s/%s", bucket, key)] = data

	return nil
}
func newMockS3Uploader(t *testing.T) *MockS3Uploader {
	return &MockS3Uploader{t: t, contents: make(map[string][]TestData)}
}

func TestFileProcessorProxy_APIs(t *testing.T) {
	var mockClock = clock.NewMock()
	var ctx = context.Background()

	var mockFileProcessor = newMockFileProcessor([]int{1, 2, 3})
	// Test returns fileProcessor before cancel.
	var proxy = NewFileProcessorProxy(ctx, mockFileProcessor, time.Second*2, mockClock)
	mockClock.Add(time.Second)

	proxy.Store(0, tuple.Tuple{}, tuple.Tuple{})
	var nextSeqNumList, _ = proxy.Commit()
	require.Equal(t, []int{1, 2, 3}, nextSeqNumList)
	require.NoError(t, proxy.Destroy())

	require.Equal(t, 1, mockFileProcessor.StoreTimes)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)
	require.Equal(t, 1, mockFileProcessor.DestroyTimes)
}

func TestFileProcessorProxy_cancelByCtx(t *testing.T) {
	var mockClock = clock.NewMock()
	var ctx, cancel = context.WithCancel(context.Background())
	var mockFileProcessor = newMockFileProcessor(nil)
	// acquireFileProcessor succeeds before cancel.
	var proxy = NewFileProcessorProxy(ctx, mockFileProcessor, 2*time.Second, mockClock)
	var fp = proxy.acquireFileProcessor()
	require.NotNil(t, fp)
	proxy.returnFileProcessor(fp)

	proxy.Store(0, tuple.Tuple{"key1"}, tuple.Tuple{"value1"})
	cancel()
	require.NoError(t, <-proxy.errorCh)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// The fileProcessor is no longer available after cancel.
	require.Panics(t, func() { proxy.acquireFileProcessor() })
}

func TestFileProcessorProxy_DestroyNoError(t *testing.T) {
	var mockClock = clock.NewMock()
	var mockFileProcessor = newMockFileProcessor(nil)

	// acquireFileProcessor succeeds before destroy.
	var proxy = NewFileProcessorProxy(context.Background(), mockFileProcessor, 2*time.Second, mockClock)
	var fp = proxy.acquireFileProcessor()
	require.NotNil(t, fp)
	proxy.returnFileProcessor(fp)

	proxy.Store(0, tuple.Tuple{"key1"}, tuple.Tuple{"value1"})
	require.Nil(t, proxy.Destroy())
	require.Equal(t, 1, mockFileProcessor.DestroyTimes)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// The fileProcessor is no longer avaliable after destroy.
	require.Panics(t, func() { proxy.acquireFileProcessor() })
}

func TestFileProcessorProxy_DestroyWithError(t *testing.T) {
	var mockClock = clock.NewMock()
	var mockFileProcessor = newMockFileProcessor(nil)
	mockFileProcessor.DestroyError = fmt.Errorf("testing error")

	// acquireFileProcessor succeeds before destroy.
	var proxy = NewFileProcessorProxy(context.Background(), mockFileProcessor, 2*time.Second, mockClock)

	var fp = proxy.acquireFileProcessor()
	require.NotNil(t, fp)
	proxy.returnFileProcessor(fp)

	var err = proxy.Destroy()
	require.Equal(t, mockFileProcessor.DestroyError, err)

	// The fileProcessor is no longer avaliable after destroy.
	require.Panics(t, func() { proxy.acquireFileProcessor() })
}

func TestFileProcessorProxy_AcquireAndReturnProcessor(t *testing.T) {
	var mockClock = clock.NewMock()
	var ctx = context.Background()
	var mockFileProcessor = newMockFileProcessor(nil)
	var proxy = NewFileProcessorProxy(ctx, mockFileProcessor, 2*time.Second, mockClock)
	mockClock.Add(time.Second)
	defer func() { require.NoError(t, proxy.Destroy()) }()

	// acquires the processor successfully.
	var p = proxy.acquireFileProcessor().(*MockFileProcessor)
	require.NotNil(t, p)

	// Go-routine is blocked when trying acquire the processor again.
	var tmpCh = make(chan struct{})
	go func() {
		var tmpFp = proxy.acquireFileProcessor()
		defer proxy.returnFileProcessor(tmpFp)
		close(tmpCh)
	}()

	select {
	case <-tmpCh:
		require.FailNow(t, "no file processor should be acquired before returning.")
	case <-time.After(100 * time.Millisecond):
		// Fallthrough expected
	}

	// returns the processor.
	proxy.returnFileProcessor(p)
	p = nil

	// acquires the processor successfully again.
	p = proxy.acquireFileProcessor().(*MockFileProcessor)
	require.NotNil(t, p)

	// returns the processor.
	proxy.returnFileProcessor(p)
}

func TestFileProcessorProxy_TimerTriggered(t *testing.T) {
	var mockClock = clock.NewMock()
	var ctx = context.Background()
	var mockFileProcessor = newMockFileProcessor(nil)
	var proxy = NewFileProcessorProxy(ctx, mockFileProcessor, 5*time.Second, mockClock)
	mockClock.Add(time.Second)
	defer func() { require.NoError(t, proxy.Destroy()) }()

	require.Equal(t, 0, mockFileProcessor.CommitTimes)
	// No force commit is triggered before `Store` is called.
	mockClock.Add(5 * time.Second)
	require.Equal(t, 0, mockFileProcessor.CommitTimes)

	// Force commit is triggered after `Store` is called.
	proxy.Store(0, tuple.Tuple{"key1"}, tuple.Tuple{"value1"})
	// Make sure mockClock.Add is called after fp.timer.Reset(fp.forceUploadInterval).
	time.Sleep(10 * time.Millisecond)
	// Wait time is not enough.
	mockClock.Add(3 * time.Second)
	require.Equal(t, 0, mockFileProcessor.CommitTimes)
	// And after the wait interval expires.
	mockClock.Add(2 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// It triggers only once until after the next time `Store` is called.
	mockClock.Add(5 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// When the file processor is acquired, no force commit is triggered.
	proxy.Store(0, tuple.Tuple{"key2"}, tuple.Tuple{"value2"})
	p := proxy.acquireFileProcessor().(*MockFileProcessor)
	// Make sure mockClock.Add is called after fp.timer.Reset(fp.forceUploadInterval).
	time.Sleep(10 * time.Millisecond)

	mockClock.Add(5 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)
	mockClock.Add(5 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// The force commit resumes after processor is returned with enough waiting time.
	proxy.returnFileProcessor(p)
	// Make sure mockClock.Add is called after fp.timer.Reset(fp.forceUploadInterval).
	time.Sleep(10 * time.Millisecond)

	require.Equal(t, 1, mockFileProcessor.CommitTimes)
	mockClock.Add(5 * time.Second)
	require.Equal(t, 2, mockFileProcessor.CommitTimes)
	mockClock.Add(5 * time.Second)
	require.Equal(t, 2, mockFileProcessor.CommitTimes)
}

func TestParquetFileProcessor_NoBinding(t *testing.T) {
	var mockS3Uploader = newMockS3Uploader(t)
	var open = &pm.TransactionRequest_Open{Materialization: &flow.MaterializationSpec{
		Materialization: flow.Materialization("test_materialization"),
	}}

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, nil, open)
	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{}, nextSeqNumList)
}

func buildTestOpenRequest(numOfBindings int) *pm.TransactionRequest_Open {
	var bindings = make([]*flow.MaterializationSpec_Binding, 0, numOfBindings)
	for i := 0; i < numOfBindings; i++ {
		resourceSpecJSON, _ := json.Marshal(
			resource{
				Bucket:         "test_bucket",
				PathPrefix:     fmt.Sprintf("test_path_%d", i),
				ParallelNumber: 4,
			},
		)
		var binding = &flow.MaterializationSpec_Binding{
			ResourceSpecJson: resourceSpecJSON,
			FieldSelection:   flow.FieldSelection{Keys: []string{"Id"}, Values: []string{"Message"}},
			Collection: flow.CollectionSpec{Projections: []flow.Projection{
				{Field: "Id", Inference: flow.Inference{Types: []string{"integer"}, MustExist: true}},
				{Field: "Message", Inference: flow.Inference{Types: []string{"string"}, MustExist: true}},
			}},
		}

		bindings = append(bindings, binding)
	}
	return &pm.TransactionRequest_Open{
		Materialization: &flow.MaterializationSpec{
			Materialization: flow.Materialization("test_materialization"),
			Bindings:        bindings,
		},
		KeyBegin: 123,
		KeyEnd:   456,
	}
}

func TestParquetFileProcessor_SingleBinding(t *testing.T) {
	var mockS3Uploader = newMockS3Uploader(t)
	var open = buildTestOpenRequest(1)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, nil, open)

	require.Equal(t, []int{0}, fileProcessor.nextSeqNumList())

	fileProcessor.Store(0, tuple.Tuple{1}, tuple.Tuple{"msg #1"})
	fileProcessor.Store(0, tuple.Tuple{1}, tuple.Tuple{"msg #1"})
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, _ := fileProcessor.Commit()
	var expectedA = []TestData{{Id: 1, Message: "msg #1"}, {Id: 1, Message: "msg #1"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_0.pq"])
	require.Equal(t, []int{1}, nextSeqNumList)

	fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"})
	nextSeqNumList, _ = fileProcessor.Commit()
	var expectedB = []TestData{{Id: 2, Message: "msg #2"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_0.pq"])
	require.Equal(t, expectedB, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_1.pq"])
	require.Equal(t, []int{2}, nextSeqNumList)

	// The process resumed with nextSeqNum being 1 to override the previous file.
	fileProcessor, _ = NewParquetFileProcessor(context.Background(), mockS3Uploader, []int{1}, open)
	fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"})
	fileProcessor.Store(0, tuple.Tuple{3}, tuple.Tuple{"msg #3"})
	nextSeqNumList, _ = fileProcessor.Commit()
	var expectedC = []TestData{{Id: 2, Message: "msg #2"}, {Id: 3, Message: "msg #3"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_0.pq"])
	require.Equal(t, expectedC, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_1.pq"])
	require.Equal(t, []int{2}, nextSeqNumList)
}

func TestParquetFileProcessor_MultipleBindings(t *testing.T) {
	var mockS3Uploader = newMockS3Uploader(t)
	var open = buildTestOpenRequest(4)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, []int{1, 2, 3, 4}, open)
	require.Equal(t, []int{1, 2, 3, 4}, fileProcessor.nextSeqNumList())

	fileProcessor.Store(0, tuple.Tuple{0}, tuple.Tuple{"msg #0"})
	fileProcessor.Store(1, tuple.Tuple{1}, tuple.Tuple{"msg #1"})
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, _ := fileProcessor.Commit()
	require.Equal(t, []int{2, 3, 3, 4}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_1.pq"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/123_456_2.pq"])

	fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"})
	fileProcessor.Store(1, tuple.Tuple{3}, tuple.Tuple{"msg #3"})
	fileProcessor.Store(2, tuple.Tuple{4}, tuple.Tuple{"msg #4"})
	fileProcessor.Store(3, tuple.Tuple{5}, tuple.Tuple{"msg #5"})

	nextSeqNumList, _ = fileProcessor.Commit()
	require.Equal(t, []int{3, 4, 4, 5}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_1.pq"])
	require.Equal(t, []TestData{{Id: 2, Message: "msg #2"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/123_456_2.pq"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/123_456_2.pq"])
	require.Equal(t, []TestData{{Id: 3, Message: "msg #3"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/123_456_3.pq"])
	require.Equal(t, []TestData{{Id: 4, Message: "msg #4"}}, mockS3Uploader.contents["s3://test_bucket/test_path_2/123_456_3.pq"])
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5"}}, mockS3Uploader.contents["s3://test_bucket/test_path_3/123_456_4.pq"])
}
