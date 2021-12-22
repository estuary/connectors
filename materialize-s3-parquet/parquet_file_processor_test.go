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

func (u *MockS3Uploader) Upload(key, localFileName string, contentType string) error {
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
	u.contents[fmt.Sprintf("s3://test_bucket/%s", key)] = data

	return nil
}
func newMockS3Uploader(t *testing.T) *MockS3Uploader {
	return &MockS3Uploader{t: t, contents: make(map[string][]TestData)}
}

func TestFileProcessorProxy_APIs(t *testing.T) {
	var mockClock = clock.NewMock()
	var ctx = context.Background()

	var mockFileProcessor = newMockFileProcessor([]int{1, 2, 3})
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

	require.Equal(t, 0, mockFileProcessor.DestroyTimes)
	require.Equal(t, 0, mockFileProcessor.CommitTimes)
	proxy.Store(0, tuple.Tuple{"key1"}, tuple.Tuple{"value1"})
	cancel()
	require.NoError(t, <-proxy.errorCh)
	require.Equal(t, 1, mockFileProcessor.DestroyTimes)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)
}

func TestFileProcessorProxy_DestroyNoError(t *testing.T) {
	var mockClock = clock.NewMock()
	var mockFileProcessor = newMockFileProcessor(nil)

	// acquireFileProcessor succeeds before destroy.
	var proxy = NewFileProcessorProxy(context.Background(), mockFileProcessor, 2*time.Second, mockClock)
	require.Equal(t, 0, mockFileProcessor.DestroyTimes)
	require.Equal(t, 0, mockFileProcessor.CommitTimes)
	proxy.Store(0, tuple.Tuple{"key1"}, tuple.Tuple{"value1"})
	require.Nil(t, proxy.Destroy())
	require.Equal(t, 1, mockFileProcessor.DestroyTimes)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)
}

func TestFileProcessorProxy_DestroyWithError(t *testing.T) {
	var mockClock = clock.NewMock()
	var mockFileProcessor = newMockFileProcessor(nil)
	mockFileProcessor.DestroyError = fmt.Errorf("testing error")

	// acquireFileProcessor succeeds before destroy.
	var proxy = NewFileProcessorProxy(context.Background(), mockFileProcessor, 2*time.Second, mockClock)

	var err = proxy.Destroy()
	require.Equal(t, mockFileProcessor.DestroyError, err)
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
	// Wait time is not enough.
	mockClock.Add(3 * time.Second)
	require.Equal(t, 0, mockFileProcessor.CommitTimes)
	// And after the wait interval expires.
	mockClock.Add(2 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// It triggers only once until after the next time `Store` is called.
	mockClock.Add(5 * time.Second)
	require.Equal(t, 1, mockFileProcessor.CommitTimes)

	// It triggers again after `Store` is called and waited for enough long.
	proxy.Store(0, tuple.Tuple{"key2"}, tuple.Tuple{"value2"})
	require.Equal(t, 1, mockFileProcessor.CommitTimes)
	mockClock.Add(5 * time.Second)
	require.Equal(t, 2, mockFileProcessor.CommitTimes)
	mockClock.Add(5 * time.Second)
	require.Equal(t, 2, mockFileProcessor.CommitTimes)

	// It does not trigger `Commit` if not been waiting for long enough.
	proxy.Store(0, tuple.Tuple{"key3"}, tuple.Tuple{"value3"})
	mockClock.Add(4 * time.Second)
	require.Equal(t, 2, mockFileProcessor.CommitTimes)

	// After a commit call, it needs to wait for another 5 or more seconds for the next force commit.
	proxy.Commit()
	require.Equal(t, 3, mockFileProcessor.CommitTimes)
	proxy.Store(0, tuple.Tuple{"key4"}, tuple.Tuple{"value4"})
	mockClock.Add(4 * time.Second)
	require.Equal(t, 3, mockFileProcessor.CommitTimes)
	mockClock.Add(2 * time.Second)
	require.Equal(t, 4, mockFileProcessor.CommitTimes)
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
				PathPrefix: fmt.Sprintf("test_path_%d", i),
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

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{1}, tuple.Tuple{"msg #1"}))
	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{1}, tuple.Tuple{"msg #1"}))
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	var expectedA = []TestData{{Id: 1, Message: "msg #1"}, {Id: 1, Message: "msg #1"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000000.parquet"])
	require.Equal(t, []int{1}, nextSeqNumList)

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"}))
	nextSeqNumList, err = fileProcessor.Commit()
	require.NoError(t, err)
	var expectedB = []TestData{{Id: 2, Message: "msg #2"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000000.parquet"])
	require.Equal(t, expectedB, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000001.parquet"])
	require.Equal(t, []int{2}, nextSeqNumList)

	// The process resumed with nextSeqNum being 1 to override the previous file.
	fileProcessor, _ = NewParquetFileProcessor(context.Background(), mockS3Uploader, []int{1}, open)
	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"}))
	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{3}, tuple.Tuple{"msg #3"}))
	nextSeqNumList, err = fileProcessor.Commit()
	require.NoError(t, err)
	var expectedC = []TestData{{Id: 2, Message: "msg #2"}, {Id: 3, Message: "msg #3"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000000.parquet"])
	require.Equal(t, expectedC, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000001.parquet"])
	require.Equal(t, []int{2}, nextSeqNumList)
}

func TestParquetFileProcessor_MultipleBindings(t *testing.T) {
	var mockS3Uploader = newMockS3Uploader(t)
	var open = buildTestOpenRequest(4)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, []int{1, 2, 3, 4}, open)
	require.Equal(t, []int{1, 2, 3, 4}, fileProcessor.nextSeqNumList())

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{0}, tuple.Tuple{"msg #0"}))
	require.NoError(t, fileProcessor.Store(1, tuple.Tuple{1}, tuple.Tuple{"msg #1"}))
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{2, 3, 3, 4}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000001.parquet"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/0000007b/000000002.parquet"])

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{2}, tuple.Tuple{"msg #2"}))
	require.NoError(t, fileProcessor.Store(1, tuple.Tuple{3}, tuple.Tuple{"msg #3"}))
	require.NoError(t, fileProcessor.Store(2, tuple.Tuple{4}, tuple.Tuple{"msg #4"}))
	require.NoError(t, fileProcessor.Store(3, tuple.Tuple{5}, tuple.Tuple{"msg #5"}))

	nextSeqNumList, err = fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{3, 4, 4, 5}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000001.parquet"])
	require.Equal(t, []TestData{{Id: 2, Message: "msg #2"}}, mockS3Uploader.contents["s3://test_bucket/test_path_0/0000007b/000000002.parquet"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/0000007b/000000002.parquet"])
	require.Equal(t, []TestData{{Id: 3, Message: "msg #3"}}, mockS3Uploader.contents["s3://test_bucket/test_path_1/0000007b/000000003.parquet"])
	require.Equal(t, []TestData{{Id: 4, Message: "msg #4"}}, mockS3Uploader.contents["s3://test_bucket/test_path_2/0000007b/000000003.parquet"])
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5"}}, mockS3Uploader.contents["s3://test_bucket/test_path_3/0000007b/000000004.parquet"])
}
