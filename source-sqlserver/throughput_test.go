package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	broker_protocol "go.gazette.dev/core/broker/protocol"
	consumer_protocol "go.gazette.dev/core/consumer/protocol"
)

var benchmarkTableShapes = map[string]struct {
	Definition  string
	InsertQuery string
}{
	// A table with relatively small rows made of integers and text.
	"small_rows": {
		Definition: `(
			id BIGINT PRIMARY KEY,
			x INTEGER,
			y INTEGER,
			z INTEGER,
			a NVARCHAR(MAX),
			b NVARCHAR(MAX),
			c NVARCHAR(MAX)
		)`,
		InsertQuery: `
			WITH NumberSeries AS (
				SELECT %[2]d AS id
				UNION ALL
				SELECT id + 1 FROM NumberSeries WHERE id < %[3]d
			)
			INSERT INTO %[1]s (id, x, y, z, a, b, c)
			SELECT
				id,
				id %% 7777,
				id %% 131313,
				id %% 33333333,
				'text-' + CAST(id AS NVARCHAR(50)),
				'text-' + CAST((id + 1) AS NVARCHAR(50)),
				'text-' + CAST((id + 2) AS NVARCHAR(50))
			FROM NumberSeries
			OPTION (MAXRECURSION 0)
		`,
	},

	// A table with many different types of columns.
	"many_types": {
		Definition: `(
    	    id BIGINT PRIMARY KEY,
    	    small_int SMALLINT,
    	    normal_int INTEGER,
    	    big_int BIGINT,
    	    float_val FLOAT,
    	    decimal_val DECIMAL(10,2),
    	    text_val TEXT,
    	    varchar_val VARCHAR(100),
    	    bool_val BIT,
    	    date_val DATE,
    	    timestamp_val DATETIME2,
    	    json_val NVARCHAR(MAX),
    	    uuid_val UNIQUEIDENTIFIER,
    	    array_val NVARCHAR(MAX)
    	)`,
		InsertQuery: `
			WITH NumberSeries AS (
				SELECT %[2]d AS id
				UNION ALL
				SELECT id + 1 FROM NumberSeries WHERE id < %[3]d
			)
			INSERT INTO %[1]s (
				id, small_int, normal_int, big_int, float_val, decimal_val,
				text_val, varchar_val, bool_val, date_val,
				timestamp_val, json_val, uuid_val, array_val
			)
			SELECT
				id,
				CAST(id %% 32767 AS SMALLINT),
				CAST(id %% 2147483647 AS INTEGER),
				CAST((id %% 1000) * 1000000 AS BIGINT),
				SQRT(CAST(id AS FLOAT)),
				CAST((id %% 1000) * 3.14 AS DECIMAL(10,2)),
				'text-' + CAST(id AS NVARCHAR(50)),
				'varchar-' + LEFT(CONVERT(NVARCHAR(32), HASHBYTES('MD5', CAST(id AS NVARCHAR(50))), 2), 32),
				CASE WHEN id %% 2 = 0 THEN 1 ELSE 0 END,
				DATEADD(DAY, id %% 365, '2024-01-01'),
				DATEADD(HOUR, id %% 8760, '2024-01-01 00:00:00'),
				'{"key1":' + CAST(id %% 1000000 AS NVARCHAR(20)) + ',"key2":"value-' + CAST(id AS NVARCHAR(50)) + '","key3":[' + CAST(id %% 100 AS NVARCHAR(10)) + ',' + CAST((id %% 100) + 1 AS NVARCHAR(10)) + ',' + CAST((id %% 100) + 2 AS NVARCHAR(10)) + ']}',
				NEWID(),
				'[' + CAST(id %% 1000 AS NVARCHAR(10)) + ',' + CAST((id %% 1000) * 2 AS NVARCHAR(10)) + ',' + CAST((id %% 1000) * 3 AS NVARCHAR(10)) + ']'
			FROM NumberSeries
			OPTION (MAXRECURSION 0)
		`,
	},

	// A table with larger rows consisting primarily of a large (4kB) text field.
	"large_text": {
		Definition: `(
			id BIGINT PRIMARY KEY,
			created_at DATETIME,
			updated_at DATETIME,
			content NVARCHAR(MAX)
		)`,
		InsertQuery: `
			WITH NumberSeries AS (
				SELECT %[2]d AS id
				UNION ALL
				SELECT id + 1 FROM NumberSeries WHERE id < %[3]d
			)
			INSERT INTO %[1]s (id, created_at, updated_at, content)
			SELECT
				id,
				DATEADD(HOUR, id %% 8760, '2024-01-01 00:00:00'),
				DATEADD(HOUR, id %% 8760, '2024-01-01 00:00:00'),
				'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla posuere eu dolor sit amet dapibus. Donec vel semper nunc. Aenean metus mauris, volutpat et pharetra a, ornare ut ex. Nunc ut sagittis turpis, ut ultrices purus. Cras ante mi, faucibus eget tellus sed, ultrices fringilla diam. Aenean massa metus, lobortis et est sit amet, viverra suscipit justo. Morbi urna arcu, elementum aliquet tincidunt sed, euismod et risus. Maecenas pulvinar elit nec diam bibendum, nec tincidunt mauris cursus. Suspendisse dictum mauris a ex molestie, quis volutpat quam rhoncus. Nulla facilisi. Etiam sodales ligula ut risus vulputate, nec semper nibh fringilla.' +
				'Quisque sit amet ex id augue laoreet lobortis. Sed ac libero convallis justo posuere dignissim at vel diam. Curabitur ac ornare ante. Integer a dolor ut dui ullamcorper scelerisque ut vel est. Curabitur dapibus ipsum ut sapien porta tempor. Vestibulum bibendum egestas purus, a euismod risus vehicula eu. Integer faucibus justo in ex sodales venenatis. Proin commodo tortor a maximus aliquam. Nam eu enim nulla. Proin ac erat augue. Aliquam eleifend sagittis lacus quis tempus. Donec nisi turpis, rhoncus at ultrices nec, faucibus eu odio. Integer consequat ex dolor, in cursus odio lacinia vitae. Sed aliquam, neque consectetur tristique rutrum, mauris ligula hendrerit dui, vitae dictum nibh lorem sed est. Fusce lobortis dictum augue dignissim fermentum. Aliquam faucibus congue nulla sed consectetur.' +
				'Etiam non libero sapien. Morbi vitae fringilla est, ut efficitur lorem. Aliquam scelerisque viverra orci, vitae ullamcorper magna pellentesque sit amet. Quisque lobortis dignissim porttitor. Suspendisse gravida magna a consectetur sagittis. Suspendisse in diam ut nunc varius dictum. Proin nec quam euismod elit rhoncus mattis vel sed leo. Praesent purus nunc, tincidunt sit amet tempor vitae, imperdiet non mauris. Maecenas commodo hendrerit ex a interdum. Morbi placerat tortor id lorem pellentesque, ac luctus arcu molestie.' +
				'Nullam sit amet tristique odio, ultrices semper lectus. Curabitur at maximus justo. Etiam nulla erat, commodo a metus in, condimentum pellentesque ipsum. Phasellus sed leo quis lectus suscipit ullamcorper. Proin eu ipsum in tortor fringilla consequat. Nullam ultrices mattis porta. Mauris orci nulla, finibus vel turpis nec, sagittis commodo odio. Aenean viverra metus leo. In hac habitasse platea dictumst. Vestibulum egestas gravida nibh non faucibus.' +
				'Ut et magna dictum, vulputate nibh nec, varius magna. Quisque dignissim neque elementum, pulvinar turpis nec, suscipit lorem. Nulla ac dui id diam sodales pulvinar sed quis lorem. Suspendisse potenti. Praesent tristique ultricies pellentesque. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Cras mollis, turpis id vehicula luctus, ante risus tincidunt sem, vitae dictum tortor tellus eget justo. Integer sed porttitor libero, sed ullamcorper sapien. Curabitur pretium nibh a tellus facilisis, non porttitor magna ultrices. Nam tincidunt ipsum volutpat erat porta, quis auctor mauris luctus. Integer ut leo turpis. Mauris egestas cursus diam, in euismod odio tempus non. Maecenas a orci vel nisi sodales mattis non a mauris. Integer molestie molestie dictum.' +
				'Nulla facilisi. Nullam tristique bibendum enim at tristique. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris eget elit varius, elementum arcu sit amet, ultricies velit. Cras eu ex quis leo fringilla congue. Ut viverra a nibh nec semper. Fusce sodales ligula dolor, tristique finibus lorem iaculis ac. Etiam pellentesque vestibulum dolor vel mollis. In tristique lacus vehicula, hendrerit felis eu, auctor leo. Nam porta sapien dolor, eget euismod dui ullamcorper nec. In id placerat magna. Nam non elementum ligula. Cras scelerisque justo at libero finibus luctus. Proin orci lacus, vulputate vitae mauris id, tristique bibendum erat. Sed sollicitudin congue felis at sodales.' +
				'Morbi at scelerisque nibh. Duis dapibus sollicitudin augue, quis volutpat ipsum varius id. Phasellus ut dolor fringilla, molestie turpis at, auctor nisl. Aliquam luctus vel.'
			FROM NumberSeries
			OPTION (MAXRECURSION 0)
		`,
	},
}

func createBenchmarkTable(ctx context.Context, t testing.TB, tb *testBackend, shape string) (string, string) {
	t.Helper()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, benchmarkTableShapes[shape].Definition)
	return tableName, uniqueID
}

func insertBenchmarkRows(ctx context.Context, t testing.TB, tb *testBackend, shape string, tableName string, minID, maxID int) {
	t.Helper()
	log.WithFields(log.Fields{
		"table": tableName,
		"min":   minID,
		"max":   maxID,
		"count": maxID - minID,
	}).Info("inserting benchmark rows")

	// Insert rows in batches so we get periodic transaction commits
	const batchSize = 5000
	for i := minID; i < maxID; i += batchSize {
		var j = i + batchSize
		if j > maxID {
			j = maxID
		}
		tb.Query(ctx, t, fmt.Sprintf(benchmarkTableShapes[shape].InsertQuery, tableName, i, j-1))
	}
}

func benchBackfillN(b *testing.B, shape string, rowCount int) {
	var ctx, tb = context.Background(), sqlserverTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb, shape)
	insertBenchmarkRows(ctx, b, tb, shape, tableName, 0, rowCount)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.EndpointSpec.(*Config).Advanced.FeatureFlags = "replica_fencing"
	cs.Sanitizers = nil // Don't waste time sanitizing the data, we're not validating against a snapshot
	setShutdownAfterCaughtUp(b, true)

	// Reset timer after setup
	b.ResetTimer()

	var totalOutputBytes int64
	for b.Loop() {
		totalOutputBytes += runBenchmarkCapture(ctx, b, cs)

		// Reset state between iterations
		cs.Checkpoint = nil
	}

	// Report rows/sec and MBps instead of ns/op
	log.WithFields(log.Fields{
		"rows":    b.N * rowCount,
		"bytes":   totalOutputBytes,
		"seconds": b.Elapsed().Seconds(),
	}).Info("backfill benchmark complete")
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(b.Elapsed().Seconds(), "seconds")
	b.ReportMetric(float64(b.N*rowCount)/float64(b.Elapsed().Seconds()), "rows/sec")
	b.ReportMetric(float64(totalOutputBytes)/float64(1000000*b.Elapsed().Seconds()), "MBps")
}

func runBenchmarkCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) int64 {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	log.WithField("checkpoint", string(cs.Checkpoint)).Debug("running test capture")
	endpointSpecJSON, err := json.Marshal(cs.EndpointSpec)
	require.NoError(t, err)

	var open proto.Message = &pc.Request{
		Open: &pc.Request_Open{
			StateJson: cs.Checkpoint,
			Capture: &pf.CaptureSpec{
				Name:       "acmeCo/test-capture/source-something",
				ConfigJson: endpointSpecJSON,
				Bindings:   cs.Bindings,
				// Dummies to satisfy protocol validation, specific values shouldn't be important.
				ShardTemplate: &consumer_protocol.ShardSpec{
					Id: "capture/acmeCo/test-capture/source-something/69f23210cf8ccfcd/00000000-00000000",

					MaxTxnDuration: 1 * time.Second,
				},
				RecoveryLogTemplate: &broker_protocol.JournalSpec{
					Name:        broker_protocol.Journal("recovery/capture/acmeCo/test-capture/source-something/69f23210cf8ccfcd/00000000-00000000"),
					Replication: 3,
					Fragment: broker_protocol.JournalSpec_Fragment{
						Length:           512 * 1024 * 1024, // 512 MiB
						CompressionCodec: broker_protocol.CompressionCodec_GZIP,
						RefreshInterval:  30 * time.Second,
					},
				},
			},
			Range: &pf.RangeSpec{
				KeyBegin:    0,
				KeyEnd:      math.MaxUint32,
				RClockBegin: 0,
				RClockEnd:   math.MaxUint32,
			},
		},
	}

	// Serialize the Open request into a byte buffer which will act as the input reader for the benchmark run.
	var input bytes.Buffer
	err = protoio.NewUint32DelimitedWriter(&input, binary.LittleEndian).WriteMsg(open)
	require.NoError(t, err)

	// Run the capture, with an output io.Writer that merely tallies up the number of bytes written.
	var totalOutputBytes int64
	var output = WriterFunc(func(bs []byte) (n int, err error) {
		totalOutputBytes += int64(len(bs))
		return len(bs), nil
	})
	require.NoError(t, boilerplate.InnerMain(ctx, cs.Driver, &input, output))
	cs.Checkpoint = sqlcapture.FinalStateCheckpoint
	return totalOutputBytes
}

type WriterFunc func(p []byte) (n int, err error)

func (w WriterFunc) Write(p []byte) (n int, err error) {
	return w(p)
}

func benchReplicationN(b *testing.B, shape string, rowCount int) {
	var ctx, tb = context.Background(), sqlserverTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb, shape)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b, regexp.MustCompile(uniqueID))
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.Sanitizers = nil // Don't waste time sanitizing the data, we're not validating against a snapshot
	setShutdownAfterCaughtUp(b, true)

	// Insert a few rows to ensure the CDC instance is fully initialized.
	var minID = 0
	insertBenchmarkRows(ctx, b, tb, shape, tableName, minID, minID+100)
	minID += 100

	// Run the capture to get the initial backfill out of the way. Discard all outputs.
	runBenchmarkCapture(ctx, b, cs)
	b.ResetTimer()

	var totalOutputBytes int64
	for b.Loop() {
		// Insert another batch of rows, excluding insert time from the benchmark timer
		b.StopTimer()
		insertBenchmarkRows(ctx, b, tb, shape, tableName, minID, minID+rowCount)
		b.StartTimer()
		minID += rowCount

		// Run the capture, which should consume all the new rows via replication.
		// Unlike the backfill benchmark we don't need to clear the checkpoint after
		// each run because we actually want it to keep going and only consume new
		// replication events from the current iteration.
		totalOutputBytes += runBenchmarkCapture(ctx, b, cs)
	}

	// Report rows/sec and MBps instead of ns/op
	log.WithFields(log.Fields{
		"rows":    b.N * rowCount,
		"bytes":   totalOutputBytes,
		"seconds": b.Elapsed().Seconds(),
	}).Info("backfill benchmark complete")
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(b.Elapsed().Seconds(), "seconds")
	b.ReportMetric(float64(b.N*rowCount)/float64(b.Elapsed().Seconds()), "rows/sec")
	b.ReportMetric(float64(totalOutputBytes)/float64(1000000*b.Elapsed().Seconds()), "MBps")
}

const M = 1000000

func BenchmarkBackfill_SmallRows_1M(b *testing.B)  { benchBackfillN(b, "small_rows", 1*M) }
func BenchmarkBackfill_SmallRows_5M(b *testing.B)  { benchBackfillN(b, "small_rows", 5*M) }
func BenchmarkBackfill_SmallRows_10M(b *testing.B) { benchBackfillN(b, "small_rows", 10*M) }

func BenchmarkBackfill_ManyTypes_1M(b *testing.B)  { benchBackfillN(b, "many_types", 1*M) }
func BenchmarkBackfill_ManyTypes_5M(b *testing.B)  { benchBackfillN(b, "many_types", 5*M) }
func BenchmarkBackfill_ManyTypes_10M(b *testing.B) { benchBackfillN(b, "many_types", 10*M) }

func BenchmarkBackfill_LargeText_100k(b *testing.B) { benchBackfillN(b, "large_text", 100_000) }
func BenchmarkBackfill_LargeText_1M(b *testing.B)   { benchBackfillN(b, "large_text", 1*M) }
func BenchmarkBackfill_LargeText_5M(b *testing.B)   { benchBackfillN(b, "large_text", 5*M) }
func BenchmarkBackfill_LargeText_10M(b *testing.B)  { benchBackfillN(b, "large_text", 10*M) }

func BenchmarkReplication_SmallRows_1M(b *testing.B)  { benchReplicationN(b, "small_rows", 1*M) }
func BenchmarkReplication_SmallRows_5M(b *testing.B)  { benchReplicationN(b, "small_rows", 5*M) }
func BenchmarkReplication_SmallRows_10M(b *testing.B) { benchReplicationN(b, "small_rows", 10*M) }

func BenchmarkReplication_ManyTypes_1M(b *testing.B)  { benchReplicationN(b, "many_types", 1*M) }
func BenchmarkReplication_ManyTypes_5M(b *testing.B)  { benchReplicationN(b, "many_types", 5*M) }
func BenchmarkReplication_ManyTypes_10M(b *testing.B) { benchReplicationN(b, "many_types", 10*M) }

func BenchmarkReplication_LargeText_100k(b *testing.B) { benchReplicationN(b, "large_text", 100_000) }
func BenchmarkReplication_LargeText_1M(b *testing.B)   { benchReplicationN(b, "large_text", 1*M) }
func BenchmarkReplication_LargeText_5M(b *testing.B)   { benchReplicationN(b, "large_text", 5*M) }
func BenchmarkReplication_LargeText_10M(b *testing.B)  { benchReplicationN(b, "large_text", 10*M) }
