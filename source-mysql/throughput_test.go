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
	"github.com/estuary/connectors/sqlcapture/tests"
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
            x INT,                                                                                                                                    
            y INT,                                                                                                                                    
            z INT,                                                                                                                                    
            a TEXT,                                                                                                                                   
            b TEXT,                                                                                                                                   
            c TEXT                                                                                                                                    
        )`,
		InsertQuery: `                                                                                                                                
            INSERT INTO %[1]s (id, x, y, z, a, b, c)                                                                                                  
            WITH RECURSIVE seq AS (                                                                                                                   
                SELECT %[2]d AS n                                                                                                                     
                UNION ALL                                                                                                                             
                SELECT n + 1 FROM seq WHERE n < %[3]d                                                                                                 
            )                                                                                                                                         
            SELECT                                                                                                                                    
                n,                                                                                                                                    
                n %% 7777,                                                                                                                            
                n %% 131313,                                                                                                                          
                n %% 33333333,                                                                                                                        
                CONCAT('text-', n),                                                                                                                   
                CONCAT('text-', n + 1),                                                                                                               
                CONCAT('text-', n + 2)                                                                                                                
            FROM seq;                                                                                                                                 
        `,
	},

	// A table with many different types of columns.
	"many_types": {
		Definition: `(                                                                                                                                
            id BIGINT PRIMARY KEY,                                                                                                                    
            small_int SMALLINT,                                                                                                                       
            normal_int INT,                                                                                                                           
            big_int BIGINT,                                                                                                                           
            float_val DOUBLE,                                                                                                                         
            decimal_val DECIMAL(10,2),                                                                                                                
            text_val TEXT,                                                                                                                            
            varchar_val VARCHAR(100),                                                                                                                 
            bool_val TINYINT(1),                                                                                                                      
            date_val DATE,                                                                                                                            
            timestamp_val DATETIME,                                                                                                                   
            json_val JSON,                                                                                                                            
            uuid_val CHAR(36),                                                                                                                        
            array_val JSON                                                                                                                            
        )`,
		InsertQuery: `                                                                                                                                
            INSERT INTO %[1]s                                                                                                                         
            WITH RECURSIVE seq AS (                                                                                                                   
                SELECT %[2]d AS n                                                                                                                     
                UNION ALL                                                                                                                             
                SELECT n + 1 FROM seq WHERE n < %[3]d                                                                                                 
            )                                                                                                                                         
            SELECT                                                                                                                                    
                n,                                                                                                                                    
                n %% 32767,                                                                                                                           
                n %% 2147483647,                                                                                                                      
                (n %% 1000) * 1000000,                                                                                                                
                SQRT(n),                                                                                                                              
                (n %% 1000) * 3.14,                                                                                                                   
                CONCAT('text-', n),                                                                                                                   
                CONCAT('varchar-', MD5(n)),                                                                                                           
                n %% 2 = 0,                                                                                                                           
                DATE_ADD('2024-01-01', INTERVAL (n %% 365) DAY),                                                                                      
                DATE_ADD('2024-01-01 00:00:00', INTERVAL (n %% 8760) HOUR),                                                                           
                JSON_OBJECT(                                                                                                                          
                    'key1', n %% 1000000,                                                                                                             
                    'key2', CONCAT('value-', n),                                                                                                      
                    'key3', JSON_ARRAY(n %% 100, n %% 100 + 1, n %% 100 + 2)                                                                          
                ),                                                                                                                                    
                UUID(),                                                                                                                               
                JSON_ARRAY(n %% 1000, (n %% 1000) * 2, (n %% 1000) * 3)                                                                               
            FROM seq;                                                                                                                                 
        `,
	},

	// A table with larger rows consisting primarily of a large (4kB) text field.
	"large_text": {
		Definition: `(                                                                                                                                
            id BIGINT PRIMARY KEY,                                                                                                                    
            created_at DATETIME,                                                                                                                      
            updated_at DATETIME,                                                                                                                      
            content TEXT                                                                                                                              
        )`,
		InsertQuery: `                                                                                                                                
            INSERT INTO %[1]s                                                                                                                         
            WITH RECURSIVE seq AS (                                                                                                                   
                SELECT %[2]d AS n                                                                                                                     
                UNION ALL                                                                                                                             
                SELECT n + 1 FROM seq WHERE n < %[3]d                                                                                                 
            )                                                                                                                                         
            SELECT                                                                                                                                    
                n,                                                                                                                                    
                DATE_ADD('2024-01-01 00:00:00', INTERVAL (n %% 8760) HOUR),                                                                           
                DATE_ADD('2024-01-01 00:00:00', INTERVAL (n %% 8760) HOUR),                                                                           
                REPEAT(                                                                                                                               
                    CONCAT(                                                                                                                           
                        'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ',                                                                  
                        'Nulla facilisi. Sed non risus. Suspendisse lectus tortor, ',                                                                 
                        'dignissim sit amet, adipiscing nec, ultricies sed, dolor.'                                                                   
                    ),                                                                                                                                
                    20                                                                                                                                
                )                                                                                                                                     
            FROM seq;                                                                                                                                 
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
		"shape": shape,
		"min":   minID,
		"max":   maxID,
		"count": maxID - minID,
	}).Info("inserting benchmark rows")

	// Insert rows in batches of size N
	const batchSize = 10000
	tb.Query(ctx, t, fmt.Sprintf(`SET SESSION cte_max_recursion_depth = %d;`, batchSize))
	for i := minID; i < maxID; i += batchSize {
		var j = i + batchSize
		if j > maxID {
			j = maxID
		}
		tb.Query(ctx, t, fmt.Sprintf(benchmarkTableShapes[shape].InsertQuery, tableName, i, j-1))
	}
}

func benchBackfillN(b *testing.B, shape string, rowCount int) {
	var ctx, tb = context.Background(), mysqlTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb, shape)
	insertBenchmarkRows(ctx, b, tb, shape, tableName, 0, rowCount)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b)
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.EndpointSpec.(*Config).Timezone = "+00:00"
	cs.Bindings = tests.ConvertBindings(b, cs.Discover(ctx, b, regexp.MustCompile(uniqueID)))
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
	var ctx, tb = context.Background(), mysqlTestBackend(b)
	var tableName, uniqueID = createBenchmarkTable(ctx, b, tb, shape)

	// Set up the capture. The backfill chunk size is set to the production default of
	// 50k rows, and the validator is set to a special benchmark one which discards the
	// output data but counts how many documents and bytes we get.
	var cs = tb.CaptureSpec(ctx, b)
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 50000
	cs.EndpointSpec.(*Config).Timezone = "+00:00"
	cs.Bindings = tests.ConvertBindings(b, cs.Discover(ctx, b, regexp.MustCompile(uniqueID)))
	cs.Sanitizers = nil // Don't waste time sanitizing the data, we're not validating against a snapshot
	setShutdownAfterCaughtUp(b, true)

	// Run the capture to get the initial backfill out of the way. Discard all outputs.
	runBenchmarkCapture(ctx, b, cs)
	b.ResetTimer()

	var minID = 0
	var totalOutputBytes int64
	for i := 0; i < b.N; i++ {
		// Insert another batch of rows, excluding insert time from the benchmark timer
		b.StopTimer()
		insertBenchmarkRows(ctx, b, tb, shape, tableName, minID, minID+rowCount)
		minID += rowCount
		b.StartTimer()

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

func BenchmarkBackfill_LargeText_1M(b *testing.B)  { benchBackfillN(b, "large_text", 1*M) }
func BenchmarkBackfill_LargeText_5M(b *testing.B)  { benchBackfillN(b, "large_text", 5*M) }
func BenchmarkBackfill_LargeText_10M(b *testing.B) { benchBackfillN(b, "large_text", 10*M) }

func BenchmarkReplication_SmallRows_1M(b *testing.B)  { benchReplicationN(b, "small_rows", 1*M) }
func BenchmarkReplication_SmallRows_5M(b *testing.B)  { benchReplicationN(b, "small_rows", 5*M) }
func BenchmarkReplication_SmallRows_10M(b *testing.B) { benchReplicationN(b, "small_rows", 10*M) }

func BenchmarkReplication_ManyTypes_1M(b *testing.B)  { benchReplicationN(b, "many_types", 1*M) }
func BenchmarkReplication_ManyTypes_5M(b *testing.B)  { benchReplicationN(b, "many_types", 5*M) }
func BenchmarkReplication_ManyTypes_10M(b *testing.B) { benchReplicationN(b, "many_types", 10*M) }

func BenchmarkReplication_LargeText_1M(b *testing.B)  { benchReplicationN(b, "large_text", 1*M) }
func BenchmarkReplication_LargeText_5M(b *testing.B)  { benchReplicationN(b, "large_text", 5*M) }
func BenchmarkReplication_LargeText_10M(b *testing.B) { benchReplicationN(b, "large_text", 10*M) }
